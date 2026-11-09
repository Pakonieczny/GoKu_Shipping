/* charm-nest-ce-eraser.js — THE ERASER TOOL of the Charm Editor (Paul, 9 Nov 2026: "for the charm editor, I need the option to erase portions of a vectorized image.
 * Adobe Illustrator has this option so I need you to copy it and make it function the exact same way and add it to the left-hand side menu").
 *
 * Illustrator's Eraser Tool (Shift+E), in the Tools panel where Illustrator has it: the first tool of the Scissors group (Eraser, Scissors), its eraser icon, "Eraser Tool (Shift+E)".
 *   Drag            erases along the stroke: the area the brush sweeps is taken out of the filled paths it touches (a boolean subtraction, CharmEditorOps.kernel.erase on the
 *                   repo's Clipper): what is left stays closed filled paths, a shape cut in two becomes two paths, holes stay holes, every curve the brush did not touch is
 *                   the same curve. A line engraving (a path drawn by its stroke alone) is cut where the brush crosses it, the pieces outside left as open paths.
 *   What            with things selected, only those (their parts, inside groups too); with nothing selected, every unlocked, visible path under the stroke on this face.
 *                   Never what is locked: the cut lines (role 'cut': the charm's outline, its holes, its ring) are locked by the editor and never erased; live words are
 *                   words (Type ▸ Create Outlines first, as in Illustrator); text made into outlines is paths and erases.
 *   Vectorized      a traced image (charm-nest-ce-trace.js, placed by charm-nest-ce-image.js as a group of grey bands that share their borders): the stroke takes the
 *   images          engraving out of every band it crosses, each band cut by one and the same brush shape, so the bands' borders stay exact: no sliver between them, none under
 *                   both. Only the rings of a band whose box meets the brush's go through the boolean: the rest of the picture is not touched (even-odd fill is parity).
 *   Alt-drag        a marquee: the rectangle is erased.          Shift-drag   the stroke constrained to 0°, 45° or 90°.
 *   [ and ]         the brush smaller and larger.                Double-click the tool: Eraser Tool Options (Size in mm, Angle, Roundness, a preview of the tip, OK / Cancel).
 *   The cursor      a circle (the tip: an ellipse when the roundness is under 100 %) the size of the brush at the zoom of the view.
 *   Undo            one stroke is one undo step; Redo; Save keeps the result (it is ordinary geometry: the parts' own subpaths, their transforms kept exactly).
 *   Speed           the erased area is painted on a canvas over the art while dragging (the paper's colour over the swept area; the cut lines drawn back over it); the boolean
 *                   runs on mouse-up, in CharmEditorOps' Worker when it is big (a 16-level photo), with "Erasing…" and a spinner when it takes over 150 ms.
 *
 * Loads in the page after charm-nest-charm-editor.js and charm-nest-ce-ops.js (it registers itself on window.CharmEditor's extension points: a tool, a command), and in
 * node (module.exports: the brush geometry and the operation on items, for tests/charm-nest/ce-eraser.cjs). */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === 'object' && module.exports) module.exports = api;
  else { root.CharmEditorEraser = api; api.register(root); }
})(typeof self !== 'undefined' ? self : this, function (root) {
  'use strict';
  const doc = root && root.document ? root.document : null;
  const PT = 72 / 25.4;
  const fin = v => typeof v === 'number' && Number.isFinite(v);
  const num = (v, d) => (fin(v) ? v : d);
  const clamp = (v, a, b) => Math.min(b, Math.max(a, v));
  const K = 0.5522847498;
  const editorApi = () => (root && root.CharmEditor && root.CharmEditor.model ? root.CharmEditor : typeof require === 'function' ? (() => { try { return require('./charm-nest-charm-editor.js'); } catch (e) { return null; } })() : null);
  const opsApi = () => (root && root.CharmEditorOps ? root.CharmEditorOps : typeof require === 'function' ? (() => { try { return require('./charm-nest-ce-ops.js'); } catch (e) { return null; } })() : null);
  const MD = () => { const E = editorApi(); if (!E || !E.model) throw new Error('CharmEditor is not on the page'); return E.model; };
  const KR = () => { const O = opsApi(); if (!O || !O.kernel || typeof O.kernel.erase !== 'function') throw new Error('CharmEditorOps is not on the page'); return O.kernel; };

  /* ═══ 1 · THE BRUSH: Illustrator's calligraphic tip (Size, Angle, Roundness) and the area it sweeps, in face pt ═══ */
  const OPT0 = { mm: 1, angle: 0, round: 100 };
  const LIM = { min: 0.05, max: 50 };
  const normOpt = o => ({ mm: clamp(num(+o.mm, OPT0.mm), LIM.min, LIM.max), angle: ((num(+o.angle, 0) % 360) + 540) % 360 - 180, round: clamp(num(+o.round, 100), 1, 100) });
  let OPT = Object.assign({}, OPT0);
  try { const o = JSON.parse((root.localStorage && root.localStorage.getItem('ce.eraser')) || 'null'); if (o && typeof o === 'object') OPT = normOpt(Object.assign({}, OPT0, o)); } catch (e) { /* storage blocked: the defaults */ }
  const saveOpt = () => { try { if (root.localStorage) root.localStorage.setItem('ce.eraser', JSON.stringify(OPT)); } catch (e) { /* fine */ } };
  /** The tip in pt: half its size along its own axis (a), across it (b, the roundness), its angle (rad, y down: a positive angle turns anticlockwise on the screen, as Illustrator). */
  const tipOf = o => { o = normOpt(o || OPT); const a = o.mm * PT / 2; return { a, b: a * o.round / 100, ang: -o.angle * Math.PI / 180 }; };
  /** The tip's matrix: the unit circle to the tip at c. */
  const tipM = (tip, c) => { const cs = Math.cos(tip.ang), sn = Math.sin(tip.ang); return [cs * tip.a, sn * tip.a, -sn * tip.b, cs * tip.b, c[0], c[1]]; };
  const apM = (m, x, y) => [m[0] * x + m[2] * y + m[4], m[1] * x + m[3] * y + m[5]];
  /** The tip at c as one closed subpath of four curves (a circle's, under the tip's matrix: exact). */
  function tipSub(tip, c) {
    const m = tipM(tip, c), q = (x, y) => apM(m, x, y);
    return [['m', q(1, 0)], ['c', q(1, K), q(K, 1), q(0, 1)], ['c', q(-K, 1), q(-1, K), q(-1, 0)], ['c', q(-1, -K), q(-K, -1), q(0, -1)], ['c', q(K, -1), q(1, -K), q(1, 0)], ['h']];
  }
  /** The point of the tip (centred at 0) furthest along n. */
  function support(tip, n) {
    const m = tipM(tip, [0, 0]);
    const u = [m[0] * n[0] + m[1] * n[1], m[2] * n[0] + m[3] * n[1]], l = Math.hypot(u[0], u[1]) || 1;
    return apM(m, u[0] / l, u[1] / l);
  }
  /** The area the tip sweeps along points [[x, y], ...]: the tip at every point and, between two, the band between their tangents (the hull of two copies of a convex
   *  tip is the two of them and that band). -> closed subpaths (they overlap: the kernel makes them one shape). */
  function sweep(points, tip) {
    const pts = (points || []).filter(p => p && fin(p[0]) && fin(p[1]));
    const out = [];
    pts.forEach((p, i) => {
      out.push(tipSub(tip, p));
      if (!i) return;
      const a = pts[i - 1], d = [p[0] - a[0], p[1] - a[1]], L = Math.hypot(d[0], d[1]);
      if (L < 1e-9) return;
      const s = support(tip, [-d[1] / L, d[0] / L]);
      out.push([['m', [a[0] + s[0], a[1] + s[1]]], ['l', [p[0] + s[0], p[1] + s[1]]], ['l', [p[0] - s[0], p[1] - s[1]]], ['l', [a[0] - s[0], a[1] - s[1]]], ['h']]);
    });
    return out;
  }
  const rectSub = (a, b) => { const x0 = Math.min(a[0], b[0]), x1 = Math.max(a[0], b[0]), y0 = Math.min(a[1], b[1]), y1 = Math.max(a[1], b[1]); return [['m', [x0, y0]], ['l', [x1, y0]], ['l', [x1, y1]], ['l', [x0, y1]], ['h']]; };
  /** Shift: the end put on the nearest of 0°, 45°, 90° ... from the start (its distance kept along that line). */
  function snap45(a, b) {
    const dx = b[0] - a[0], dy = b[1] - a[1], ang = Math.round(Math.atan2(dy, dx) / (Math.PI / 4)) * (Math.PI / 4), c = Math.cos(ang), s = Math.sin(ang), l = dx * c + dy * s;
    return [a[0] + l * c, a[1] + l * s];
  }
  /** A drawn line thinned (Ramer–Douglas–Peucker) to within tol, its ends kept. */
  function thin(pts, tol) {
    if (!pts || pts.length < 3) return (pts || []).slice();
    const keep = new Uint8Array(pts.length); keep[0] = keep[pts.length - 1] = 1;
    const stack = [[0, pts.length - 1]];
    while (stack.length) {
      const [i, j] = stack.pop(), a = pts[i], b = pts[j], dx = b[0] - a[0], dy = b[1] - a[1], L = Math.hypot(dx, dy);
      let worst = -1, at = -1;
      for (let k = i + 1; k < j; k++) { const p = pts[k], d = L ? Math.abs((p[0] - a[0]) * dy - (p[1] - a[1]) * dx) / L : Math.hypot(p[0] - a[0], p[1] - a[1]); if (d > worst) { worst = d; at = k; } }
      if (worst > tol && at > 0) { keep[at] = 1; stack.push([i, at], [at, j]); }
    }
    return pts.filter((p, k) => keep[k]);
  }
  /** The eraser of one gesture: { kind: 'stroke', points, tip } | { kind: 'rect', a, b } -> { subpaths, step (pt), box } */
  function eraserOf(g) {
    if (g.kind === 'rect') {
      const sub = rectSub(g.a, g.b), w = Math.abs(g.b[0] - g.a[0]), h = Math.abs(g.b[1] - g.a[1]);
      return { subpaths: w > 1e-6 && h > 1e-6 ? [sub] : [], step: Math.max(Math.min(w, h) / 3, 0.01) };
    }
    const tip = g.tip || tipOf(), pts = g.tol > 0 ? thin(g.points, g.tol) : g.points;
    return { subpaths: sweep(pts, tip), step: Math.max(2 * Math.min(tip.a, tip.b) / 3, 0.01) };
  }

  /* ═══ 2 · ON THE EDITOR'S ITEMS (pure: the editor's tree in, a new tree out; CharmEditor.model does the tree work) ═══ */
  const evenOf = it => !!(it && (it.evenOdd || it.even === true || it.fillRule === 'evenodd'));
  /** What the eraser may take from: the selection's paths (their parts at any depth) or, with nothing selected, every path on the face; never a locked, hidden or cut
   *  thing, nor anything in a locked or hidden group; live words are not paths. -> [{ it, P (its parent's matrix), W, fill }] */
  function targetsOf(items, sel, geo) {
    const M = MD(), out = [], ix = M.indexTree(items, geo);
    const leaf = (it, P) => {
      if (it.kind !== 'path' || it.hidden || it.locked || it.role === 'cut' || !Array.isArray(it.subpaths) || !it.subpaths.length) return;
      const fill = M.paintFill(it); if (!fill && !M.paintStroke(it)) return;
      out.push({ it, P, W: M.mul(P, geo.matrix(it)), fill });
    };
    const rec = (list, P) => { for (const it of list || []) { if (it.hidden || it.locked) continue; if (it.kind === 'group') rec(it.children, M.mul(P, geo.matrix(it))); else leaf(it, P); } };
    const ids = (sel || []).filter(id => ix.has(id));
    if (!ids.length) { rec(items, M.I); return out; }
    for (const id of M.topmost(ids, ix)) {
      const e = ix.get(id);
      if ((e.anc || []).some(a => { const x = ix.get(a); return !x || x.it.hidden || x.it.locked; })) continue;
      if (e.it.kind === 'group') { if (!e.it.hidden && !e.it.locked) rec(e.it.children, M.mul(e.P, geo.matrix(e.it))); } else leaf(e.it, e.P);
    }
    return out;
  }
  /** The work of one stroke: the targets whose drawn box meets the eraser's, as the kernel's operands (face pt). -> { items, sel, ts, operands, eraser } */
  function eraseJob(items, sel, geo, er) {
    const M = MD(), K2 = KR(), eb = K2.boxOfSps(er.subpaths);
    const ts = [], operands = [];
    if (eb) for (const t of targetsOf(items, sel, geo)) {
      const b = geo.drawn(t.it, t.P), pad = t.fill ? 0 : M.lwOf(t.it) / 2;
      if (!b || b[0] - pad > eb[2] || b[2] + pad < eb[0] || b[1] - pad > eb[3] || b[3] + pad < eb[1]) continue;
      ts.push(t);
      operands.push(t.fill ? { subpaths: M.transformSubpaths(t.it.subpaths, t.W), evenOdd: evenOf(t.it) } : { subpaths: M.transformSubpaths(t.it.subpaths, t.W), open: true, step: er.step });
    }
    return { items, sel: (sel || []).slice(), ts, operands, eraser: er.subpaths };
  }
  /** How much work the operands are (their ops): over BIG the boolean goes to the Worker. */
  const workOf = job => { let n = 0; for (const o of job.operands) for (const sub of o.subpaths) n += sub.length; for (const sub of job.eraser) n += sub.length; return n; };
  const BIG = 6000;
  /** A part's matrix kept when its geometry changes (the editor turns and scales a part about the middle of its own box): its move set again so every point nobody
   *  touched stays exactly where it was on the charm. */
  function keepMatrix(n, old, geo) {
    const A = geo.matrix(old), B = geo.matrix(n), dx = A[4] - B[4], dy = A[5] - B[5];
    if (Math.abs(dx) > 1e-12 || Math.abs(dy) > 1e-12) { n.tx = num(n.tx, 0) + dx; n.ty = num(n.ty, 0) + dy; }
    return n;
  }
  /** Items in the places of others (map id -> [items]; [] removes), at any depth; a group whose parts changed keeps its matrix, a group left empty goes. */
  function splice(list, map, geo) {
    let changed = false; const out = [];
    for (const it of list) {
      if (map.has(it.id)) { changed = true; out.push(...map.get(it.id)); continue; }
      if (it.kind === 'group' && Array.isArray(it.children)) {
        const ch = splice(it.children, map, geo);
        if (ch !== it.children) { changed = true; if (ch.length) out.push(keepMatrix(Object.assign({}, it, { children: ch }), it, geo)); continue; }
      }
      out.push(it);
    }
    return changed ? out : list;
  }
  /** The kernel's answers put in place: each erased part with its new geometry in its own frame (its transform kept), a simple path cut in two as two paths, a part with
   *  nothing left removed. -> { items, sel, n (parts changed), ids (the parts made) } or { items, sel, n: 0 } */
  function placeErase(job, rs) {
    const M = MD(), map = new Map(), made = new Map();
    job.ts.forEach((t, i) => {
      const r = rs && rs[i]; if (!r) return;
      const Wi = M.inv(t.W), it = t.it;
      // (a path of one subpath cut in pieces becomes as many paths, as Illustrator's; a compound path, a traced band, stays one path of what is left)
      const parts = (it.subpaths.length === 1 && !evenOf(it) && !it.artGrey && r.pieces && r.pieces.length > 1) ? r.pieces : [r.subpaths];
      const out = [];
      parts.forEach((sps, k) => {
        if (!sps || !sps.length) return;
        const n = Object.assign({}, it, { subpaths: M.transformSubpaths(sps, Wi) });
        if (k) { n.id = M.newId(); n.source = 'added'; delete n._autoLock; delete n._lockedIn; }
        out.push(keepMatrix(n, it, job.geo));
      });
      map.set(it.id, out); made.set(it.id, out.map(x => x.id));
    });
    if (!map.size) return { items: job.items, sel: job.sel, n: 0, ids: [] };
    const items = splice(job.items, map, job.geo);
    const sel = [].concat(...job.sel.map(id => (made.has(id) ? made.get(id) : [id])));
    return { items, sel, n: map.size, ids: [].concat(...made.values()) };
  }
  /** One stroke on the items, start to end on this thread (node, the tests, a small stroke on the page). */
  function eraseItems(items, sel, geo, g) {
    const er = g.subpaths ? g : eraserOf(g), job = eraseJob(items, sel, geo, er); job.geo = geo;
    if (!job.ts.length) return { items, sel: (sel || []).slice(), n: 0, ids: [] };
    return placeErase(job, KR().erase(job.operands, job.eraser));
  }

  /* ═══ 3 · THE TOOL ═══ */
  const IC = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.25" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">'
    + '<path d="M14.6 4.1 20 9.5a1.4 1.4 0 0 1 0 2L11.6 19.9H7.4l-3.1-3.1a1.4 1.4 0 0 1 0-2Z"/><path d="m9 9.5 5.6 5.6"/><path d="M11.6 19.9h8.4"/></svg>';
  const CSS = `
.cer-cv{ position:absolute; left:0; top:0; pointer-events:none; }
.cer-cur{ position:absolute; left:0; top:0; pointer-events:none; overflow:visible; z-index:6; }
.cer-cur ellipse{ fill:none; stroke:#fff; stroke-width:2.5; } .cer-cur ellipse + ellipse{ stroke:#1f1f1f; stroke-width:1; } .cer-cur circle{ fill:#1f1f1f; stroke:#fff; stroke-width:.6; }
.cer-cur[hidden]{ display:none; }
.cer-size{ position:absolute; z-index:8; display:flex; align-items:center; gap:4px; padding:4px 6px; border-radius:4px; background:rgba(40,40,40,.92); color:#e8e8e8;
  font:12px/1.2 'Source Sans 3',system-ui,sans-serif; box-shadow:0 1px 4px rgba(0,0,0,.35); }
.cer-size b{ font-weight:600; margin-right:2px; }
.cer-size button{ min-width:22px; height:22px; padding:0 6px; border:1px solid rgba(255,255,255,.22); border-radius:3px; background:#535353; color:inherit; font:inherit; cursor:pointer; }
.cer-size button:hover{ background:#636363; }
.cer-size input{ width:46px; height:20px; padding:0 4px; border:1px solid rgba(255,255,255,.22); border-radius:3px; background:#2b2b2b; color:inherit; font:inherit; text-align:right; }
.cer-size input[type=range]{ width:110px; padding:0; border:0; background:none; }
.cer-busy{ position:absolute; right:12px; bottom:12px; z-index:7; display:flex; align-items:center; gap:8px; padding:6px 10px; border-radius:4px; background:rgba(30,30,30,.88); color:#eee;
  font:12px/1.2 'Source Sans 3',system-ui,sans-serif; pointer-events:none; }
.cer-busy i{ width:12px; height:12px; border-radius:50%; border:2px solid rgba(255,255,255,.3); border-top-color:#fff; animation:cerSpin .7s linear infinite; }
@keyframes cerSpin{ to{ transform:rotate(360deg); } }
@media (prefers-reduced-motion: reduce){ .cer-busy i{ animation-duration:2s; } }
.cer-tip{ display:flex; align-items:center; justify-content:center; height:84px; margin:4px 0 10px; background:#fff; border:1px solid rgba(0,0,0,.25); }
.cer-tip svg{ width:80px; height:80px; } .cer-tip ellipse{ fill:#1f1f1f; } .cer-tip path{ stroke:#9a9a9a; stroke-width:1; fill:none; }
`;
  function injectCss() { if (!doc || doc.getElementById('ceEraserCss')) return; const s = doc.createElement('style'); s.id = 'ceEraserCss'; s.textContent = CSS; (doc.head || doc.documentElement).appendChild(s); }

  let St = null;   // the workspace's: { ctx, cur, cv, g (the gesture), queue, busy, ... }
  const S = ctx => { if (!St || St.ctx !== ctx) St = { ctx, cur: null, cv: null, g: null, pending: 0, chain: null, spin: null }; return St; };
  const viewK = ctx => { const V = ctx.view(); return Math.hypot(V[0], V[1]) || 1; };
  const say = (ctx, t, ms) => { if (typeof ctx.say === 'function') ctx.say(t, ms == null ? 2600 : ms); };

  /* the cursor: the tip, drawn at the size it erases, where the pointer is */
  function cursorEl(ctx) {
    const st = S(ctx);
    if (st.cur && st.cur.isConnected) return st.cur;
    injectCss();
    const el = doc.createElementNS('http://www.w3.org/2000/svg', 'svg'); el.setAttribute('class', 'cer-cur'); el.setAttribute('aria-hidden', 'true'); el.setAttribute('hidden', '');
    el.innerHTML = '<ellipse/><ellipse/><circle r="1.2"/>';
    ctx.stage.appendChild(el); st.cur = el; return el;
  }
  function placeCursor(ctx, p) {
    const el = cursorEl(ctx), tip = tipOf(), k = viewK(ctx), q = ctx.stageOf(p.x, p.y);
    const A = Math.max(tip.a * k, 1.5), B = Math.max(tip.b * k, 1.5), w = Math.ceil(Math.max(A, B)) + 3;
    el.setAttribute('width', 2 * w); el.setAttribute('height', 2 * w); el.setAttribute('viewBox', `${-w} ${-w} ${2 * w} ${2 * w}`);
    el.style.transform = `translate(${(q[0] - w).toFixed(2)}px,${(q[1] - w).toFixed(2)}px)`;
    const deg = (tip.ang * 180 / Math.PI).toFixed(3);
    for (const e of el.querySelectorAll('ellipse')) { e.setAttribute('rx', A.toFixed(2)); e.setAttribute('ry', B.toFixed(2)); e.setAttribute('transform', `rotate(${deg})`); }
    el.removeAttribute('hidden');
    el.dataset.d = String(+(2 * A).toFixed(3));   // (the circle's diameter on the screen, px: read by the tests)
  }
  const hideCursor = ctx => { const st = S(ctx); if (st.cur) st.cur.setAttribute('hidden', ''); };

  /* the preview: a canvas over the art; the swept area painted in the paper's colour, the cut lines drawn back over it */
  function canvasOf(ctx) {
    const st = S(ctx), stage = ctx.stage, dpr = root.devicePixelRatio || 1, w = stage.clientWidth, h = stage.clientHeight;
    let cv = st.cv;
    if (!cv || !cv.isConnected) {
      injectCss();
      cv = doc.createElement('canvas'); cv.className = 'cer-cv'; cv.setAttribute('aria-hidden', 'true');
      const art = stage.querySelector('.ce-art'); if (art && art.nextSibling) stage.insertBefore(cv, art.nextSibling); else stage.appendChild(cv);
      st.cv = cv;
    }
    if (cv.width !== Math.round(w * dpr) || cv.height !== Math.round(h * dpr)) { cv.width = Math.round(w * dpr); cv.height = Math.round(h * dpr); cv.style.width = w + 'px'; cv.style.height = h + 'px'; }
    const c = cv.getContext('2d'); c.setTransform(dpr, 0, 0, dpr, 0, 0);
    return c;
  }
  function clearCanvas(ctx) { const st = S(ctx); if (st.cv && st.cv.isConnected) { const c = st.cv.getContext('2d'); c.setTransform(1, 0, 0, 1, 0, 0); c.clearRect(0, 0, st.cv.width, st.cv.height); } }
  /** What the preview needs for this gesture: the paper's colour, the boxes it may paint in (with a selection: the selected parts' boxes), the cut lines to draw back. */
  function previewSetup(ctx) {
    const M = MD(), geo = ctx.geo, V = ctx.view();
    const paperEl = ctx.stage.querySelector('.ce-paper');
    let paper = paperEl ? root.getComputedStyle(paperEl).backgroundColor : '';
    if (!paper || /rgba\(0, 0, 0, 0\)|transparent/.test(paper)) paper = '#ffffff';
    const toS = b => { const p = M.ap(V, b[0], b[1]), q = M.ap(V, b[2], b[3]); return [Math.min(p[0], q[0]), Math.min(p[1], q[1]), Math.abs(q[0] - p[0]), Math.abs(q[1] - p[1])]; };
    let clip = null;
    if (ctx.selection().length) { clip = []; for (const t of targetsOf(ctx.items(), ctx.selection(), geo)) { const b = geo.drawn(t.it, t.P); if (b) { const r = toS(b); clip.push([r[0] - 2, r[1] - 2, r[2] + 4, r[3] + 4]); } } }
    const cuts = [];
    M.walk(ctx.items(), it => {
      if (it.kind === 'group' || it.hidden || it.role !== 'cut' || cuts.length > 40) return;
      const e = ctx.entry(it.id); if (!e) return;
      let path = null; try { path = new root.Path2D(M.dOf(M.outlinesOf(it, geo, e.P))); } catch (er) { path = null; }
      if (!path) return;
      const el = ctx.stage.querySelector(`.ce-items [data-id="${String(it.id).replace(/["\\]/g, '\\$&')}"]`), cs = el ? root.getComputedStyle(el) : null;
      const sw = cs ? parseFloat(cs.strokeWidth) || 1 : 1, nonScaling = cs && /non-scaling/.test(String(cs.vectorEffect || (el && el.getAttribute('vector-effect')) || ''));
      cuts.push({ path, color: cs && cs.stroke && cs.stroke !== 'none' ? cs.stroke : '#1f1f1f', px: nonScaling ? sw : sw * Math.hypot(V[0], V[1]) });
    });
    return { paper, clip, cuts, V };
  }
  const scr = (V, p) => [V[0] * p[0] + V[2] * p[1] + V[4], V[1] * p[0] + V[3] * p[1] + V[5]];
  /** Paints one piece of the sweep (the tip at b, and the band from a to b), in stage px. */
  function dab(ctx, pv, a, b, tip) {
    const c = canvasOf(ctx), k = viewK(ctx), V = pv.V, A = Math.max(tip.a * k, 0.5), B = Math.max(tip.b * k, 0.5);
    const pa = scr(V, a), pb = scr(V, b);
    c.save();
    if (pv.clip) { c.beginPath(); for (const r of pv.clip) c.rect(r[0], r[1], r[2], r[3]); c.clip(); }
    c.fillStyle = pv.paper; c.beginPath(); c.ellipse(pb[0], pb[1], A, B, tip.ang, 0, 2 * Math.PI); c.fill();
    const d = [pb[0] - pa[0], pb[1] - pa[1]], L = Math.hypot(d[0], d[1]);
    if (L > 1e-6) {
      c.beginPath(); c.ellipse(pa[0], pa[1], A, B, tip.ang, 0, 2 * Math.PI); c.fill();
      const s = support({ a: A, b: B, ang: tip.ang }, [-d[1] / L, d[0] / L]);
      c.beginPath(); c.moveTo(pa[0] + s[0], pa[1] + s[1]); c.lineTo(pb[0] + s[0], pb[1] + s[1]); c.lineTo(pb[0] - s[0], pb[1] - s[1]); c.lineTo(pa[0] - s[0], pa[1] - s[1]); c.closePath(); c.fill();
    }
    c.restore();
    redrawCuts(c, pv, [Math.min(pa[0], pb[0]) - Math.max(A, B) - 2, Math.min(pa[1], pb[1]) - Math.max(A, B) - 2, Math.abs(pb[0] - pa[0]) + 2 * Math.max(A, B) + 4, Math.abs(pb[1] - pa[1]) + 2 * Math.max(A, B) + 4]);
  }
  function redrawCuts(c, pv, r) {
    if (!pv.cuts.length) return;
    const dpr = root.devicePixelRatio || 1, V = pv.V;
    c.save(); c.beginPath(); c.rect(r[0], r[1], r[2], r[3]); c.clip();
    c.setTransform(dpr * V[0], dpr * V[1], dpr * V[2], dpr * V[3], dpr * V[4], dpr * V[5]);
    const k = Math.hypot(V[0], V[1]) || 1;
    for (const cu of pv.cuts) { c.strokeStyle = cu.color; c.lineWidth = cu.px / k; c.lineJoin = 'round'; c.stroke(cu.path); }
    c.restore();
  }
  /** The whole gesture painted again (Shift pressed or let go, Alt's marquee). */
  function repaint(ctx) {
    const st = S(ctx), g = st.g; if (!g) return;
    clearCanvas(ctx);
    const c = canvasOf(ctx);
    if (g.alt) {
      const a = scr(g.pv.V, g.start), b = scr(g.pv.V, g.last);
      c.save(); c.setLineDash([4, 3]); c.strokeStyle = '#1f1f1f'; c.lineWidth = 1; c.strokeRect(Math.min(a[0], b[0]) + 0.5, Math.min(a[1], b[1]) + 0.5, Math.abs(b[0] - a[0]), Math.abs(b[1] - a[1]));
      c.strokeStyle = '#fff'; c.lineDashOffset = 3.5; c.strokeRect(Math.min(a[0], b[0]) + 0.5, Math.min(a[1], b[1]) + 0.5, Math.abs(b[0] - a[0]), Math.abs(b[1] - a[1])); c.restore();
      return;
    }
    const pts = linePoints(g);
    for (let i = 0; i < pts.length; i++) dab(ctx, g.pv, pts[Math.max(0, i - 1)], pts[i], g.tip);
  }
  const linePoints = g => (g.shift && g.pts.length > 1 ? [g.start, snap45(g.start, g.last)] : g.pts);
  /** The swept rectangle painted as erased (Alt's marquee let go). */
  function paintRect(ctx, g) {
    clearCanvas(ctx);
    const c = canvasOf(ctx), a = scr(g.pv.V, g.start), b = scr(g.pv.V, g.last);
    const r = [Math.min(a[0], b[0]), Math.min(a[1], b[1]), Math.abs(b[0] - a[0]), Math.abs(b[1] - a[1])];
    c.save(); if (g.pv.clip) { c.beginPath(); for (const q of g.pv.clip) c.rect(q[0], q[1], q[2], q[3]); c.clip(); }
    c.fillStyle = g.pv.paper; c.fillRect(r[0], r[1], r[2], r[3]); c.restore();
    redrawCuts(c, g.pv, [r[0] - 2, r[1] - 2, r[2] + 4, r[3] + 4]);
  }

  /* the stroke let go: the boolean (here, or in the Worker when big), one undo step */
  function spinner(ctx, on) {
    const st = S(ctx);
    if (!on) { if (st.spin) { st.spin.remove(); st.spin = null; } return; }
    if (st.spin && st.spin.isConnected) return;
    injectCss();
    const el = doc.createElement('div'); el.className = 'cer-busy'; el.setAttribute('role', 'status'); el.innerHTML = '<i aria-hidden="true"></i><span>Erasing…</span>';
    ctx.stage.appendChild(el); st.spin = el;
  }
  function land(ctx, job, rs) {
    const r = placeErase(job, rs);
    if (!r.n) return false;
    const cur = ctx.items(), next = r.items, map = new Map(), n = Math.min(cur.length, next.length);
    for (let i = 0; i < n; i++) if (cur[i] !== next[i]) map.set(cur[i].id, next[i]);
    if (map.size) ctx.replaceItems(map);
    if (next.length > cur.length) ctx.addItems(next.slice(cur.length), { select: false });
    else if (next.length < cur.length) ctx.removeItems(cur.slice(next.length).map(it => it.id));
    ctx.select(r.sel.filter(id => !!ctx.entry(id)));
    if (typeof ctx.commit === 'function') ctx.commit('Erase');
    try { if (root.CNDesignLog && typeof root.CNDesignLog.event === 'function') root.CNDesignLog.event('editorOp', { op: 'erase', face: ctx.face(), n: r.n }); } catch (e) { /* the log is a courtesy */ }
    return true;
  }
  const raf2 = () => new Promise(r => { if (typeof root.requestAnimationFrame !== 'function') { setTimeout(r, 0); return; } root.requestAnimationFrame(() => root.requestAnimationFrame(r)); });
  /** One stroke's boolean, after the strokes before it. */
  function run(ctx, g) {
    const st = S(ctx);
    const go = () => {
      const er = g.alt ? eraserOf({ kind: 'rect', a: g.start, b: g.last }) : eraserOf({ kind: 'stroke', points: linePoints(g), tip: g.tip, tol: 0.35 / viewK(ctx) });
      if (!er.subpaths.length) return null;
      const items0 = ctx.items(), job = eraseJob(items0, ctx.selection(), ctx.geo, er); job.geo = ctx.geo;
      if (!job.ts.length) return null;
      const t0 = Date.now(), O = opsApi(), big = workOf(job) > BIG;
      const sync = () => { try { return KR().erase(job.operands, job.eraser); } catch (e) { say(ctx, 'The eraser could not do this: ' + String(e && e.message || e).slice(0, 100), 4000); return null; } };
      if (!big) { land(ctx, job, sync()); return null; }
      let timer = setTimeout(() => { timer = 0; spinner(ctx, true); }, 150);
      const finish = rs => {
        if (timer) clearTimeout(timer);
        if (ctx.items() !== items0) { say(ctx, 'The work changed while the eraser worked: nothing was erased, try again', 3600); return; }
        if (rs) land(ctx, job, rs);
        st.lastMs = Date.now() - t0;
      };
      const viaPage = () => { if (timer) { clearTimeout(timer); timer = 0; } spinner(ctx, true); return raf2().then(() => finish(sync())); };
      if (O && typeof O.eraseInWorker === 'function') return O.eraseInWorker(job.operands, job.eraser).then(finish, viaPage);
      return viaPage();
    };
    st.pending++;
    const done = () => { st.pending--; if (!st.pending) { spinner(ctx, false); clearCanvas(ctx); } };
    if (!st.chain) {
      // (nothing waiting: a small stroke lands now, in the gesture's own frame)
      let p = null; try { p = go(); } catch (e) { say(ctx, 'The eraser could not do this', 3000); }
      if (!p) { done(); return; }
      st.chain = p.then(done, done).then(() => { if (!st.pending) st.chain = null; });
      return;
    }
    st.chain = st.chain.then(() => { let p = null; try { p = go(); } catch (e) { p = null; } return p; }).then(done, done).then(() => { if (!st.pending) st.chain = null; });
  }

  function resize(ctx, dir) {
    const mm = OPT.mm;
    const step = mm < 1 - 1e-9 || (dir < 0 && mm <= 1 + 1e-9) ? 0.1 : mm < 5 - 1e-9 || (dir < 0 && mm <= 5 + 1e-9) ? 0.25 : 1;
    OPT = normOpt(Object.assign({}, OPT, { mm: Math.round((mm + dir * step) / step) * step }));
    OPT.mm = +OPT.mm.toFixed(3);
    saveOpt();
    const st = S(ctx); if (st.lastP && st.cur && !st.cur.hasAttribute('hidden')) placeCursor(ctx, st.lastP);
    syncBar(ctx);
    say(ctx, `Eraser size ${OPT.mm} mm`, 1200);
  }

  /* the size bar (Paul, 9 Nov 2026: "make sure the user can resize the eraser circle"): over the top left of the work area while the Eraser is in hand: − / a slider /
     the size in mm / + / Options… ([ and ] and Eraser Tool Options do the same) */
  const SL = { lo: Math.log(LIM.min), hi: Math.log(LIM.max) };
  const sliderOf = mm => Math.round((Math.log(mm) - SL.lo) / (SL.hi - SL.lo) * 1000);
  const mmOfSlider = v => +Math.exp(SL.lo + v / 1000 * (SL.hi - SL.lo)).toFixed(mm2dp(Math.exp(SL.lo + v / 1000 * (SL.hi - SL.lo))));
  function mm2dp(mm) { return mm < 1 ? 2 : mm < 10 ? 1 : 0; }
  function syncBar(ctx) {
    const st = S(ctx), b = st.bar; if (!b) return;
    const inp = b.querySelector('[data-er="mm"]'), sl = b.querySelector('[data-er="slide"]');
    if (doc.activeElement !== inp) inp.value = String(OPT.mm);
    sl.value = String(sliderOf(OPT.mm));
  }
  function setSize(ctx, mm) {
    if (!fin(mm)) { syncBar(ctx); return; }
    OPT = normOpt(Object.assign({}, OPT, { mm })); OPT.mm = +OPT.mm.toFixed(3); saveOpt();
    const st = S(ctx); if (st.lastP && st.cur && !st.cur.hasAttribute('hidden')) placeCursor(ctx, st.lastP);
    syncBar(ctx);
  }
  function showBar(ctx) {
    const st = S(ctx); if (st.bar && st.bar.isConnected) { syncBar(ctx); return; }
    injectCss();
    const host = ctx.stage.parentNode || ctx.stage;
    const b = doc.createElement('div'); b.className = 'cer-size'; b.setAttribute('role', 'toolbar'); b.setAttribute('aria-label', 'Eraser size');
    b.innerHTML = '<b>Eraser</b><button type="button" data-er="-" aria-label="Smaller brush" title="Smaller ( [ )">−</button>'
      + '<input type="range" min="0" max="1000" step="1" data-er="slide" aria-label="Brush size">'
      + '<input data-er="mm" inputmode="decimal" autocomplete="off" spellcheck="false" aria-label="Brush size, mm"><span>mm</span>'
      + '<button type="button" data-er="+" aria-label="Larger brush" title="Larger ( ] )">+</button><button type="button" data-er="opts" title="Eraser Tool Options (or double-click the tool)">Options…</button>';
    host.appendChild(b);
    const sr = ctx.stage.getBoundingClientRect(), hr = host.getBoundingClientRect();
    Object.assign(b.style, { left: Math.round(sr.left - hr.left + 10) + 'px', top: Math.round(sr.top - hr.top + 10) + 'px' });
    const inp = b.querySelector('[data-er="mm"]'), sl = b.querySelector('[data-er="slide"]');
    b.addEventListener('pointerdown', e => e.stopPropagation());
    b.addEventListener('click', e => {
      const x = e.target.closest('[data-er]'); if (!x) return;
      if (x.dataset.er === '-') resize(ctx, -1); else if (x.dataset.er === '+') resize(ctx, 1); else if (x.dataset.er === 'opts') optionsDialog(ctx);
    });
    sl.addEventListener('input', () => setSize(ctx, mmOfSlider(+sl.value)));
    const commitIn = () => setSize(ctx, parseFloat(String(inp.value).replace(',', '.')));
    inp.addEventListener('change', commitIn);
    // (the editor takes every key at the window; an element that carries __ceKey is handed its own)
    const keys = e => {
      if (e.key === 'Enter') { e.preventDefault(); commitIn(); inp.blur(); }
      else if (e.key === 'Escape') { e.preventDefault(); syncBar(ctx); inp.blur(); }
      else if (e.key === 'ArrowUp' || e.key === 'ArrowDown') { e.preventDefault(); resize(ctx, e.key === 'ArrowUp' ? 1 : -1); }
    };
    inp.__ceKey = keys; sl.__ceKey = e => { if (e.key === 'Escape') sl.blur(); };
    for (const btn of b.querySelectorAll('button')) btn.__ceKey = e => { if (e.key === 'Escape') btn.blur(); };
    st.bar = b; syncBar(ctx);
  }
  function hideBar(ctx) { const st = S(ctx); if (st.bar) { st.bar.remove(); st.bar = null; } }

  const tool = {
    id: 'eraser', icon: IC, title: 'Eraser', key: 'Shift+E', flyout: 'Pencil', cursor: 'none', tip: 'Erase parts of paths',
    hint: 'Drag to erase · Alt-drag: a box · Shift: straight at 45° · brush size: the bar at the top left, or [ ] · double-click the tool for its options',
    onArm(ctx) {
      injectCss();
      const st = S(ctx);
      if (!st.leave) { st.leave = () => hideCursor(ctx); ctx.stage.addEventListener('pointerleave', st.leave); }
      showBar(ctx);
    },
    onDisarm(ctx) {
      const st = S(ctx);
      if (st.leave) { ctx.stage.removeEventListener('pointerleave', st.leave); st.leave = null; }
      if (st.cur) { st.cur.remove(); st.cur = null; }
      hideBar(ctx);
      if (st.g) { st.g = null; if (!st.pending) clearCanvas(ctx); }
    },
    onDown(ctx, ev, p) {
      if (!p || (ev && ev.button)) return;
      const st = S(ctx);
      if (!st.pending) clearCanvas(ctx);
      const pt = [p.x, p.y];
      st.g = { start: pt, last: pt, pts: [pt], alt: !!ev.altKey, shift: !!ev.shiftKey, tip: tipOf(), pv: previewSetup(ctx) };
      st.lastP = p; placeCursor(ctx, p);
      if (!st.g.alt) dab(ctx, st.g.pv, pt, pt, st.g.tip);
    },
    onMove(ctx, ev, p) {
      if (!p) return;
      const st = S(ctx); st.lastP = p; placeCursor(ctx, p);
      const g = st.g; if (!g) return;
      if (!(ev.buttons & 1)) { st.g = null; if (!st.pending) clearCanvas(ctx); return; }   // (the gesture was dropped, Esc: nothing is erased)
      const pt = [p.x, p.y], k = viewK(ctx), was = g.shift;
      g.shift = !!ev.shiftKey; g.last = pt;
      if (g.alt) { repaint(ctx); return; }
      const prev = g.pts[g.pts.length - 1];
      if (Math.hypot(pt[0] - prev[0], pt[1] - prev[1]) * k >= 1) g.pts.push(pt);
      if (g.shift || was) { repaint(ctx); return; }
      if (g.pts[g.pts.length - 1] === pt) dab(ctx, g.pv, prev, pt, g.tip);
    },
    onUp(ctx, ev, p) {
      const st = S(ctx), g = st.g; st.g = null;
      if (!g) return;
      if (p) { g.last = [p.x, p.y]; if (!g.alt && !g.shift) { const prev = g.pts[g.pts.length - 1]; if (prev[0] !== p.x || prev[1] !== p.y) g.pts.push(g.last); } }
      if (ev && typeof ev.shiftKey === 'boolean') g.shift = ev.shiftKey;
      if (g.alt) paintRect(ctx, g); else if (g.shift) repaint(ctx);
      run(ctx, g);
    },
    onKey(ctx, ev) {
      if (ev.metaKey || ev.ctrlKey || ev.altKey) return false;
      if (ev.key === ']' || ev.code === 'BracketRight') { resize(ctx, 1); return true; }
      if (ev.key === '[' || ev.code === 'BracketLeft') { resize(ctx, -1); return true; }
      return false;
    }
  };

  /* ═══ 4 · ERASER TOOL OPTIONS (a double-click on the tool, as Illustrator): Size (mm), Angle, Roundness, the tip drawn as it is set; OK / Cancel ═══ */
  function optionsDialog(ctx) {
    if (!doc || !ctx || !ctx.root) return null;
    const win = ctx.root.querySelector('.ce-win') || ctx.root;
    if (win.querySelector('.cer-opts')) return null;
    injectCss();
    const box = doc.createElement('div'); box.className = 'ce-ask cops-modal cer-opts'; box.tabIndex = -1;
    const card = doc.createElement('div'); card.className = 'cops-card'; card.tabIndex = -1; card.setAttribute('role', 'dialog'); card.setAttribute('aria-modal', 'true'); card.setAttribute('aria-label', 'Eraser Tool Options');
    const v = { mm: String(OPT.mm), angle: String(OPT.angle), round: String(OPT.round) };
    const field = (id, label, unit) => `<label class="cops-f"><span>${label}</span><span class="ce-num"><input data-eo="${id}" inputmode="decimal" autocomplete="off" spellcheck="false" value="${v[id]}" aria-label="${label}"><i>${unit}</i></span></label>`;
    card.innerHTML = '<h3>Eraser Tool Options</h3><div class="cer-tip" aria-hidden="true"><svg viewBox="-40 -40 80 80"><path d="M-36 0H36M0 -36V36"/><ellipse/></svg></div>'
      + field('mm', 'Size', 'mm') + field('angle', 'Angle', '°') + field('round', 'Roundness', '%')
      + '<div class="ce-ask__b"><button class="ce-btn ce-btn--ghost" type="button" data-eob="cancel">Cancel</button><button class="ce-btn ce-btn--ink" type="button" data-eob="ok">OK</button></div>';
    box.appendChild(card); win.appendChild(box);
    const read = () => { const o = {}; for (const i of card.querySelectorAll('[data-eo]')) o[i.dataset.eo] = parseFloat(String(i.value).replace(',', '.').replace(/−/g, '-')); return o; };
    const draw = () => {
      const o = normOpt(Object.assign({}, OPT, read())), e = card.querySelector('.cer-tip ellipse'), a = 34, b = a * o.round / 100;
      e.setAttribute('rx', a); e.setAttribute('ry', Math.max(b, 0.6)); e.setAttribute('transform', `rotate(${-o.angle})`);
    };
    const close = ok => {
      if (ok) { OPT = normOpt(Object.assign({}, OPT, read())); OPT.mm = +OPT.mm.toFixed(3); saveOpt(); const st = S(ctx); if (st.lastP && ctx.tool() === 'eraser' && st.cur && !st.cur.hasAttribute('hidden')) placeCursor(ctx, st.lastP); syncBar(ctx); }
      box.remove();
      try { (ctx.root.querySelector('.ce-win') || ctx.root).focus({ preventScroll: true }); } catch (e) { /* fine */ }
    };
    const STEP = { mm: 0.1, angle: 1, round: 1 };
    const onKey = e => {
      if (e.key === 'Escape') { e.preventDefault(); close(false); return; }
      if (e.key === 'Enter' && !(e.target && e.target.tagName === 'BUTTON')) { e.preventDefault(); close(true); return; }
      const id = e.target && e.target.dataset && e.target.dataset.eo;
      if ((e.key === 'ArrowUp' || e.key === 'ArrowDown') && id) {
        e.preventDefault();
        const x = parseFloat(e.target.value), s = STEP[id] * (e.shiftKey ? 10 : 1);
        e.target.value = String(+((fin(x) ? x : 0) + (e.key === 'ArrowUp' ? s : -s)).toFixed(3)); draw();
      }
    };
    for (const el of card.querySelectorAll('input,button')) el.__ceKey = onKey;
    card.__ceKey = onKey; box.__ceKey = onKey;
    card.addEventListener('input', draw);
    card.addEventListener('click', e => { const b = e.target.closest('[data-eob]'); if (b) close(b.dataset.eob === 'ok'); });
    box.addEventListener('pointerdown', e => { if (e.target === box) { e.preventDefault(); close(false); } });
    draw();
    const first = card.querySelector('[data-eo]'); try { first.focus(); first.select(); } catch (e) { /* fine */ }
    return box;
  }

  /* ═══ 5 · REGISTERED with the editor's extension points: the tool (in the Scissors group, before the Scissors, as Illustrator's panel has it), its options ═══ */
  function register(r) {
    if (!r || !r.document) return;
    const CE = r.CharmEditor || (r.CharmEditor = {});
    const list = CE.tools || (CE.tools = []);
    if (!list.some(t => t && t.id === tool.id)) { const i = list.findIndex(t => t && t.id === 'scissors'); if (i >= 0) list.splice(i, 0, tool); else list.push(tool); }
    const cmds = CE.commands || (CE.commands = {});
    if (!cmds.eraserOptions) cmds.eraserOptions = { title: 'Eraser Tool Options…', menu: false, run: ctx => { optionsDialog(ctx); } };
    (CE.onOpen || (CE.onOpen = [])).push(ctx => {
      const dbl = e => { const b = e.target && e.target.closest && e.target.closest('[data-tool="eraser"]'); if (b && ctx.root.contains(b)) { e.preventDefault(); optionsDialog(ctx); } };
      ctx.root.addEventListener('dblclick', dbl);
      S(ctx).dbl = dbl;
    });
    (CE.onClose || (CE.onClose = [])).push(ctx => {
      if (!St || St.ctx !== ctx) return;
      if (St.dbl && ctx.root) ctx.root.removeEventListener('dblclick', St.dbl);
      if (St.leave && ctx.stage) ctx.stage.removeEventListener('pointerleave', St.leave);
      for (const el of [St.cur, St.cv, St.spin, St.bar]) if (el && el.remove) el.remove();
      St = null;
    });
  }

  return {
    tool, register, optionsDialog, tipOf, tipSub, support, sweep, rectSub, snap45, thin, eraserOf, targetsOf, eraseJob, placeErase, eraseItems, workOf, keepMatrix,
    options: () => Object.assign({}, OPT), setOptions: o => { OPT = normOpt(Object.assign({}, OPT, o || {})); saveOpt(); return Object.assign({}, OPT); },
    state: () => St, BIG, LIM, version: 1
  };
});
