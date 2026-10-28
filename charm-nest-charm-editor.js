/* charm-nest-charm-editor.js — THE CHARM EDITOR: one charm, its front and its back, every layer in reach (Paul, 27 Oct 2026: "The user needs full control similar to
 * Adobe Illustrator ... over each individual layer contained on any charm ... the ability to individually select and fully manipulate (size, position, rotation,
 * deletion, copy, paste, add/remove text, etc.) each layer of engravings and cut lines, outlines ... e.g. add a date inside a given calendar day square and update the
 * month name located on top of the calendar charm. This means the user needs to be able to zoom into a given charm while maintaining full vector file details and have
 * very fine and accurate control when adding text ... a watered-down version of Adobe Illustrator ... this is already a crowded interface ... I really like the UI and
 * the cleanliness of the UI design of the studio and I would like to replicate this design and functionality as much as possible ... wherever we can borrow a given
 * implementation, take it from the studio and adapt it ... I really liked the design of the icons, work area, etc.").
 *
 * This file is the EDITOR ONLY: a full-window workspace over the page that edits plain data and hands plain data back. It saves nothing, writes no laser file and
 * knows nothing of jobs, pools or sheets; the caller turns the result into records and files. Loads in the page (window.CharmEditor) and in node (module.exports, the
 * model only: node has no page to open a workspace on).
 *
 *   CharmEditor.open(opts) -> Promise<result | null>       null when the person cancels (Cancel, the ×, Esc with nothing selected); asked first when there are changes
 *     opts.title, opts.subtitle      "CALENDAR 86188 · SS" / "order 4213955608 · piece 2 of 3"
 *     opts.faces                     { front?: FaceDoc, back?: FaceDoc }     opts.startFace 'front' | 'back'
 *     opts.fonts                     [{ key, name }]                         the first is the default for new text
 *     opts.textToPaths(text, fontKey, sizePt, { align, lineGap }) -> { subpaths, bbox:[x0,y0,x1,y1] }
 *                                    the app's font engine: outlines in pt, y down, the anchor (start of the first baseline for 'left') at 0,0. Without it the
 *                                    editor uses CharmEditor.builtinTextToPaths, a plain single-stroke stand-in (for tests and for a page with no font engine)
 *     opts.lockRoles                 default ['cut']: items of these roles open locked (the Layers panel unlocks them); the lock the editor adds is not a change
 *
 *   FaceDoc  { face:'front'|'back', unitPt:true, frame:{ x0, y0, wPt, hPt }, items:[Item] }      points, y down; the frame is the charm's bounding view (shown in mm)
 *   Item     { id, kind:'path'|'text'|'group', role:'cut'|'line'|'hatch'|'art', source:'master'|'engrave'|'added', name, hidden, locked,
 *              subpaths:[[op,...],...], stroke, fill, lwPt,              (path; ops ["m",[x,y]] ["l",[x,y]] ["c",p1,p2,p3] ["h"])
 *              text, fontKey, sizePt, align, lineGap,                    (text)
 *              weightMm, lwPt                                            (27 Oct 2026: a filled text's or hatched part's WEIGHT, mm thicker (+) or thinner (−) than drawn: a text's
 *                                                                         outlines are made at it from its words; a path's geometry already has it in it, so a path that carries it
 *                                                                         is a weighted drawing (words saved and opened again). lwPt: a line's width, pt; none = the laser's 0.25 pt.
 *                                                                         A weight is made with the page's Clipper (window.ClipperLib); without it the Weight field is not offered)
 *              children:[Item],                                          (group)
 *              tx, ty, sx, sy, rot }                                     (every kind: a transform on top of the geometry)
 *     The transform is  T(c + t) · R(rot) · S(sx, sy) · T(-c)  where c is the centre of the TIGHT bounding box of the item's own geometry (Bézier extrema included;
 *     for a group, the union of its children as drawn; for text, the tight box of the outlines textToPaths returns, its bbox only when there is no ink) and rot
 *     is in degrees, clockwise on screen (y down, as SVG and CSS).
 *     So tx, ty move the centre, sx, sy scale about it and rot turns about it. A group's transform applies on top of its children's.
 *     Unknown fields on an item (layer, colours, evenOdd ...) travel through untouched.
 *
 *   result  { faces:{ front, back }, changes:{ front:{ removed:[ids], changed:{ [id]: Item }, added:[Item] }, back:{ ... } } }
 *     faces: the FaceDocs as edited (only the faces that were given). Every item in them carries its own transform; in addition
 *       - a text item carries `subpaths`: its outlines from textToPaths ALREADY TRANSFORMED into face coordinates (group transforms included) and `bbox` — pure
 *         vector; do not apply tx..rot to them again (those fields stay so the text can be edited again: open() makes a text's outlines afresh from its words)
 *       - a path or group item carries `matrix` [a,b,c,d,e,f] (its own transform as an SVG matrix, geometry -> parent) and a path its `pivot` (the c above), so
 *         nobody has to work out a bounding box the same way this file does
 *       - every item carries `bbox`: its tight box as drawn, in face coordinates (pt)
 *     changes, per face, relative to the FaceDocs given in (ids at any depth):
 *       removed  ids that are gone;  added  items whose ids are new (a new group listed once, with its children);
 *       changed  items still there whose own fields or whose group changed (a group is compared on its own fields; its children are listed on their own)
 *     z-order is in `faces`. A path that is turned and then stretched along a slant cannot be said with one turn and two scales: its geometry is redrawn (subpaths
 *     change, transform back to none), exactly as Illustrator does.
 *
 *   CharmEditor.current           while a workspace is open: small read-outs for tests and for the caller (face(), items(), selection(), zoom(), clientOf(x, y),
 *                                 box(id), result(), dirty(), tool()); null when closed
 *   CharmEditor.model             the pure parts, for node tests and for the caller (matrix maths, transforms, hit tests, history, clipboard, group/ungroup, diff)
 *   CharmEditor.builtinTextToPaths
 *
 * EXTENSION POINTS (28 Oct 2026, Design & Engrave: the pen, warp, path ops, image and vector import, the text tools and the design library are files of their own that
 * plug in here; Paul: "I just feel like I need more control and abilities that Photoshop and illustrator both have in this application"). A file registers by pushing
 * onto these lists at load time, before or after this file loads (`(window.CharmEditor && CharmEditor.tools || (CharmEditor.tools = [])).push(...)` after it; a plain
 * `window.CharmEditor = { tools: [...] }` set before it is taken in); the lists are read every time a workspace opens.
 *   CharmEditor.tools      [{ id, icon, title, key, cursor, onDown(ctx, ev, p), onMove(ctx, ev, p), onUp(ctx, ev, p), onKey?(ctx, ev), tip?, hint?, flyout?, overlay?(ctx),
 *                            onArm?(ctx), onDisarm?(ctx) }]
 *                          a button on the rail (before More). flyout: tools that name the same flyout share one rail button with a hold / right-click / corner menu, the
 *                          first is the default; flyout 'shapes' joins the Rectangle / Ellipse / Line menu. key: one letter that arms it (the editor's own letters win).
 *                          cursor: the CSS cursor over the work area while it is armed. p = { x, y } in pt (face coordinates). onMove is called while dragging and,
 *                          with no button down, on hover (ev.buttons says which). onKey returns true (or prevents the default) when it used the key. overlay(ctx)
 *                          returns SVG markup drawn over the charm in screen px (ctx.stageOf, ctx.view); ctx.redraw() asks for it again.
 *   CharmEditor.commands   { [id]: { title, key?, run(ctx), enabled?(ctx), group?, icon?, bar?, menu? } }
 *                          listed by group in the head's Object menu and, when enabled, in the right-click menu; bar: true with an icon puts a button on the property
 *                          bar while enabled; menu: false keeps it to its key. key: 'Mod+Shift+O', 'Alt+P', 'P' (Mod is ⌘ on a Mac, Ctrl elsewhere; the editor's own
 *                          keys win).
 *   CharmEditor.importers  [{ id, accept: ['.png', 'image/', ...], title, run(ctx, file, info) -> Promise }]
 *                          one Import entry for all of them: the head's Import button (a file picker of every accepted type), a file dropped on the work area
 *                          (info.at = [x, y] pt where it fell) and a file pasted (⌘V with a file or image on the system clipboard; the editor's own copy wins
 *                          over an older clipboard). A file goes to the first importer whose accept matches its extension or its type's prefix.
 *   CharmEditor.panels     [{ id, title, side: 'right', icon?, open?, mount(ctx, el), unmount?(), update?(ctx) }]
 *                          a panel slot to the right of the Layers, tabs when there are several, each with a head button that opens and folds it (remembered);
 *                          mount is called the first time its tab is shown, update (at most once a frame) after the work or the selection changes, unmount on close
 *   CharmEditor.onOpen     [fn(ctx)]              after a workspace opens          CharmEditor.onClose   [fn(ctx, result)]   as it closes (result null: cancelled)
 *   ctx (one per open workspace; also CharmEditor.current.ctx)
 *     doc                  the face being edited as a FaceDoc { face, unitPt, frame, items } (read-only: change it through the calls below)
 *     face() items() selection() frame() zoom() tool()      read-outs, as CharmEditor.current's
 *     select(ids)          the selection
 *     addItems(items, o)   new items on top (o.parent: inside that group; o.select false: the selection stays); returns their ids. Ids, kinds and roles are filled in
 *     replaceItems(map)    Map or { id: item | null } (null removes), at any depth;   removeItems(ids)
 *                          these three change the work AT ONCE (drawn in the same frame) and are gathered into ONE undo step by
 *     commit(label)        which closes the step; one is also made for you when a tool's gesture ends, a command or an importer finishes, the face or the tool changes
 *     clientOf(x, y) -> { x, y } client px;  pointOf(clientX, clientY) -> [x, y] pt;  stageOf(x, y) -> [x, y] px in the work area;  view() -> the pt -> work-area matrix
 *     setZoom(pct, about)  about: { x, y } client px (default the middle of the work area)
 *     geo                  the geometry provider (geo.raw(it), geo.box(it), geo.matrix(it), geo.drawn(it, P)); model: CharmEditor.model; entry(id) -> { it, P, parent }
 *     say(text, ms)        a short line over the work area;  redraw()  the selection furniture and overlays again;  setTool(id);  root, stage  the elements
 *
 * WHAT CAME FROM THE STUDIO (Brites Custom Studio, sections/brites-custom-studio.liquid "S" and assets/brites-custom-studio.js "J", brites-custom-studio.css "C",
 * brites-design-language.css "DL"): the design tokens (DL 19-27, C 6529-6536), the rail and its tiles, corner wedge, hold/right-click flyouts, hover cards and
 * wave (S 2108-2251, C 5049-5146, J 18674-19041), the property bar, size box and dropdowns (S 2257-2397, C 3572-3800), the rulers (J 19164-19327), the zoom
 * about the cursor and space/middle-button pan (J 13257-13268, 17201-17211, 18468-18507), the selection furniture (J 12238-12300, C 3948-4034), the transform
 * drags (J 17696-17840: Shift locks the ratio, a centred resize, Shift snaps the turn to 15°), the align engine (J 16670-16957), the snapshot undo (J 11732-11823),
 * the clipboard and keyboard map (J 18558-18615, 21920-21955), the one-textarea text editor (J 13030-13101), the layer rows, eye and lock icons (J 19492-19535, C
 * 4219-4331) and the Cancel / Save pair (S 2807-2815, C 5172-5184, 6654-6674). What the studio does not do, this does: it draws in SVG (crisp at 4000%), it has a
 * true 1 mm grid with 0.1 mm lines when zoomed in, X / Y are a position, and text comes back as outlines. */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === 'object' && module.exports) module.exports = api; else root.CharmEditor = api;
})(typeof self !== 'undefined' ? self : this, function (root) {
  'use strict';
  const doc = root && root.document ? root.document : null;
  const MM = 25.4 / 72, PT = 72 / 25.4;              // mm per pt, pt per mm
  const PX100 = 96 / 72;                              // CSS px per pt at 100%: true size, as Illustrator's 100% is
  const ZMIN = 0.25, ZMAX = 40;                       // 25% … 4000%
  const NUDGE_MM = 0.05, NUDGE_BIG_MM = 1, PASTE_MM = 0.5;   // (Paul, 27 Oct 2026, in another tool the same day: arrows 0.05 mm, Shift + arrow 1 mm; keep it the same)
  const ROLES = ['cut', 'line', 'hatch', 'art'];
  const ROLE_NAME = { cut: 'Cut line', line: 'Line engrave', hatch: 'Hatch engrave', art: 'Artwork' };
  const ROLE_SAYS = { cut: 'the laser cuts through', line: 'engraved along the line', hatch: 'engraved as a fill', art: "the charm's own drawing" };
  const ROLE_SHORT = { cut: 'Cut', line: 'Line', hatch: 'Hatch', art: 'Art' };
  // (a hidden part is not engraved: what the person sees is what the laser does. A cut line hidden stays cut: the outline is the piece itself)
  const eyeSays = it => it.role === 'cut' ? (it.hidden ? 'Show' : 'Hide on screen (the laser still cuts it)') : it.hidden ? 'Show: engraved again' : 'Hide: not engraved';
  const fin = v => typeof v === 'number' && Number.isFinite(v);
  const num = (v, d) => (fin(v) ? v : d);
  const clamp = (v, a, b) => Math.min(b, Math.max(a, v));
  const r9 = v => { const r = Math.round(v * 1e9) / 1e9; return Object.is(r, -0) ? 0 : r; };
  const wrap180 = d => { d = ((d % 360) + 360) % 360; return d > 180 ? d - 360 : d; };
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const trim = (v, dp) => String(+(+v).toFixed(dp));
  let idSeq = 0;
  const newId = () => 'ce' + Date.now().toString(36) + (++idSeq).toString(36);

  /* ═══ 1 · matrices (SVG order: [a,b,c,d,e,f] maps x,y to a·x + c·y + e, b·x + d·y + f) ═══ */
  const I = Object.freeze([1, 0, 0, 1, 0, 0]);
  const mul = (m, n) => [m[0] * n[0] + m[2] * n[1], m[1] * n[0] + m[3] * n[1], m[0] * n[2] + m[2] * n[3], m[1] * n[2] + m[3] * n[3],
    m[0] * n[4] + m[2] * n[5] + m[4], m[1] * n[4] + m[3] * n[5] + m[5]];
  const ap = (m, x, y) => [m[0] * x + m[2] * y + m[4], m[1] * x + m[3] * y + m[5]];
  function inv(m) {
    const det = m[0] * m[3] - m[1] * m[2]; if (!det) return null;
    const a = m[3] / det, b = -m[1] / det, c = -m[2] / det, d = m[0] / det;
    return [a, b, c, d, -(a * m[4] + c * m[5]), -(b * m[4] + d * m[5])];
  }
  const tr = (x, y) => [1, 0, 0, 1, x, y];
  const rotM = deg => { const r = deg * Math.PI / 180, c = Math.cos(r), s = Math.sin(r); return [c, s, -s, c, 0, 0]; };
  const scM = (x, y) => [x, 0, 0, y, 0, 0];
  const about = (m, cx, cy) => mul(tr(cx, cy), mul(m, tr(-cx, -cy)));
  const isI = m => Math.abs(m[0] - 1) < 1e-12 && Math.abs(m[1]) < 1e-12 && Math.abs(m[2]) < 1e-12 && Math.abs(m[3] - 1) < 1e-12 && Math.abs(m[4]) < 1e-9 && Math.abs(m[5]) < 1e-9;
  const mkey = m => m.map(v => v.toFixed(9)).join(',');

  /* ═══ 2 · geometry of subpaths ═══ */
  const bez = (a0, a1, a2, a3, t) => { const u = 1 - t; return u * u * u * a0 + 3 * u * u * t * a1 + 3 * u * t * t * a2 + t * t * t * a3; };
  /** The parameters in (0,1) where a cubic's derivative is zero on one axis: its extremes. */
  function cubicExt(a0, a1, a2, a3, add) {
    const d0 = a1 - a0, d1 = a2 - a1, d2 = a3 - a2;
    const A = d0 - 2 * d1 + d2, B = 2 * (d1 - d0), C = d0;
    if (Math.abs(A) < 1e-12) { if (Math.abs(B) > 1e-12) { const t = -C / B; if (t > 0 && t < 1) add(t); } return; }
    const disc = B * B - 4 * A * C; if (disc < 0) return;
    const s = Math.sqrt(disc);
    for (const t of [(-B + s) / (2 * A), (-B - s) / (2 * A)]) if (t > 0 && t < 1) add(t);
  }
  /** The TIGHT bounding box of subpaths, under an optional matrix (an affine map of a cubic is the cubic of its mapped control points, so this is exact). */
  function bboxOf(sps, m) {
    let x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity;
    const add = (x, y) => { if (x < x0) x0 = x; if (x > x1) x1 = x; if (y < y0) y0 = y; if (y > y1) y1 = y; };
    const P = m ? (p => ap(m, p[0], p[1])) : (p => p);
    for (const sub of sps || []) {
      let cur = null, start = null;
      for (const op of sub || []) {
        const k = op && op[0];
        if (k === 'm' || k === 'l') { const p = P(op[1]); add(p[0], p[1]); cur = p; if (k === 'm') start = p; }
        else if (k === 'c') {
          const p1 = P(op[1]), p2 = P(op[2]), p3 = P(op[3]); add(p3[0], p3[1]);
          if (cur) {
            const a = cur;
            cubicExt(a[0], p1[0], p2[0], p3[0], t => add(bez(a[0], p1[0], p2[0], p3[0], t), bez(a[1], p1[1], p2[1], p3[1], t)));
            cubicExt(a[1], p1[1], p2[1], p3[1], t => add(bez(a[0], p1[0], p2[0], p3[0], t), bez(a[1], p1[1], p2[1], p3[1], t)));
          } else { add(p1[0], p1[1]); add(p2[0], p2[1]); }
          cur = p3;
        } else if (k === 'h') cur = start;
      }
    }
    return x0 <= x1 ? [x0, y0, x1, y1] : null;
  }
  const union = (a, b) => (!a ? (b ? b.slice() : null) : !b ? a.slice() : [Math.min(a[0], b[0]), Math.min(a[1], b[1]), Math.max(a[2], b[2]), Math.max(a[3], b[3])]);
  const centreOf = b => (b ? [(b[0] + b[2]) / 2, (b[1] + b[3]) / 2] : [0, 0]);
  function transformSubpaths(sps, m) {
    const P = p => { const q = ap(m, p[0], p[1]); return [r9(q[0]), r9(q[1])]; };
    return (sps || []).map(sub => (sub || []).map(op => {
      const k = op && op[0];
      if (k === 'm' || k === 'l') return [k, P(op[1])];
      if (k === 'c') return ['c', P(op[1]), P(op[2]), P(op[3])];
      return op.slice ? op.slice() : op;
    }));
  }
  /** Polylines of subpaths (cubics cut into short chords), cached by the subpaths array: for hit tests and the marquee. */
  const flatCache = new WeakMap();
  function flatten(sps) {
    if (!sps) return [];
    let f = flatCache.get(sps); if (f) return f;
    f = [];
    for (const sub of sps) {
      let pts = [], closed = false, cur = null;
      for (const op of sub || []) {
        const k = op && op[0];
        if (k === 'm') { if (pts.length >= 2) f.push({ pts, closed }); pts = [op[1][0], op[1][1]]; closed = false; cur = op[1]; }
        else if (k === 'l') { if (!cur) pts = []; pts.push(op[1][0], op[1][1]); cur = op[1]; }
        else if (k === 'c') {
          const a = cur || op[1], p1 = op[1], p2 = op[2], p3 = op[3];
          const len = Math.hypot(p1[0] - a[0], p1[1] - a[1]) + Math.hypot(p2[0] - p1[0], p2[1] - p1[1]) + Math.hypot(p3[0] - p2[0], p3[1] - p2[1]);
          const n = clamp(Math.ceil(len / 0.35), 2, 48);
          if (!cur) pts.push(a[0], a[1]);
          for (let i = 1; i <= n; i++) { const t = i / n; pts.push(bez(a[0], p1[0], p2[0], p3[0], t), bez(a[1], p1[1], p2[1], p3[1], t)); }
          cur = p3;
        } else if (k === 'h') closed = true;
      }
      if (pts.length >= 2) f.push({ pts, closed });
    }
    flatCache.set(sps, f); return f;
  }
  const dCache = new WeakMap();
  /** SVG path data of subpaths (pt, four decimals: a tenth of a micron). */
  function dOf(sps) {
    if (!sps) return '';
    let d = dCache.get(sps); if (d != null) return d;
    const f = v => Math.round(v * 1e4) / 1e4;
    d = '';
    for (const sub of sps) for (const op of sub || []) {
      const k = op && op[0];
      if (k === 'm') d += 'M' + f(op[1][0]) + ' ' + f(op[1][1]);
      else if (k === 'l') d += 'L' + f(op[1][0]) + ' ' + f(op[1][1]);
      else if (k === 'c') d += 'C' + f(op[1][0]) + ' ' + f(op[1][1]) + ' ' + f(op[2][0]) + ' ' + f(op[2][1]) + ' ' + f(op[3][0]) + ' ' + f(op[3][1]);
      else if (k === 'h') d += 'Z';
    }
    dCache.set(sps, d); return d;
  }
  const KAPPA = 0.5522847498;
  const rectSP = (x0, y0, x1, y1) => [[['m', [x0, y0]], ['l', [x1, y0]], ['l', [x1, y1]], ['l', [x0, y1]], ['h']]];
  function ellipseSP(cx, cy, rx, ry) {
    const kx = rx * KAPPA, ky = ry * KAPPA;
    return [[['m', [cx + rx, cy]], ['c', [cx + rx, cy + ky], [cx + kx, cy + ry], [cx, cy + ry]], ['c', [cx - kx, cy + ry], [cx - rx, cy + ky], [cx - rx, cy]],
      ['c', [cx - rx, cy - ky], [cx - kx, cy - ry], [cx, cy - ry]], ['c', [cx + kx, cy - ry], [cx + rx, cy - ky], [cx + rx, cy]], ['h']]];
  }
  const lineSP = (x0, y0, x1, y1) => [[['m', [x0, y0]], ['l', [x1, y1]]]];

  /* ═══ 2b · weight: a filled shape grown or thinned (Paul, 27 Oct 2026: "I need the ability to control the thickness of fonts as the current 'pt' control does not correctly
     control the boldness of the text. I need fine font thickness control over each font") ═══
     A polygon offset of the shape itself, so it works the same for every font (most of the shop's fonts have one weight file, the scripts among them, and none has a lighter one):
     the outlines cut into chords never more than 0.01 pt off the curve, made ONE clean shape (non-zero, as the laser fills them; even-odd when the part says so), then every edge
     moved out by d (in, for a negative d) with mitred corners, by Clipper (vendor/clipper-6.4.2.js, the page's own; charm-nest-monogram.js grows its letters with it). These are the
     outlines the laser gets: nothing is drawn one way and cut another. A weight of nothing is the drawing itself, curves and all. */
  const clipperLib = () => (root && root.ClipperLib) || (typeof require === 'function' ? (() => { try { return require('./vendor/clipper-6.4.2.js'); } catch (e) { return null; } })() : null);
  const canWeigh = () => !!clipperLib();
  const WSC = 1e4, FLAT_PT = 0.01;   // (Clipper works in integers: a ten-thousandth of a point; a chord strays at most 0.01 pt, 3.5 µm, from its curve)
  function ringsOf(sps) {
    const out = [];
    const P = p => ({ X: Math.round(p[0] * WSC), Y: Math.round(p[1] * WSC) });
    for (const sub of sps || []) {
      let ring = [], cur = null;
      const end = () => { if (ring.length >= 3) out.push(ring); ring = []; };
      for (const op of sub || []) {
        const k = op && op[0];
        if (k === 'm') { end(); ring.push(P(op[1])); cur = op[1]; }
        else if (k === 'l') { ring.push(P(op[1])); cur = op[1]; }
        else if (k === 'c') {
          const a = cur || op[1], p1 = op[1], p2 = op[2], p3 = op[3];
          // (n chords keep a cubic within 3/4 · max|second difference| / n² of itself)
          const dd = Math.max(Math.hypot(a[0] - 2 * p1[0] + p2[0], a[1] - 2 * p1[1] + p2[1]), Math.hypot(p1[0] - 2 * p2[0] + p3[0], p1[1] - 2 * p2[1] + p3[1]));
          const n = clamp(Math.ceil(Math.sqrt(0.75 * dd / FLAT_PT)), 1, 64);
          for (let i = 1; i <= n; i++) { const t = i / n; ring.push(P([bez(a[0], p1[0], p2[0], p3[0], t), bez(a[1], p1[1], p2[1], p3[1], t)])); }
          cur = p3;
        }
      }
      end();
    }
    return out;
  }
  /** Subpaths (pt) as a filled shape grown by d pt (thinned for a negative d); the same subpaths when d is nothing or the page has no Clipper. */
  function weighSubpaths(sps, d, evenOdd) {
    const C = clipperLib();
    if (!C || !fin(d) || Math.abs(d) < 1e-6 || !sps || !sps.length) return sps;
    const rings = ringsOf(sps); if (!rings.length) return sps;
    const ft = evenOdd ? C.PolyFillType.pftEvenOdd : C.PolyFillType.pftNonZero;
    const cl = new C.Clipper(), shape = new C.Paths();
    cl.AddPaths(rings, C.PolyType.ptSubject, true);
    if (!cl.Execute(C.ClipType.ctUnion, shape, ft, ft)) return sps;
    const co = new C.ClipperOffset(2, 0.25 * FLAT_PT * WSC), grown = new C.Paths();
    co.AddPaths(shape, C.JoinType.jtMiter, C.EndType.etClosedPolygon);
    co.Execute(grown, d * WSC);
    const out = [];
    for (const ring of C.Clipper.CleanPolygons(grown, 0.002 * WSC)) {
      if (ring.length < 3 || Math.abs(C.Clipper.Area(ring)) < 1e-6 * WSC * WSC) continue;
      out.push(ring.map((q, i) => [i ? 'l' : 'm', [q.X / WSC, q.Y / WSC]]).concat([['h']]));
    }
    return out;
  }
  /** The inked area of filled subpaths, pt² (non-zero): for tests that a heavier weight is more ink. */
  function inkArea(sps) {
    const C = clipperLib(); if (!C) return NaN;
    const cl = new C.Clipper(), shape = new C.Paths();
    cl.AddPaths(ringsOf(sps), C.PolyType.ptSubject, true); cl.Execute(C.ClipType.ctUnion, shape, C.PolyFillType.pftNonZero, C.PolyFillType.pftNonZero);
    return shape.reduce((a, r) => a + C.Clipper.Area(r), 0) / (WSC * WSC);
  }

  /* ═══ 3 · the stand-in font: single strokes on a 4 × 6 grid, outlined as square-capped bars (nonzero fill unions them) ═══ */
  const GLYPHS = {
    A: '0,6 0,2 2,0 4,2 4,6|0,3.6 4,3.6', B: '0,3 3,3 4,4 4,5 3,6 0,6 0,0 3,0 4,1 4,2 3,3', C: '4,1 3,0 1,0 0,1 0,5 1,6 3,6 4,5', D: '0,0 0,6 3,6 4,5 4,1 3,0 0,0',
    E: '4,0 0,0 0,6 4,6|0,3 3,3', F: '4,0 0,0 0,6|0,3 3,3', G: '4,1 3,0 1,0 0,1 0,5 1,6 3,6 4,5 4,3.4 2.2,3.4', H: '0,0 0,6|4,0 4,6|0,3 4,3',
    I: '1,0 3,0|2,0 2,6|1,6 3,6', J: '4,0 4,5 3,6 1,6 0,5', K: '0,0 0,6|4,0 0,3.6|1.4,2.6 4,6', L: '0,0 0,6 4,6', M: '0,6 0,0 2,3 4,0 4,6', N: '0,6 0,0 4,6 4,0',
    O: '1,0 3,0 4,1 4,5 3,6 1,6 0,5 0,1 1,0', P: '0,6 0,0 3,0 4,1 4,2 3,3 0,3', Q: '1,0 3,0 4,1 4,5 3,6 1,6 0,5 0,1 1,0|2.6,4.6 4,6',
    R: '0,6 0,0 3,0 4,1 4,2 3,3 0,3|2,3 4,6', S: '4,1 3,0 1,0 0,1 0,2 1,3 3,3 4,4 4,5 3,6 1,6 0,5', T: '0,0 4,0|2,0 2,6', U: '0,0 0,5 1,6 3,6 4,5 4,0',
    V: '0,0 2,6 4,0', W: '0,0 1,6 2,3 3,6 4,0', X: '0,0 4,6|4,0 0,6', Y: '0,0 2,3 4,0|2,3 2,6', Z: '0,0 4,0 0,6 4,6',
    0: '1,0 3,0 4,1 4,5 3,6 1,6 0,5 0,1 1,0|3.6,0.8 0.4,5.2', 1: '1,1.2 2,0 2,6|1,6 3,6', 2: '0,1 1,0 3,0 4,1 4,2 0,6 4,6',
    3: '0,1 1,0 3,0 4,1 4,2 3,3 1.6,3|3,3 4,4 4,5 3,6 1,6 0,5', 4: '3,6 3,0 0,4 4,4', 5: '4,0 0,0 0,3 3,3 4,4 4,5 3,6 1,6 0,5',
    6: '4,1 3,0 1,0 0,1 0,5 1,6 3,6 4,5 4,4 3,3 0,3', 7: '0,0 4,0 1.6,6', 8: '1,3 0,2 0,1 1,0 3,0 4,1 4,2 3,3 1,3 0,4 0,5 1,6 3,6 4,5 4,4 3,3',
    9: '4,3 1,3 0,2 0,1 1,0 3,0 4,1 4,5 3,6 1,6 0,5', '.': '2,5.7 2,6', ',': '2.1,5.4 1.6,6.9', '-': '1,3 3,3', '/': '4,0 0,6', "'": '2,0 2,1.6',
    '!': '2,0 2,4|2,5.7 2,6', '?': '0,1 1,0 3,0 4,1 4,2 2,3.4 2,4.2|2,5.7 2,6', ':': '2,1.9 2,2.2|2,5.7 2,6', '&': '4,6 1,2 1,1 2,0 3,1 3,2 0,4 0,5 1,6 2,6 4,4',
    '+': '0,3 4,3|2,1 2,5', '#': '1,0 1,6|3,0 3,6|0,2 4,2|0,4 4,4', '"': '1.3,0 1.3,1.6|2.7,0 2.7,1.6', '(': '2.6,0 1.4,1.5 1.4,4.5 2.6,6', ')': '1.4,0 2.6,1.5 2.6,4.5 1.4,6'
  };
  const glyphCache = new Map();
  const strokesOf = ch => {
    if (glyphCache.has(ch)) return glyphCache.get(ch);
    const src = GLYPHS[ch];
    const s = src ? src.split('|').map(run => run.trim().split(/\s+/).map(p => p.split(',').map(Number))) : [[[0, 0], [4, 0], [4, 6], [0, 6], [0, 0]]];
    glyphCache.set(ch, s); return s;
  };
  /** The stand-in for the app's font engine: legible capitals, figures and common marks as filled outlines (each stroke a square-capped bar; all bars wind the same
   *  way, so a nonzero fill makes one shape). Lower case is drawn as small capitals. lineGap is a factor of the size (default 1.25). The anchor is the start of the
   *  first baseline ('left'), its middle ('center') or its end ('right'). */
  function builtinTextToPaths(text, fontKey, sizePt, o) {
    o = o || {};
    const size = fin(+sizePt) && +sizePt > 0 ? +sizePt : 10;
    const u = size * 0.7 / 6, hw = u * (/bold|heavy|black/i.test(String(fontKey || '')) ? 0.62 : 0.46);
    const lead = size * (fin(+o.lineGap) && +o.lineGap > 0 ? +o.lineGap : 1.25);
    const subpaths = [];
    const bar = (ax, ay, bx, by) => {
      let dx = bx - ax, dy = by - ay; const L = Math.hypot(dx, dy);
      if (L < 1e-9) { dx = 1; dy = 0; } else { dx /= L; dy /= L; }
      const nx = -dy * hw, ny = dx * hw, ex = dx * hw, ey = dy * hw;
      subpaths.push([['m', [ax + nx - ex, ay + ny - ey]], ['l', [bx + nx + ex, by + ny + ey]], ['l', [bx - nx + ex, by - ny + ey]], ['l', [ax - nx - ex, ay - ny - ey]], ['h']]);
    };
    const lines = String(text == null ? '' : text).split('\n');
    lines.forEach((line, li) => {
      const chars = Array.from(line);
      const adv = ch => (ch === ' ' ? 3 * u : (/[a-z]/.test(ch) ? 0.86 : 1) * 5.2 * u);
      let width = 0; chars.forEach((ch, i) => { width += i === chars.length - 1 && ch !== ' ' ? (/[a-z]/.test(ch) ? 0.86 : 1) * 4 * u : adv(ch); });
      let x = o.align === 'center' ? -width / 2 : o.align === 'right' ? -width : 0;
      const base = li * lead;
      for (const ch of chars) {
        if (ch !== ' ') {
          const small = /[a-z]/.test(ch), kx = small ? 0.86 : 1, ky = small ? 0.74 : 1;
          for (const run of strokesOf(small ? ch.toUpperCase() : ch)) {
            const pts = run.map(p => [x + p[0] * u * kx, base + (p[1] - 6) * u * ky]);
            if (pts.length === 1) bar(pts[0][0], pts[0][1], pts[0][0], pts[0][1]);
            for (let i = 1; i < pts.length; i++) bar(pts[i - 1][0], pts[i - 1][1], pts[i][0], pts[i][1]);
          }
        }
        x += adv(ch);
      }
    });
    return { subpaths, bbox: bboxOf(subpaths) };
  }

  /* ═══ 4 · items: geometry, transforms ═══ */
  const T0 = it => !num(it.tx, 0) && !num(it.ty, 0) && num(it.sx, 1) === 1 && num(it.sy, 1) === 1 && !num(it.rot, 0);
  /* WEIGHT AND LINE WIDTH (27 Oct 2026). weightMm: how much thicker (thinner, below 0) every stroke of a filled shape is than its own drawing, in mm on the charm: a text's outlines
     are made from its words at that weight every time; a path's geometry already HAS its weight in it (a text saved and opened again comes back as its outlines with the weight it was
     saved with), so a new weight grows it by the difference. lwPt: a line's width; a line that names none is drawn at the width the laser file gives it (LW_LASER_PT). */
  const WT_MIN_MM = -0.3, WT_MAX_MM = 0.6, WT_STEP_MM = 0.01;
  const LW_LASER_PT = 0.25, LW_MIN_MM = 0.01, LW_MAX_MM = 2;   // (0.25 pt: what charm-nest-art-edit.js paintOf and charm-nest-pdf.js artOps write for a line that names no width)
  const weightOf = it => { const w = num(it && it.weightMm, 0); return Math.abs(w) < 1e-9 ? 0 : r9(clamp(w, WT_MIN_MM, WT_MAX_MM)); };
  const lwOf = it => { const v = it && it.lwPt != null && it.lwPt !== '' ? +it.lwPt : NaN; return fin(v) && v >= 0 ? v : LW_LASER_PT; };
  /** Can this part take a weight: a filled text, a hatched path, or filled words saved as their outlines (a path that keeps its words). A line engraving takes a width instead. */
  const weighable = it => !!it && (it.kind === 'text' || (it.kind === 'path' && (it.role === 'hatch' || (it.text != null && String(it.text) !== '')))) && paintFill(it);
  const EMPTY = Object.freeze([]);
  /** The geometry provider: raw outlines of a leaf (a path's own subpaths, a text's outlines from the font engine, cached by its words, font, size, align and line
   *  gap), the raw box of any item and its transform matrix. Every pure function below takes one, so the font engine is a parameter and not a global. */
  function createGeo(textToPaths) {
    const engine = typeof textToPaths === 'function' ? textToPaths : builtinTextToPaths;
    const tcache = new Map(), pcache = new WeakMap();
    function textGeom(it) {
      const words = it.text == null ? '' : String(it.text);
      const wt = weightOf(it);
      const key = [words, it.fontKey == null ? '' : it.fontKey, num(it.sizePt, 10), it.align || 'left', it.lineGap == null ? '' : it.lineGap, wt].join('\u0001');
      let g = tcache.get(key);
      if (g) { tcache.delete(key); tcache.set(key, g); return g; }
      let r = null;
      if (words.trim()) { try { r = engine(words, it.fontKey, num(it.sizePt, 10), { align: it.align || 'left', lineGap: it.lineGap }); } catch (e) { r = null; } }
      let subpaths = r && Array.isArray(r.subpaths) ? r.subpaths : [];
      // (the weight: the font's own drawing grown or thinned by half of it on every edge, so a stroke of the letters is that much thicker; the engine always draws the font's own)
      if (wt && subpaths.length) subpaths = weighSubpaths(subpaths, wt * PT / 2, false);
      // the words' own box is the tight box of their outlines, as a path's is (an engine's bbox may be a layout box, with the ascent and the side bearings in it, and
      // the handles, the pivot and X / Y / W / H must all be about the same box); the engine's is used only when there is no ink
      const b = bboxOf(subpaths) || (r && Array.isArray(r.bbox) && r.bbox.length === 4 && r.bbox.every(fin) && r.bbox[2] >= r.bbox[0] && r.bbox[3] >= r.bbox[1] ? r.bbox.slice() : null);
      g = { subpaths, bbox: b };
      tcache.set(key, g); if (tcache.size > 800) tcache.delete(tcache.keys().next().value);
      return g;
    }
    function raw(it) {
      if (it.kind === 'text') return textGeom(it);
      const sp = Array.isArray(it.subpaths) ? it.subpaths : EMPTY;
      let g = pcache.get(sp);
      if (!g) { g = { subpaths: sp, bbox: bboxOf(sp) }; pcache.set(sp, g); }
      return g;
    }
    function box(it) {
      if (it.kind === 'group') { let b = null; for (const ch of it.children || []) b = union(b, drawn(ch, I)); return b; }
      return raw(it).bbox;
    }
    function matrix(it) {
      if (T0(it)) return I;
      const c = centreOf(box(it));
      const r = num(it.rot, 0) * Math.PI / 180, cs = Math.cos(r), sn = Math.sin(r), sx = num(it.sx, 1), sy = num(it.sy, 1);
      const a = cs * sx, b = sn * sx, cc = -sn * sy, d = cs * sy;
      return [a, b, cc, d, c[0] + num(it.tx, 0) - (a * c[0] + cc * c[1]), c[1] + num(it.ty, 0) - (b * c[0] + d * c[1])];
    }
    /** Tight box as drawn, under the parent's matrix P. */
    function drawn(it, P) {
      const W = mul(P || I, matrix(it));
      if (it.kind === 'group') { let b = null; for (const ch of it.children || []) if (!ch.hidden) b = union(b, drawn(ch, W)); return b; }
      const g = raw(it);
      const b = bboxOf(g.subpaths, W);
      if (b || it.kind !== 'text') return b;
      const q = quadOf(g.bbox, W); return q ? polyBox(q) : null;
    }
    return { raw, box, matrix, drawn, engine };
  }
  const quadOf = (b, W) => (b ? [ap(W, b[0], b[1]), ap(W, b[2], b[1]), ap(W, b[2], b[3]), ap(W, b[0], b[3])] : null);
  const polyBox = q => { let b = null; for (const p of q) b = union(b, [p[0], p[1], p[0], p[1]]); return b; };

  /** Turn and two scales of a 2 × 2 map (its columns must be at right angles; ortho says whether they were). The sign of sx follows `sign` so a mirrored item stays
   *  mirrored the same way rather than flipping into "turned 180° and mirrored the other way", which draws the same and reads as a change nobody made. */
  function decompose(L, sign) {
    const a = L[0], b = L[1], c = L[2], d = L[3];
    const n1 = Math.hypot(a, b), n2 = Math.hypot(c, d);
    if (n1 < 1e-12 || n2 < 1e-12) return { sx: 1, sy: 1, rot: 0, ortho: false };
    const ortho = Math.abs(a * c + b * d) <= 1e-7 * n1 * n2;
    const sx = (sign < 0 ? -1 : 1) * n1, det = a * d - b * c;
    return { sx, sy: det / sx, rot: wrap180(Math.atan2(b / sx, a / sx) * 180 / Math.PI), ortho };
  }
  /** A leaf with the face-space map M applied on top of its own transform. Exact whenever the result is a turn and two scales; a path that would need a slant is
   *  redrawn (its geometry baked, its transform back to none); a text keeps its turn and the scales that keep its area (text has no slant to give). */
  function composeInto(it, M, geo) {
    const W = mul(M, geo.matrix(it));
    const c = centreOf(geo.box(it));
    const dec = decompose(W, num(it.sx, 1) < 0 ? -1 : 1);
    if (dec.ortho || it.kind === 'text') {
      const pc = ap(W, c[0], c[1]);
      const snap1 = v => (Math.abs(v - 1) < 1e-9 ? 1 : Math.abs(v + 1) < 1e-9 ? -1 : r9(v));
      return Object.assign({}, it, { tx: r9(pc[0] - c[0]), ty: r9(pc[1] - c[1]), sx: snap1(dec.sx), sy: snap1(dec.sy), rot: Math.abs(dec.rot) < 1e-9 ? 0 : r9(dec.rot) });
    }
    return Object.assign({}, it, { subpaths: transformSubpaths(geo.raw(it).subpaths, W), tx: 0, ty: 0, sx: 1, sy: 1, rot: 0 });
  }
  /** M applied to an item from the outside: a leaf takes it (unless locked), a group passes it, with its own transform, down to every leaf and keeps none itself.
   *  So transforms always end on the leaves and a group's own transform is back to none after any edit; `acc` carries the transforms of the groups above.
   *  M null: only the groups' own transforms are pushed down (normalising). force: locks are ignored (the clipboard, normalising). */
  function pushDown(it, M, geo, acc, force) {
    acc = acc || I;
    if (it.kind === 'group') {
      const G = mul(acc, geo.matrix(it));
      const lockAll = !force && !!it.locked;
      let same = T0(it);
      const kids = (it.children || []).map(ch => { const n = pushDown(ch, lockAll ? null : M, geo, G, force); if (n !== ch) same = false; return n; });
      return same ? it : Object.assign({}, it, { tx: 0, ty: 0, sx: 1, sy: 1, rot: 0, children: kids });
    }
    const eff = M && (force || !it.locked) ? mul(M, acc) : acc;
    return isI(eff) ? it : composeInto(it, eff, geo);
  }
  /** Words, font, size or alignment changed: the new outlines are placed so the text's anchor stays where it was on the charm (as Illustrator keeps a point text's
   *  origin), whatever its turn and scale. */
  function retext(it, patch, geo) {
    const n = Object.assign({}, it, patch);
    if (T0(it)) return n;
    const c0 = centreOf(geo.box(it)), c1 = centreOf(geo.box(n));
    const L = geo.matrix(it);
    const lin = (x, y) => [L[0] * x + L[2] * y, L[1] * x + L[3] * y];
    const a = lin(c0[0], c0[1]), b = lin(c1[0], c1[1]);
    n.tx = r9(num(it.tx, 0) + c0[0] - a[0] - c1[0] + b[0]); n.ty = r9(num(it.ty, 0) + c0[1] - a[1] - c1[1] + b[1]);
    return n;
  }
  /** A text stretched evenly takes the stretch into its point size (its outlines are drawn at that size and placed where they were), so the size on the bar is the
   *  size on the charm. Uneven stretches stay as sx / sy. */
  function foldTextScale(it, geo) {
    if (it.kind !== 'text') return it;
    const sx = num(it.sx, 1), sy = num(it.sy, 1);
    if (!(sx > 0) || Math.abs(sx - sy) > 1e-6 * sx || Math.abs(sx - 1) < 1e-9) return it;
    const n = retext(Object.assign({}, it, { sx: 1, sy: 1 }), { sizePt: r9(num(it.sizePt, 10) * sx) }, geo);
    // retext kept the anchor under a 1:1 map; with the old scale folded in, the anchor under the OLD map must land where the new map puts it
    const W0 = geo.matrix(it), W1 = geo.matrix(n);
    const p0 = ap(W0, 0, 0), p1 = ap(W1, 0, 0);
    n.tx = r9(num(n.tx, 0) + p0[0] - p1[0]); n.ty = r9(num(n.ty, 0) + p0[1] - p1[1]);
    return n;
  }
  /** A part at a new weight (mm), drawn where it was: a text makes its outlines at that weight from its words; a path's own geometry (which already has its weight in it) is
   *  grown by the difference, in its own frame and so divided by its own scale, and placed so nothing else of it moves. Weight 0 is the part's own drawing (the field goes). */
  function weighItem(it, wMm, geo) {
    if (!weighable(it)) return it;
    const w1 = clamp(r9(+wMm || 0), WT_MIN_MM, WT_MAX_MM), w0 = weightOf(it);
    if (Math.abs(w1 - w0) < 1e-9) return it;
    const bare = n => { if (!n.weightMm) delete n.weightMm; return n; };
    if (it.kind === 'text') return bare(retext(it, { weightMm: w1 }, geo));
    const s = Math.sqrt(Math.abs(num(it.sx, 1) * num(it.sy, 1))) || 1;
    const sp = weighSubpaths(geo.raw(it).subpaths, (w1 - w0) * PT / 2 / s, !!(it.evenOdd || it.even || it.fillRule === 'evenodd'));
    if (sp === geo.raw(it).subpaths) return it;
    return bare(retext(it, { subpaths: sp, weightMm: w1 }, geo));
  }

  /* ═══ 5 · the tree (immutable: every edit makes new arrays and objects where it changes, so a snapshot is a reference) ═══ */
  function walk(items, fn, parent, depth) {
    (items || []).forEach((it, i) => { fn(it, parent || null, i, depth || 0); if (it.kind === 'group') walk(it.children, fn, it, (depth || 0) + 1); });
  }
  /** id -> { it, parent, i, P (the parent's matrix in face space), anc (ancestor ids, top first) } */
  function indexTree(items, geo) {
    const map = new Map();
    const rec = (list, parent, P, anc) => (list || []).forEach((it, i) => {
      map.set(it.id, { it, parent, i, P, anc });
      if (it.kind === 'group') rec(it.children, it, mul(P, geo.matrix(it)), anc.concat(it.id));
    });
    rec(items, null, I, []);
    return map;
  }
  /** Items replaced (map id -> item) or removed (map id -> null), at any depth; a group left empty goes too (unless keepEmpty: a move that empties a group
   *  and fills it again in the same breath must not lose it on the way). */
  function replaceIn(items, map, keepEmpty) {
    let changed = false; const out = [];
    for (const it of items) {
      if (map.has(it.id)) { changed = true; const n = map.get(it.id); if (n) out.push(n); continue; }
      if (it.kind === 'group' && it.children) {
        const ch = replaceIn(it.children, map, keepEmpty);
        if (ch !== it.children) { changed = true; if (ch.length || keepEmpty) out.push(Object.assign({}, it, { children: ch })); continue; }
      }
      out.push(it);
    }
    return changed ? out : items;
  }
  const removeIds = (items, ids, keepEmpty) => replaceIn(items, new Map(ids.map(id => [id, null])), keepEmpty);
  /** Groups with nothing in them taken out. */
  function pruneEmpty(items) {
    let changed = false; const out = [];
    for (const it of items) {
      if (it.kind === 'group') {
        const ch = pruneEmpty(it.children || []);
        if (!ch.length) { changed = true; continue; }
        if (ch !== it.children) { changed = true; out.push(Object.assign({}, it, { children: ch })); continue; }
      }
      out.push(it);
    }
    return changed ? out : items;
  }
  /** The children of a parent (null: the top level) replaced by `list`. */
  function setList(items, parentId, list) {
    if (parentId == null) return list;
    return replaceInDeep(items, parentId, g => Object.assign({}, g, { children: list }));
  }
  function replaceInDeep(items, id, fn) {
    let hit = false;
    const rec = list => list.map(it => {
      if (it.id === id) { hit = true; return fn(it); }
      if (it.kind === 'group' && it.children) { const ch = rec(it.children); if (ch.some((c, i) => c !== it.children[i])) return Object.assign({}, it, { children: ch }); }
      return it;
    });
    const out = rec(items); return hit ? out : items;
  }
  const listOf = (items, parentId, ix) => (parentId == null ? items : ((ix.get(parentId) || {}).it || {}).children || []);
  /** Ids with none of their ancestors also listed (a group and its child both selected would move the child twice). */
  function topmost(ids, ix) {
    const set = new Set(ids.filter(id => ix.has(id)));
    return [...set].filter(id => !ix.get(id).anc.some(a => set.has(a)));
  }
  /** Every ancestor group of these ids with a transform of its own pushes it down to its leaves first, so an edit inside a group works in face space. */
  function normaliseAncestors(items, ids, geo) {
    let ix = indexTree(items, geo);
    for (const id of ids) {
      const e = ix.get(id); if (!e) continue;
      const top = e.anc.find(a => !T0(ix.get(a).it));
      if (!top) continue;
      const t = ix.get(top);
      items = replaceIn(items, new Map([[top, pushDown(t.it, null, geo, t.P, true)]]));
      ix = indexTree(items, geo);
    }
    return items;
  }
  /** Clean copies for the clipboard, each in face space (the groups above it pushed into it). */
  function copyItems(items, ids, geo) {
    const ix = indexTree(items, geo);
    return topmost(ids, ix).map(id => { const e = ix.get(id); return pushDown(e.it, null, geo, e.P, true); });
  }
  function reid(it, idf) {
    const o = Object.assign({}, it, { id: (idf || newId)(), source: 'added' });
    delete o._autoLock; delete o._lockedIn;
    if (it.kind === 'group') o.children = (it.children || []).map(ch => reid(ch, idf));
    return o;
  }
  /** Clipboard items as new items (new ids, source 'added'), moved by dx, dy pt. */
  const pasteItems = (clip, dx, dy, geo, idf) => (clip || []).map(it => pushDown(reid(it, idf), (dx || dy) ? tr(dx, dy) : null, geo, I, true));
  /** The ids as one new group, in their stacking order, where the topmost of them was. */
  function groupItems(items, ids, geo, gid) {
    items = normaliseAncestors(items, ids, geo);
    let ix = indexTree(items, geo);
    const sel = topmost(ids, ix);
    if (!sel.length) return { items, id: null };
    const order = []; walk(items, it => { if (sel.includes(it.id)) order.push(it.id); });
    const topId = order[order.length - 1], te = ix.get(topId);
    const parentId = te.parent ? te.parent.id : null;
    const sibs = listOf(items, parentId, ix);
    const at = sibs.slice(0, te.i).filter(s => !sel.includes(s.id)).length;
    const kids = order.map(id => ix.get(id).it);
    const roles = {}; kids.forEach(k => { roles[k.role] = (roles[k.role] || 0) + 1; });
    const role = Object.keys(roles).sort((a, b) => roles[b] - roles[a])[0] || 'art';
    const g = { id: gid || newId(), kind: 'group', role, source: 'added', name: 'Group', hidden: false, locked: false, tx: 0, ty: 0, sx: 1, sy: 1, rot: 0, children: kids };
    items = removeIds(items, sel, true);
    ix = indexTree(items, geo);
    const list = listOf(items, parentId, ix).slice();
    list.splice(Math.min(at, list.length), 0, g);
    return { items: pruneEmpty(setList(items, parentId, list)), id: g.id };
  }
  /** The group's children take its place (its transform pushed into them). */
  function ungroupItem(items, gid, geo) {
    items = normaliseAncestors(items, [gid], geo);
    const ix = indexTree(items, geo), e = ix.get(gid);
    if (!e || e.it.kind !== 'group') return { items, ids: [] };
    const flat = pushDown(e.it, null, geo, I, true);
    const parentId = e.parent ? e.parent.id : null;
    const list = listOf(items, parentId, ix).slice();
    list.splice(e.i, 1, ...flat.children);
    return { items: setList(items, parentId, list), ids: flat.children.map(c => c.id) };
  }
  /** Stacking order within each parent: 'forward' | 'backward' | 'front' | 'back'. */
  function arrange(items, ids, how, geo) {
    const ix = indexTree(items, geo);
    const sel = topmost(ids, ix), byParent = new Map();
    sel.forEach(id => { const p = ix.get(id).parent; const k = p ? p.id : null; if (!byParent.has(k)) byParent.set(k, []); byParent.get(k).push(id); });
    for (const [pid, mine] of byParent) {
      const list = listOf(items, pid, ix).slice(), on = new Set(mine);
      let out;
      if (how === 'front') out = list.filter(x => !on.has(x.id)).concat(list.filter(x => on.has(x.id)));
      else if (how === 'back') out = list.filter(x => on.has(x.id)).concat(list.filter(x => !on.has(x.id)));
      else {
        out = list;
        if (how === 'forward') { for (let i = out.length - 2; i >= 0; i--) if (on.has(out[i].id) && !on.has(out[i + 1].id)) { const t = out[i]; out[i] = out[i + 1]; out[i + 1] = t; } }
        else { for (let i = 1; i < out.length; i++) if (on.has(out[i].id) && !on.has(out[i - 1].id)) { const t = out[i]; out[i] = out[i - 1]; out[i - 1] = t; } }
      }
      items = setList(items, pid, out);
    }
    return items;
  }

  /* ═══ 6 · hit tests ═══ */
  const paintFill = it => (it.kind === 'text' ? !(it.role === 'line' || it.role === 'cut') : it.role === 'hatch' ? true : it.role === 'art' ? (!!it.fill || !it.stroke) : false);
  const paintStroke = it => (it.kind === 'text' ? (it.role === 'line' || it.role === 'cut') : (it.role === 'cut' || it.role === 'line') ? true : it.role === 'art' ? !!it.stroke : false);
  // (Paul, 28 Oct 2026: "the colour should be a greyscale using the black as base") a traced image's part (artGrey, charm-nest-ce-image.js) is drawn in its own grey, not its role's colour
  const greyOf = it => (it && it.artGrey && Array.isArray(it.fill) && it.fill.length >= 3 ? `rgb(${it.fill.slice(0, 3).map(v => Math.round(clamp(+v || 0, 0, 1) * 255)).join(',')})` : '');
  function distSeg(px, py, ax, ay, bx, by) {
    const dx = bx - ax, dy = by - ay, L = dx * dx + dy * dy;
    let t = L ? ((px - ax) * dx + (py - ay) * dy) / L : 0; t = clamp(t, 0, 1);
    return Math.hypot(px - ax - t * dx, py - ay - t * dy);
  }
  /** Face-space polylines of a leaf (cached for the last matrix it was asked with). */
  const polyCache = new WeakMap();
  function polysOf(it, W, geo) {
    const key = mkey(W), hit = polyCache.get(it);
    if (hit && hit.key === key) return hit;
    const g = geo.raw(it);
    let out;
    if (it.kind === 'text') { const q = quadOf(g.bbox, W); out = q ? [{ pts: [].concat(...q), closed: true }] : []; }
    else out = flatten(g.subpaths).map(pl => { const p = new Array(pl.pts.length); for (let i = 0; i < p.length; i += 2) { const q = ap(W, pl.pts[i], pl.pts[i + 1]); p[i] = q[0]; p[i + 1] = q[1]; } return { pts: p, closed: pl.closed }; });
    let b = null; for (const pl of out) for (let i = 0; i < pl.pts.length; i += 2) b = union(b, [pl.pts[i], pl.pts[i + 1], pl.pts[i], pl.pts[i + 1]]);
    const rec = { key, polys: out, box: b }; polyCache.set(it, rec); return rec;
  }
  function hitLeaf(it, W, x, y, tol, geo) {
    const P = polysOf(it, W, geo);
    const lw = paintStroke(it) ? lwOf(it) / 2 : 0, pad = tol + lw;
    if (!P.box || x < P.box[0] - pad || x > P.box[2] + pad || y < P.box[1] - pad || y > P.box[3] + pad) return false;
    const fill = it.kind === 'text' || paintFill(it);
    let wind = 0, best = Infinity;
    for (const pl of P.polys) {
      const p = pl.pts, n = p.length / 2;
      const segs = pl.closed || fill ? n : n - 1;
      for (let i = 0; i < segs; i++) {
        const j = (i + 1) % n, ax = p[2 * i], ay = p[2 * i + 1], bx = p[2 * j], by = p[2 * j + 1];
        if (i < n - 1 || pl.closed) best = Math.min(best, distSeg(x, y, ax, ay, bx, by));
        if (fill) { if (ay <= y) { if (by > y && (bx - ax) * (y - ay) - (x - ax) * (by - ay) > 0) wind++; } else if (by <= y && (bx - ax) * (y - ay) - (x - ax) * (by - ay) < 0) wind--; }
      }
    }
    return (fill && wind !== 0) || best <= pad;
  }
  /** The topmost item under x, y (pt): the top-level item, or with deep the leaf itself (Alt reaches inside a group). Hidden and locked items are not hit. */
  function hitTest(items, x, y, tol, geo, opts) {
    opts = opts || {};
    const rec = (list, P, top) => {
      for (let i = list.length - 1; i >= 0; i--) {
        const it = list[i];
        if (!it || it.hidden || (it.locked && !opts.locked)) continue;
        const W = mul(P, geo.matrix(it));
        if (it.kind === 'group') { const r = rec(it.children || [], W, top || it); if (r) return r; continue; }
        if (opts.filter && !opts.filter(it)) continue;
        if (hitLeaf(it, W, x, y, tol, geo)) return { top: top || it, leaf: it };
      }
      return null;
    };
    const r = rec(items, I, null);
    return r ? (opts.deep ? r.leaf : r.top) : null;
  }
  function segHitsRect(ax, ay, bx, by, r) {
    const inR = (x, y) => x >= r[0] && x <= r[2] && y >= r[1] && y <= r[3];
    if (inR(ax, ay) || inR(bx, by)) return true;
    if (Math.max(ax, bx) < r[0] || Math.min(ax, bx) > r[2] || Math.max(ay, by) < r[1] || Math.min(ay, by) > r[3]) return false;
    const cross = (x1, y1, x2, y2, x3, y3, x4, y4) => {
      const d = (x2 - x1) * (y4 - y3) - (y2 - y1) * (x4 - x3); if (!d) return false;
      const t = ((x3 - x1) * (y4 - y3) - (y3 - y1) * (x4 - x3)) / d, u = ((x3 - x1) * (y2 - y1) - (y3 - y1) * (x2 - x1)) / d;
      return t >= 0 && t <= 1 && u >= 0 && u <= 1;
    };
    return cross(ax, ay, bx, by, r[0], r[1], r[2], r[1]) || cross(ax, ay, bx, by, r[2], r[1], r[2], r[3]) || cross(ax, ay, bx, by, r[0], r[3], r[2], r[3]) || cross(ax, ay, bx, by, r[0], r[1], r[0], r[3]);
  }
  function leafTouches(it, W, r, geo) {
    const P = polysOf(it, W, geo);
    if (!P.box || P.box[2] < r[0] || P.box[0] > r[2] || P.box[3] < r[1] || P.box[1] > r[3]) return false;
    for (const pl of P.polys) {
      const p = pl.pts, n = p.length / 2;
      for (let i = 0; i < (pl.closed ? n : n - 1); i++) { const j = (i + 1) % n; if (segHitsRect(p[2 * i], p[2 * i + 1], p[2 * j], p[2 * j + 1], r)) return true; }
    }
    // the box wholly inside a filled shape (or a text) touches it too
    return (it.kind === 'text' || paintFill(it)) && hitLeaf(it, W, (r[0] + r[2]) / 2, (r[1] + r[3]) / 2, 0, geo);
  }
  /** Everything a box [x0,y0,x1,y1] (pt) touches: top-level items (or leaves with deep), bottom first. Hidden and locked items are left out. */
  function touching(items, rect, geo, opts) {
    opts = opts || {};
    const r = [Math.min(rect[0], rect[2]), Math.min(rect[1], rect[3]), Math.max(rect[0], rect[2]), Math.max(rect[1], rect[3])];
    const out = [];
    const rec = (list, P) => {
      let any = false;
      for (const it of list || []) {
        if (it.hidden || it.locked) continue;
        const W = mul(P, geo.matrix(it));
        const t = it.kind === 'group' ? rec(it.children, W) : (leafTouches(it, W, r, geo) && (opts.deep && out.push(it), true));
        if (t) any = true;
      }
      return any;
    };
    for (const it of items) {
      if (it.hidden || it.locked) continue;
      const W = geo.matrix(it);
      if (it.kind === 'group') { if (opts.deep) rec(it.children, W); else if (rec(it.children, W)) out.push(it); }
      else if (leafTouches(it, W, r, geo)) out.push(it);
    }
    return out;
  }

  /* ═══ 7 · history (the studio's snapshot stack, J 11732-11823; snapshots are references, the tree being immutable) ═══ */
  class History {
    constructor(limit) { this.limit = limit || 120; this.back = []; this.fwd = []; }
    push(snap) { this.back.push(snap); if (this.back.length > this.limit) this.back.shift(); this.fwd.length = 0; }
    peekBack() { return this.back[this.back.length - 1] || null; }
    peekFwd() { return this.fwd[this.fwd.length - 1] || null; }
    undo(current) { if (!this.back.length) return null; this.fwd.push(current); return this.back.pop(); }
    redo(current) { if (!this.fwd.length) return null; this.back.push(current); return this.fwd.pop(); }
    get canUndo() { return this.back.length > 0; }
    get canRedo() { return this.fwd.length > 0; }
  }

  /* ═══ 8 · in and out: the items handed in, the items handed back, and what changed ═══ */
  /** The caller's items as the editor keeps them: copies (the caller's objects are never touched; geometry arrays are shared, never mutated), ids filled in, the roles
   *  in lockRoles locked with a mark that says the lock is the editor's and not a change. */
  function prepIn(items, lockRoles) {
    const lock = new Set(lockRoles || []);
    const rec = it => {
      if (!it || typeof it !== 'object') return null;
      const o = Object.assign({}, it);
      if (o.id == null || o.id === '') o.id = newId(); else o.id = String(o.id);
      if (o.kind !== 'text' && o.kind !== 'group') o.kind = 'path';
      if (!ROLES.includes(o.role)) o.role = o.kind === 'text' ? 'hatch' : 'art';
      if (lock.has(o.role) && !o.locked) { o._autoLock = true; o._lockedIn = !!it.locked; o.locked = true; }
      if (o.kind === 'group') o.children = (it.children || []).map(rec).filter(Boolean);
      return o;
    };
    return (items || []).map(rec).filter(Boolean);
  }
  /** Items as they leave: internal marks gone, the editor's own locks taken back off, transforms written out, text outlined in face space, matrices stated. */
  function exportItems(items, geo, P) {
    P = P || I;
    return (items || []).map(it => {
      const o = {};
      for (const k of Object.keys(it)) if (k[0] !== '_') o[k] = it[k];
      if (it._autoLock) o.locked = !!it._lockedIn;
      o.tx = r9(num(it.tx, 0)); o.ty = r9(num(it.ty, 0)); o.sx = r9(num(it.sx, 1)); o.sy = r9(num(it.sy, 1)); o.rot = r9(num(it.rot, 0));
      o.hidden = !!it.hidden; o.locked = !!o.locked;
      const own = geo.matrix(it), W = mul(P, own);
      if (it.kind === 'group') { o.children = exportItems(it.children, geo, W); o.matrix = own.map(r9); }
      else if (it.kind === 'text') { o.subpaths = transformSubpaths(geo.raw(it).subpaths, W); }
      else { o.matrix = own.map(r9); o.pivot = centreOf(geo.box(it)).map(r9); }
      const b = it.kind === 'text' ? bboxOf(o.subpaths) : geo.drawn(it, P);
      o.bbox = b ? b.map(r9) : null;
      return o;
    });
  }
  const SKIP = new Set(['children', 'subpaths', 'matrix', 'pivot', 'bbox']);
  const DEF = { tx: 0, ty: 0, rot: 0, sx: 1, sy: 1, hidden: false, locked: false };
  function sameVal(a, b) {
    if (a === b) return true;
    if (typeof a === 'number' && typeof b === 'number') return Math.abs(a - b) <= 1e-9 * Math.max(1, Math.abs(a), Math.abs(b));
    if (a == null || b == null || typeof a !== 'object' || typeof b !== 'object') return false;
    if (Array.isArray(a) !== Array.isArray(b)) return false;
    const ka = Object.keys(a), kb = Object.keys(b);
    if (ka.length !== kb.length) return false;
    return ka.every(k => sameVal(a[k], b[k]));
  }
  function sameOwn(a, b) {
    if (a === b) return true;
    const keys = new Set(Object.keys(a).concat(Object.keys(b)));
    for (const k of keys) {
      if (SKIP.has(k) || k[0] === '_') continue;
      const va = a[k] === undefined ? DEF[k] : a[k], vb = b[k] === undefined ? DEF[k] : b[k];
      if (!sameVal(va, vb)) return false;
    }
    if (a.kind !== 'text' && a.kind !== 'group' && a.subpaths !== b.subpaths && !sameVal(a.subpaths || [], b.subpaths || [])) return false;
    return true;
  }
  /** What changed from `before` to `after` (both as exportItems gives them). */
  function diffFace(before, after) {
    const A = new Map(), B = new Map();
    walk(before, (it, p) => A.set(it.id, { it, p: p ? p.id : null }));
    walk(after, (it, p) => B.set(it.id, { it, p: p ? p.id : null }));
    const removed = [...A.keys()].filter(id => !B.has(id));
    const added = [], changed = {};
    const newIds = new Set([...B.keys()].filter(id => !A.has(id)));
    for (const [id, b] of B) {
      if (newIds.has(id)) { if (!(b.p && newIds.has(b.p))) added.push(b.it); continue; }
      const a = A.get(id);
      if (a.p !== b.p || !sameOwn(a.it, b.it)) changed[id] = b.it;
    }
    return { removed, changed, added };
  }
  const changeCount = d => d.removed.length + d.added.length + Object.keys(d.changed).length;

  /** Any item's outlines in face space (a group's leaves, a text's glyphs): for a caller that wants geometry and nothing else. */
  function outlinesOf(it, geo, P) {
    const W = mul(P || I, geo.matrix(it));
    if (it.kind === 'group') return [].concat(...(it.children || []).map(ch => outlinesOf(ch, geo, W)));
    return transformSubpaths(geo.raw(it).subpaths, W);
  }

  const model = { MM, PT, I, mul, ap, inv, tr, rotM, scM, about, isI, bboxOf, union, transformSubpaths, flatten, dOf, rectSP, ellipseSP, lineSP,
    createGeo, decompose, composeInto, pushDown, retext, foldTextScale, walk, indexTree, replaceIn, removeIds, pruneEmpty, topmost, normaliseAncestors, copyItems, pasteItems, reid,
    groupItems, ungroupItem, arrange, hitTest, touching, History, prepIn, exportItems, diffFace, changeCount, outlinesOf, newId, paintFill, paintStroke,
    weighSubpaths, weighItem, weighable, weightOf, lwOf, inkArea, canWeigh, LW_LASER_PT, WT_MIN_MM, WT_MAX_MM };

  /* ════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
     9 · THE WORKSPACE
     ════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════ */

  /* The studio's icons, verbatim where the studio has one (S line numbers in the header); the hand, the link and the zoom have none there and are drawn in the same
     hand: 24-unit box, 1.6 stroke, round ends. */
  const SV = (d, extra) => `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"${extra || ''}>${d}</svg>`;
  const IC = {
    select: SV('<path d="m4 3 7 17 2.2-6.8L20 11Z"/>'),
    marquee: SV('<rect x="3.5" y="4.5" width="17" height="15" rx="1" stroke-dasharray="3.2 2.6"/>'),
    text: SV('<path d="M5 7h14"/><path d="M12 7v11"/><path d="M9 18h6"/>'),
    rect: SV('<rect x="3.5" y="5.5" width="17" height="13" rx="1"/>'),
    ellipse: SV('<circle cx="12" cy="12" r="8.5"/>'),
    line: SV('<path d="M4 20 20 4"/>'),
    hand: SV('<path d="M8 12.5V5.6a1.4 1.4 0 0 1 2.8 0V11"/><path d="M10.8 10.6V4.4a1.4 1.4 0 0 1 2.8 0v6.2"/><path d="M13.6 10.8V5.6a1.4 1.4 0 0 1 2.8 0v6"/><path d="M16.4 11.4V8.6a1.4 1.4 0 0 1 2.8 0v5.2a7 7 0 0 1-7 7h-.9a6.4 6.4 0 0 1-5.2-2.7l-2.6-3.7a1.5 1.5 0 0 1 2.3-1.9L8 14.8"/>'),
    more: '<svg viewBox="0 0 24 24" fill="currentColor" aria-hidden="true"><circle cx="5" cy="12" r="1.8"/><circle cx="12" cy="12" r="1.8"/><circle cx="19" cy="12" r="1.8"/></svg>',
    undo: SV('<path d="M9 7H4v-5"/><path d="M4.5 7.2A9 9 0 1 1 3 15"/>', ' stroke-width="1.7"'),
    redo: SV('<path d="M15 7h5v-5"/><path d="M19.5 7.2A9 9 0 1 0 21 15"/>', ' stroke-width="1.7"'),
    up: SV('<path d="M12 19V5"/><path d="m6 11 6-6 6 6"/>', ' stroke-width="1.7"'),
    down: SV('<path d="M12 5v14"/><path d="m6 13 6 6 6-6"/>', ' stroke-width="1.7"'),
    group: SV('<path d="M3 8V3h5"/><path d="M16 3h5v5"/><path d="M21 16v5h-5"/><path d="M8 21H3v-5"/><rect x="6.5" y="6.5" width="6" height="6"/><rect x="11.5" y="11.5" width="6" height="6"/>'),
    ungroup: SV('<path d="M3 7V3h4"/><path d="M17 21h4v-4"/><rect x="3" y="9.5" width="7" height="7"/><rect x="14" y="4.5" width="7" height="7"/>'),
    del: SV('<path d="M4 6.5h16"/><path d="M9 6.5V4h6v2.5"/><path d="M6.5 6.5 7.5 20h9l1-13.5"/>'),
    layers: SV('<path d="m12 3 8 4-8 4-8-4Z"/><path d="m4 12 8 4 8-4"/><path d="m4 17 8 4 8-4"/>'),
    // (weight: a shape and the same shape grown round it, Illustrator's "offset path"; stroke: lines of three widths, its "stroke weight")
    weight: SV('<rect x="8" y="8" width="8" height="8" rx=".6"/><rect x="4" y="4" width="16" height="16" rx="3.2" stroke-dasharray="2.2 2.2"/>'),
    stroke: SV('<path d="M4 6.5h16" stroke-width="1"/><path d="M4 12h16" stroke-width="2"/><path d="M4 17.8h16" stroke-width="3.2"/>', ' stroke-linecap="butt"'),
    link: SV('<path d="M9.5 14.5 14.5 9.5"/><path d="M11 6.6 12.6 5a3.6 3.6 0 0 1 5.1 5.1L16 11.7"/><path d="M13 17.4 11.4 19a3.6 3.6 0 0 1-5.1-5.1L8 12.3"/>'),
    eyeOn: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" aria-hidden="true"><path d="M3 12s3.5-6 9-6 9 6 9 6-3.5 6-9 6-9-6-9-6Z"/><circle cx="12" cy="12" r="2.4"/></svg>',
    eyeOff: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" aria-hidden="true"><path d="M3 3l18 18"/><path d="M10.6 6.3A9.6 9.6 0 0 1 12 6c5 0 9 4.5 9 6a11 11 0 0 1-2.6 3.4"/><path d="M6.2 8.6A11 11 0 0 0 3 12c0 1.5 4 6 9 6a9.5 9.5 0 0 0 3.5-.7"/></svg>',
    lockOn: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" aria-hidden="true"><rect x="4.5" y="10.5" width="15" height="10"/><path d="M8 10.5V7a4 4 0 0 1 8 0v3.5"/></svg>',
    lockOff: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" aria-hidden="true"><rect x="4.5" y="10.5" width="15" height="10"/><path d="M8 10.5V7a4 4 0 0 1 7.5-2"/></svg>',
    folder: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linejoin="round" aria-hidden="true"><path d="M3 7.5A1.5 1.5 0 0 1 4.5 6h4l2 2.5h7A1.5 1.5 0 0 1 19 10v7a1.5 1.5 0 0 1-1.5 1.5h-13A1.5 1.5 0 0 1 3 17Z"/></svg>',
    // (28 Oct 2026, the extension points: a file coming in, the Object menu, a panel at the side, a tool that brings no icon of its own)
    import: SV('<path d="M12 3.5v11"/><path d="m7.5 10 4.5 4.5 4.5-4.5"/><path d="M4 15.5v4h16v-4"/>'),
    object: SV('<rect x="4" y="4" width="7" height="7" rx=".6"/><rect x="13" y="13" width="7" height="7" rx=".6"/><path d="M15 4.5h4.5V9"/><path d="M9 19.5H4.5V15"/>'),
    panel: SV('<rect x="3.5" y="4.5" width="17" height="15" rx="1"/><path d="M14.5 4.5v15"/><path d="M16.5 8.5h2"/><path d="M16.5 11.5h2"/>'),
    tool: SV('<circle cx="12" cy="12" r="7.5"/><path d="M12 8.5v7"/><path d="M8.5 12h7"/>')
  };
  /* name · what it does in four words or fewer · the key that arms it (the studio's MK_TIPS, J 18882) */
  const TIPS = {
    select: ['Select', 'Move, resize, rotate', 'V'], marquee: ['Marquee', 'Select what a box touches', 'M'], text: ['Text', 'Click to place words', 'T'],
    shapes: ['Shapes', 'Rectangle, ellipse, line', 'R'], hand: ['Hand', 'Move around the charm', 'H'], more: ['More', 'Grid, guides, zoom', ''],
    undo: ['Undo', 'Take the last thing back', '⌘Z'], redo: ['Redo', 'Put it back', '⇧⌘Z']
  };
  /* the instruction for the tool in hand, in the foot (the studio's MK_HINTS, J 9260) */
  const HINTS = {
    select: 'Click to select · Shift-click adds · drag on empty space to sweep · Alt-click reaches inside a group · double-click words to edit',
    marquee: 'Drag a box · everything it touches is selected · Shift adds',
    text: 'Click where the words start, then type · click a text to edit it',
    rect: 'Drag a box · Shift for a square · Alt from the centre', ellipse: 'Drag an oval · Shift for a circle · Alt from the centre',
    line: 'Drag a line · Shift keeps it to 45°', hand: 'Drag to move around · or hold Space with any tool'
  };

  const CSS = `
.ce-root{
  --ink:#1c1d1d; --paper:#fff; --band:#faf9f7; --hair:#e6e4e0; --muted:#6d6a66; --gold:#a58a52; --gold-lt:#cdb98a;
  --hov:#f2eee4; --hov-line:#c6bfaf; --press:#e6dfcd; --on-hov:#33342f;
  --in:.11s cubic-bezier(.22,.9,.28,1); --press-t:.06s cubic-bezier(.3,.8,.4,1);
  --sans:'Nunito Sans','Helvetica Neue',Helvetica,Arial,sans-serif; --serif:'Cormorant','Hoefler Text',Georgia,serif;
  --rule:19px; --c-cut:#1c1d1d; --c-line:#d4291f; --c-hatch:#1a5fc4; --c-art:#57534e;
  position:fixed; inset:0; z-index:2147482500; background:rgba(28,29,29,.36);
  font-family:var(--sans); font-size:13px; line-height:1.4; color:var(--ink); -webkit-font-smoothing:antialiased; text-align:left;
}
.ce-root *,.ce-root *::before,.ce-root *::after{ box-sizing:border-box; }
.ce-root button{ font-family:inherit; border-radius:0; margin:0; box-shadow:none; text-transform:none; letter-spacing:normal; line-height:1.2; min-width:0; min-height:0; }
.ce-root label{ font:inherit; letter-spacing:normal; text-transform:none; color:inherit; margin:0; }
.ce-root input,.ce-root textarea{ border-radius:0; box-shadow:none; margin:0; }
.ce-root [hidden]{ display:none !important; }
.ce-win{
  position:absolute; inset:14px 16px; background:#fff; overflow:hidden; outline:none;
  display:grid; grid-template-columns:auto minmax(0,1fr) auto; grid-template-rows:auto auto minmax(0,1fr) auto;
  column-gap:12px; padding:10px 16px 10px; box-shadow:0 30px 80px rgba(28,29,29,.30);
}
.ce-win.is-nodock{ grid-template-columns:auto minmax(0,1fr); }
/* ── head ── */
.ce-head{ grid-area:1/1/2/-1; display:flex; align-items:center; gap:10px; min-width:0; padding:0 0 9px; margin-bottom:8px; border-bottom:1px solid var(--hair); }
.ce-id{ flex:1 1 auto; min-width:0; display:flex; align-items:baseline; gap:14px; white-space:nowrap; overflow:hidden; }
.ce-id h2{ font-family:var(--serif); font-weight:500; font-size:22px; line-height:1.1; margin:0; letter-spacing:.01em; color:var(--ink); flex:none; max-width:60%; overflow:hidden; text-overflow:ellipsis; }
.ce-eyebrow{ font-size:10px; font-weight:700; letter-spacing:.2em; text-transform:uppercase; color:#8a8784; overflow:hidden; text-overflow:ellipsis; min-width:0; }
.ce-icon{ flex:none; display:inline-flex; align-items:center; justify-content:center; gap:5px; height:32px; min-width:32px; padding:0 8px; border:1px solid var(--hair); background:#fff; color:var(--ink); cursor:pointer; font-size:9.5px; font-weight:700; letter-spacing:.12em; text-transform:uppercase; }
.ce-icon svg{ width:16px; height:16px; }
.ce-icon b{ font-size:9px; font-weight:700; background:var(--ink); color:#fff; padding:1px 5px; letter-spacing:.04em; }
.ce-icon:not(.is-on):hover{ background:var(--hov); border-color:var(--hov-line); }
.ce-icon.is-on{ border-color:var(--ink); background:var(--band); }
.ce-close{ font-size:20px; font-weight:400; border-color:transparent; color:var(--muted); }
.ce-head .ce-seg{ flex:none; }
.ce-short{ display:none; }
/* (27 Oct 2026: a bar that would wrap on a desk, words with a long font name and their weight, says the role in one word, Hatch, Line, and the menu says the rest) */
.ce-props.is-tight .ce-long{ display:none; } .ce-props.is-tight .ce-short{ display:inline; }
/* ── the rail (C 5049-5111) ── */
.ce-rail{ grid-area:2/1/5/2; align-self:stretch; display:flex; flex-direction:column; width:52px; padding:5px 4px; border:1px solid var(--hair); background:#fff; overflow:visible; min-height:0; }
.ce-tools{ display:flex; flex-direction:column; gap:1px; }
.ce-tool{ position:relative; width:42px; height:42px; padding:0; display:inline-flex; align-items:center; justify-content:center; border:1px solid transparent; background:none; cursor:pointer; color:var(--muted);
  transform-origin:center; transition:background var(--in), color var(--in), border-color var(--in); }
.ce-tool svg{ width:18px; height:18px; transition:transform .13s cubic-bezier(.22,.9,.25,1); will-change:transform; }
.ce-tool.is-on{ background:var(--ink); border-color:var(--ink); color:#fff; }
.ce-tool:not(.is-on):hover{ background:var(--hov); color:var(--ink); }
.ce-tool.is-on:hover{ background:var(--on-hov); border-color:var(--on-hov); }
.ce-tool:not(.is-on):active{ background:var(--press); transform:translateY(1px); }
.ce-rail .ce-drop__b::after{ content:""; position:absolute; right:3px; bottom:3px; width:0; height:0; border-left:4.5px solid transparent; border-bottom:4.5px solid currentColor; opacity:.5; pointer-events:none; }
.ce-rail .ce-drop__b.is-on::after,.ce-rail .ce-drop__b:hover::after{ opacity:.85; }
.ce-hist{ display:flex; flex-direction:column; gap:3px; margin:auto 0 0; padding:7px 0 1px; border-top:1px solid var(--hair); }
.ce-hist__b{ width:42px; height:32px; border:1px solid var(--hair); background:#fff; cursor:pointer; color:var(--ink); display:inline-flex; align-items:center; justify-content:center; }
.ce-hist__b svg{ width:15px; height:15px; }
.ce-hist__b:not(:disabled):hover{ background:var(--hov); border-color:var(--hov-line); }
.ce-hist__b:disabled{ opacity:.35; cursor:default; }
/* ── the property bar (C 3572-3734) ── */
.ce-props{ grid-area:2/2/3/3; display:flex; align-items:center; flex-wrap:wrap; gap:6px; row-gap:6px; min-width:0; padding:0 0 7px; margin-bottom:7px; border-bottom:1px solid var(--hair); }
.ce-prop{ display:inline-flex; align-items:center; gap:6px; white-space:nowrap; height:30px; border:1px solid var(--hair); background:#fff; cursor:pointer; padding:0 9px;
  font-size:9.5px; font-weight:700; letter-spacing:.12em; text-transform:uppercase; color:var(--ink); flex:0 0 auto; transition:border-color var(--in), background var(--in); }
.ce-prop:not(:disabled):hover{ background:var(--hov); border-color:var(--hov-line); }
.ce-prop:disabled{ opacity:.32; cursor:default; }
.ce-caret{ width:0; height:0; margin-left:2px; flex:none; border-left:3.5px solid transparent; border-right:3.5px solid transparent; border-top:4px solid currentColor; opacity:.75; }
.ce-fontA{ font-size:12px; font-weight:700; letter-spacing:0; text-transform:none; }
.ce-fontname{ max-width:96px; overflow:hidden; text-overflow:ellipsis; letter-spacing:.08em; }
/* position and size, then the angle behind a shaded gutter: one box on a desktop (C 3707-3716), two boxes that wrap on their own on a phone */
.ce-sizes{ display:inline-flex; align-items:stretch; flex:0 0 auto; border:1px solid var(--hair); background:#fff; height:30px; }
.ce-size{ display:inline-flex; align-items:center; flex:0 0 auto; background:#fff; height:100%; }
.ce-size__f{ display:inline-flex; align-items:center; gap:6px; padding:0 8px; height:100%; cursor:text; border-left:1px solid var(--hair); }
.ce-size__f:first-child,.ce-size__sep + .ce-size__f,.ce-size__link + .ce-size__f{ border-left:0; }
.ce-size__f > span{ font-size:12px; font-weight:800; letter-spacing:.06em; color:var(--ink); opacity:.62; }
.ce-size input{ width:40px; border:0; padding:0; background:none; outline:none; height:auto; font-family:inherit; font-size:11px; font-weight:600; color:var(--ink); text-align:right; -moz-appearance:textfield; }
.ce-size input:disabled{ color:var(--muted); opacity:.55; }
.ce-size__f:focus-within{ background:var(--band); }
.ce-size__f:focus-within > span{ opacity:1; }
.ce-size__sep{ align-self:stretch; flex:0 0 auto; width:10px; border-left:1px solid var(--hair); border-right:1px solid var(--hair); background:var(--band); }
.ce-size__link{ align-self:stretch; width:22px; border:0; border-left:1px solid var(--hair); border-right:1px solid var(--hair); background:none; cursor:pointer; color:var(--hov-line); display:inline-flex; align-items:center; justify-content:center; padding:0; }
.ce-size__link svg{ width:13px; height:13px; }
.ce-size__link.is-on{ color:var(--ink); }
.ce-size__link:hover{ background:var(--hov); }
.ce-size__u{ padding:0 8px 0 4px; font-style:normal; font-size:10px; font-weight:800; letter-spacing:.1em; text-transform:uppercase; color:var(--muted); line-height:28px; }
.ce-size__f--rot{ padding-left:8px; }
.ce-size__f--rot > span{ font-size:16px; line-height:1; font-weight:600; letter-spacing:0; position:relative; top:-1px; }
.ce-size__f--rot input{ width:38px; }
.ce-size__u--deg{ font-size:13px; letter-spacing:0; padding:0 8px 0 1px; }
.ce-num{ display:inline-flex; align-items:center; gap:4px; height:30px; padding:0 8px; border:1px solid var(--hair); background:#fff; flex:0 0 auto; }
.ce-num input{ width:34px; border:0; padding:0; outline:none; background:none; font-family:inherit; font-size:11px; font-weight:600; color:var(--ink); text-align:right; height:auto; }
.ce-num i{ font-style:normal; font-size:10px; font-weight:800; letter-spacing:.1em; color:var(--muted); }
.ce-num:focus-within{ background:var(--band); }
.ce-num__b{ flex:none; width:20px; height:24px; margin:0 0 0 -4px; padding:0; border:0; background:none; cursor:pointer; color:var(--muted); display:inline-flex; align-items:center; justify-content:center; }
.ce-num__b svg{ width:15px; height:15px; }
.ce-num__b:hover{ background:var(--hov); color:var(--ink); }
.ce-num__b.is-on{ color:var(--gold); }
.ce-num--thick{ padding:0 7px; gap:3px; } .ce-num--thick input{ width:34px; }
.ce-seg{ display:inline-flex; flex:0 0 auto; border:1px solid var(--hair); height:30px; }
.ce-seg button{ border:0; background:#fff; padding:0 11px; cursor:pointer; font-size:9.5px; font-weight:700; letter-spacing:.14em; text-transform:uppercase; color:var(--muted); }
.ce-seg button + button{ border-left:1px solid var(--hair); }
.ce-seg button.is-on{ background:var(--ink); color:#fff; }
.ce-seg button:not(.is-on):not(:disabled):hover{ background:var(--hov); color:var(--ink); }
.ce-seg button:disabled{ opacity:.35; cursor:default; }
.ce-zoomb{ min-width:74px; justify-content:space-between; font-variant-numeric:tabular-nums; letter-spacing:.06em; }
.ce-sw{ width:12px; height:12px; flex:none; display:inline-block; position:relative; }
.ce-sw--cut{ border:1.5px solid var(--c-cut); background:#fff; }
.ce-sw--line{ border:1.5px solid var(--c-line); background:#fff; }
.ce-sw--hatch{ background:var(--c-hatch); }
.ce-sw--art{ background:var(--c-art); }
.ce-sw--art.is-outline{ background:#fff; border:1.5px solid var(--c-art); }
.ce-sw--mixed{ background:linear-gradient(135deg,var(--c-line) 0 50%,var(--c-hatch) 50% 100%); }
.ce-sw.is-text{ background:none; border:0; font-size:12px; font-weight:800; line-height:12px; text-align:center; }
.ce-sw.is-text.ce-sw--cut{ color:var(--c-cut); } .ce-sw.is-text.ce-sw--line{ color:var(--c-line); } .ce-sw.is-text.ce-sw--hatch{ color:var(--c-hatch); } .ce-sw.is-text.ce-sw--art{ color:var(--c-art); }
/* ── menus (C 3736-3826, 5149-5170) ── */
.ce-drop{ position:relative; flex:0 0 auto; }
.ce-menu{ position:fixed; z-index:20; min-width:200px; max-height:min(70vh,520px); max-width:min(320px,calc(100vw - 32px)); overflow-y:auto; background:#fff; border:1px solid var(--ink); padding:5px; box-shadow:0 18px 44px rgba(28,29,29,.22); }
.ce-rail .ce-menu{ min-width:190px; padding:4px; }
.ce-menu__head{ font-size:9px; font-weight:700; letter-spacing:.18em; text-transform:uppercase; color:var(--muted); padding:8px 9px 5px; }
.ce-menu__note{ font-size:11px; line-height:1.55; color:var(--muted); padding:7px 9px 4px; border-top:1px solid var(--hair); margin-top:4px; }
.ce-menu__note b{ color:var(--ink); font-weight:700; }
.ce-item{ position:relative; display:flex; align-items:center; gap:9px; width:100%; text-align:left; border:0; background:none; cursor:pointer; padding:7px 9px; font-size:12.5px; color:var(--ink); }
.ce-item svg{ width:15px; height:15px; flex:none; opacity:.75; }
.ce-item em{ font-style:normal; margin-left:auto; padding-left:14px; font-size:10px; letter-spacing:.06em; color:var(--muted); white-space:nowrap; }
.ce-item:hover,.ce-item:focus-visible{ background:#f1ede3; outline:none; }
.ce-item:hover::before,.ce-item:focus-visible::before{ content:""; position:absolute; left:0; top:0; bottom:0; width:2px; background:var(--gold); }
.ce-item:active{ background:#e7e1d3; }
.ce-item.is-on{ box-shadow:inset 3px 0 0 0 var(--gold); font-weight:700; }
.ce-item.is-off{ opacity:.34; cursor:default; }
.ce-item.is-off:hover{ background:none; } .ce-item.is-off:hover::before{ display:none; }
.ce-item--danger:not(.is-off):hover{ color:#c0392b; }
.ce-tick{ width:14px; height:14px; flex:none; border:1px solid var(--hair); position:relative; }
.ce-item--tog.is-on{ box-shadow:none; font-weight:400; }
.ce-item--tog.is-on .ce-tick::after{ content:"✓"; position:absolute; inset:0; display:flex; align-items:center; justify-content:center; font-size:11px; line-height:1; }
.ce-menu__sep{ height:1px; background:var(--hair); margin:4px 2px; }
@keyframes cePopDown{ from{ opacity:0; transform:translateY(-5px) scale(.985); } to{ opacity:1; transform:none; } }
@keyframes cePopRight{ from{ opacity:0; transform:translateX(-7px) scale(.985); } to{ opacity:1; transform:none; } }
.ce-menu.is-in{ animation:cePopDown .17s cubic-bezier(.16,1,.3,1); }
.ce-menu.is-in--right{ animation:cePopRight .18s cubic-bezier(.16,1,.3,1); }
/* ── the work area: rulers and the room (C 4939-4995, 5207-5228) ── */
.ce-wrap{ grid-area:3/2/4/3; min-height:0; min-width:0; display:grid; grid-template-columns:var(--rule) minmax(0,1fr); grid-template-rows:var(--rule) minmax(0,1fr); border:1px solid var(--hair); }
.ce-rule{ display:block; background:var(--band); min-width:0; min-height:0; width:100%; height:100%; }
.ce-rule__corner{ grid-area:1/1; border-right:1px solid var(--hair); border-bottom:1px solid var(--hair); font-size:7.5px; font-weight:700; letter-spacing:.06em; text-transform:uppercase; color:var(--muted); display:flex; align-items:center; justify-content:center; }
.ce-rule--x{ grid-area:1/2; border-bottom:1px solid var(--hair); }
.ce-rule--y{ grid-area:2/1; border-right:1px solid var(--hair); }
.ce-stage{ grid-area:2/2; position:relative; overflow:hidden; background:var(--band); touch-action:none; user-select:none; -webkit-user-select:none; cursor:default; }
.ce-stage[data-tool="text"]{ cursor:text; }
.ce-stage[data-tool="marquee"],.ce-stage[data-tool="rect"],.ce-stage[data-tool="ellipse"],.ce-stage[data-tool="line"]{ cursor:crosshair; }
.ce-stage[data-tool="hand"],.ce-stage.is-space{ cursor:grab; }
.ce-stage.is-panning{ cursor:grabbing; }
.ce-paper{ position:absolute; background:#fff; pointer-events:none; box-shadow:0 0 0 1px var(--hair), 0 6px 26px rgba(28,29,29,.10); }
.ce-art,.ce-ui{ position:absolute; left:0; top:0; width:100%; height:100%; overflow:hidden; display:block; }
.ce-ui{ pointer-events:none; }
.ce-art .ce-p{ vector-effect:non-scaling-stroke; fill:none; stroke:none; stroke-width:max(calc(var(--lw, .25) * var(--k, 1) * 1px), var(--hair, 1px)); stroke-linejoin:round; stroke-linecap:round; }
.ce-art .ce-p.is-fill.ce-p--cut{ fill:var(--c-cut); } .ce-art .ce-p.is-stroke.ce-p--cut{ stroke:var(--c-cut); }
.ce-art .ce-p.is-fill.ce-p--line{ fill:var(--c-line); } .ce-art .ce-p.is-stroke.ce-p--line{ stroke:var(--c-line); }
.ce-art .ce-p.is-fill.ce-p--hatch{ fill:var(--c-hatch); } .ce-art .ce-p.is-stroke.ce-p--hatch{ stroke:var(--c-hatch); }
.ce-art .ce-p.is-fill.ce-p--art{ fill:var(--c-art); } .ce-art .ce-p.is-stroke.ce-p--art{ stroke:var(--c-art); }
.ce-art .ce-p.is-evenodd{ fill-rule:evenodd; }
.ce-art .ce-grid path{ fill:none; vector-effect:non-scaling-stroke; stroke-width:1px; shape-rendering:crispEdges; }
.ce-art .ce-grid .g-fine{ stroke:rgba(28,29,29,.045); } .ce-art .ce-grid .g-mm{ stroke:rgba(28,29,29,.10); } .ce-art .ce-grid .g-five{ stroke:rgba(28,29,29,.17); }
/* selection furniture, in screen pixels (C 3948-4034, 6150-6171) */
.ce-ui .u-hover{ fill:none; stroke:var(--gold); stroke-width:1.5; opacity:.9; }
.ce-ui .u-sel{ fill:none; stroke:var(--gold); stroke-width:1; }
.ce-ui .u-box{ fill:none; stroke:var(--ink); stroke-width:1; stroke-dasharray:4 3; }
.ce-ui .u-box--under{ fill:none; stroke:rgba(255,255,255,.85); stroke-width:3; }
.ce-ui .u-box.is-many{ stroke:var(--gold); stroke-dasharray:none; }
.ce-ui .u-h{ fill:#fff; stroke:var(--ink); stroke-width:1; pointer-events:all; }
.ce-ui .u-h[data-h="nw"],.ce-ui .u-h[data-h="se"]{ cursor:nwse-resize; } .ce-ui .u-h[data-h="ne"],.ce-ui .u-h[data-h="sw"]{ cursor:nesw-resize; }
.ce-ui .u-h[data-h="n"],.ce-ui .u-h[data-h="s"]{ cursor:ns-resize; } .ce-ui .u-h[data-h="e"],.ce-ui .u-h[data-h="w"]{ cursor:ew-resize; }
.ce-ui .u-h[data-h="rot"]{ cursor:grab; }
.ce-ui .u-stem{ stroke:var(--ink); stroke-width:1; }
.ce-ui .u-marq{ fill:rgba(28,29,29,.06); stroke:var(--ink); stroke-width:1; stroke-dasharray:4 3; }
.ce-ui .u-draft{ fill:none; stroke:var(--c-line); stroke-width:1.5; stroke-dasharray:5 3; }
.ce-ui .u-gd{ stroke:var(--gold); stroke-width:1; } .ce-ui .u-gd--mid{ stroke:#e0483c; stroke-dasharray:4 3; } .ce-ui .u-gd--frame{ stroke:rgba(165,138,82,.6); }
.ce-ui .u-dim rect{ fill:var(--gold); } .ce-ui .u-dim text{ fill:#fff; font:600 9.5px var(--sans); letter-spacing:.06em; }
/* the one text editor (C 3940-3946) */
.ce-editor{ position:absolute; z-index:6; resize:none; overflow:hidden; background:rgba(255,255,255,.98); border:1px solid var(--ink); box-shadow:0 10px 30px rgba(28,29,29,.22);
  padding:6px 9px; line-height:1.35; color:var(--ink); font-family:var(--sans); font-size:14px; outline:none; min-width:160px; width:220px; max-width:calc(100% - 12px); }
/* ── foot (C 5172-5184, 6654-6674) ── */
.ce-foot{ grid-area:4/2/5/3; display:flex; align-items:center; gap:14px; padding:10px 0 0; margin-top:8px; border-top:1px solid var(--hair); min-width:0; }
.ce-foot .ce-drop{ flex:none; }
.ce-hint{ flex:1 1 0; min-width:0; font-size:10px; letter-spacing:.12em; text-transform:uppercase; color:var(--muted); white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
.ce-actions{ display:flex; gap:12px; flex:none; margin-left:auto; }
.ce-btn{ display:inline-flex; align-items:center; justify-content:center; gap:8px; border:1px solid var(--ink); background:var(--ink); color:#fff; cursor:pointer; font-weight:700; font-size:11.5px; text-transform:uppercase !important; letter-spacing:.26em !important; padding:11px 24px; transition:background var(--in), border-color var(--in), color var(--in), transform var(--press-t); }
.ce-btn:active{ transform:translateY(1px); }
.ce-btn--ghost{ background:transparent; border-color:transparent; color:var(--muted); padding:11px 12px; }
.ce-btn--ghost:hover,.ce-btn--ghost:focus-visible{ background:var(--hov); border-color:var(--hov-line); color:var(--ink); }
.ce-btn--gold{ background:var(--gold); border-color:var(--gold); }
.ce-btn--gold:hover{ background:#a2854b; border-color:#a2854b; }
.ce-btn--gold:active{ background:#8f7644; }
.ce-btn--ink:hover{ background:var(--on-hov); border-color:var(--on-hov); }
/* ── the dock: layers (C 4095-4331, 6677-6697) ── */
.ce-dock{ grid-area:2/3/5/4; width:clamp(232px,19vw,296px); min-height:0; display:flex; flex-direction:column; border:1px solid var(--hair); background:#fff; }
.ce-dock__tabs{ flex:none; display:flex; align-items:stretch; border-bottom:1px solid var(--hair); }
.ce-dock__tab{ flex:1; border:0; background:none; cursor:pointer; padding:11px 6px; font-size:10px; font-weight:700; letter-spacing:.16em; text-transform:uppercase; color:var(--muted); border-bottom:2px solid transparent; }
.ce-dock__tab.is-on{ color:var(--ink); border-bottom-color:var(--gold); }
.ce-dock__tab:not(.is-on):not(:disabled):hover{ background:var(--hov); color:var(--ink); border-bottom-color:var(--gold-lt); }
.ce-dock__tab:disabled{ opacity:.35; cursor:default; }
.ce-dock__tab b{ display:inline-block; margin-left:6px; padding:1px 5px; min-width:16px; background:var(--ink); color:#fff; font-size:9px; font-weight:700; letter-spacing:.04em; vertical-align:1px; }
.ce-dock__tab.is-on b{ background:var(--gold); }
.ce-dock__x{ flex:none; width:38px; border:0; border-left:1px solid var(--hair); background:none; cursor:pointer; font-size:15px; color:var(--muted); }
.ce-dock__x:hover{ background:var(--hov); color:var(--ink); }
.ce-pane__bar{ flex:none; display:flex; gap:3px; padding:7px; border-bottom:1px solid var(--hair); align-items:stretch; }
.ce-act{ flex:1 1 0; min-width:0; display:inline-flex; align-items:center; justify-content:center; border:1px solid var(--hair); background:#fff; cursor:pointer; min-height:30px; padding:0 2px; color:var(--ink); }
.ce-act svg{ width:17px; height:17px; flex:none; }
.ce-act:not(:disabled):hover{ background:var(--hov); border-color:var(--hov-line); }
.ce-act:disabled{ opacity:.32; cursor:default; }
.ce-act--danger:not(:disabled):hover{ background:#fdf1ef; border-color:#e0b4ad; color:#c0392b; }
.ce-act__sep{ flex:0 0 1px; align-self:stretch; background:var(--hair); margin:0 2px; }
.ce-layers{ flex:1 1 auto; min-height:0; overflow-y:auto; padding:5px; touch-action:pan-y; overflow-anchor:none; }
.ce-layer{ position:relative; display:flex; align-items:center; gap:7px; min-height:31px; padding:2px 2px 2px 2px; padding-left:calc(2px + var(--depth,0) * 16px); border:1px solid transparent; background:#fff; cursor:pointer; user-select:none; -webkit-user-select:none;
  transition:background var(--in), border-color var(--in); }
.ce-layer + .ce-layer{ margin-top:1px; }
.ce-layer:hover{ background:var(--hov); }
.ce-layer.is-sel{ background:#eddfba; border-color:var(--ink); box-shadow:inset 3px 0 0 0 var(--gold); }
.ce-layer.is-sel:hover{ background:#e6d5a8; }
.ce-layer.is-sel .ce-layer__name{ font-weight:700; }
.ce-layer.is-hidden{ opacity:.5; } .ce-layer.is-hidden.is-sel{ opacity:.85; }
.ce-layer.is-locked .ce-layer__name{ color:var(--muted); }
.ce-layer.is-drop-above{ box-shadow:inset 0 2px 0 0 var(--gold); } .ce-layer.is-drop-below{ box-shadow:inset 0 -2px 0 0 var(--gold); }
.ce-layer.is-dragging{ opacity:.4; }
.ce-layer__grip{ flex:none; width:12px; display:flex; flex-direction:column; gap:2.5px; align-items:center; cursor:grab; padding:5px 0; }
.ce-layer__grip i{ width:9px; height:1.5px; background:var(--hair); display:block; }
.ce-layer:hover .ce-layer__grip i{ background:var(--muted); }
.ce-layer.is-sel .ce-layer__grip i{ background:var(--gold); }
.ce-layer__tw{ flex:none; width:16px; height:22px; border:0; background:none; cursor:pointer; color:var(--muted); font-size:10px; padding:0; }
.ce-layer__fold{ flex:none; width:16px; height:16px; color:var(--ink); display:inline-flex; } .ce-layer__fold svg{ width:16px; height:16px; }
.ce-layer__name{ flex:1 1 auto; min-width:0; font-size:12.5px; line-height:1.3; color:var(--ink); overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
.ce-layer__name input{ width:100%; font:inherit; border:1px solid var(--ink); padding:1px 4px; outline:none; }
.ce-layer__new{ flex:none; width:5px; height:5px; border-radius:50%; background:var(--gold); }
.ce-layer__b{ flex:none; width:26px; height:26px; border:0; background:none; cursor:pointer; color:var(--muted); display:inline-flex; align-items:center; justify-content:center; padding:0; }
.ce-layer__b svg{ width:15px; height:15px; }
.ce-layer__b:hover{ background:var(--hov); color:var(--ink); }
.ce-layer__b.is-quiet{ opacity:0; } .ce-layer:hover .ce-layer__b.is-quiet,.ce-layer__b.is-quiet:focus-visible{ opacity:1; }
.ce-layers__empty{ font-size:12px; line-height:1.7; color:var(--muted); padding:20px 12px; text-align:center; }
/* ── the hover card (C 5122-5146, 5871-5877) ── */
.ce-tip{ position:fixed; z-index:30; pointer-events:none; display:flex; align-items:center; gap:11px; max-width:264px; padding:9px 13px 9px 9px; background:#fff; border:1px solid var(--ink);
  box-shadow:0 14px 34px rgba(28,29,29,.19); opacity:0; transform:translateX(-6px) scale(.98); transform-origin:left center; transition:opacity .12s ease-out, transform .19s cubic-bezier(.16,1,.3,1); }
.ce-tip.is-in{ opacity:1; transform:none; }
.ce-tip__ico{ width:40px; height:40px; flex:none; display:block; overflow:visible; }
.ce-tip__ico .b{ fill:none; stroke:var(--gold); stroke-width:2.6; } .ce-tip__ico .c{ fill:#eddfbc; stroke:var(--gold); stroke-width:1.8; }
.ce-tip b{ display:flex; align-items:center; gap:7px; font-size:11.5px; font-weight:700; color:var(--ink); line-height:1.25; }
.ce-tip i{ font-style:normal; font-size:8px; font-weight:700; letter-spacing:.1em; text-transform:uppercase; color:var(--gold); border:1px solid var(--gold-lt); background:var(--band); padding:2px 5px; flex:none; }
.ce-tip span{ display:block; margin-top:3px; font-size:10.5px; line-height:1.45; color:var(--muted); }
/* ── asking before anything is thrown away ── */
.ce-ask{ position:absolute; inset:0; z-index:40; background:rgba(255,255,255,.55); display:flex; align-items:center; justify-content:center; padding:16px; }
.ce-ask__card{ width:min(380px,100%); background:#fff; border:1px solid var(--ink); box-shadow:0 18px 44px rgba(28,29,29,.22); padding:22px 22px 18px; }
.ce-ask__card h3{ margin:0 0 8px; font-family:var(--serif); font-weight:500; font-size:22px; line-height:1.15; color:var(--ink); }
.ce-ask__card p{ margin:0 0 18px; font-size:12.5px; line-height:1.6; color:var(--muted); }
.ce-ask__b{ display:flex; justify-content:flex-end; gap:10px; }
.ce-root :focus-visible{ outline:2px solid var(--gold); outline-offset:1px; }
.ce-root .ce-size input:focus-visible,.ce-root .ce-num input:focus-visible{ outline:none; }
/* ── the extension points (28 Oct 2026): a panel slot right of the Layers, the property bar's command buttons, a file held over the work area, a line said over it ── */
.ce-win.has-xpanel{ grid-template-columns:auto minmax(0,1fr) auto auto; }
.ce-win.has-xpanel.is-nodock{ grid-template-columns:auto minmax(0,1fr) auto; }
.ce-xpanel{ grid-area:2/4/5/5; width:clamp(260px,22vw,340px); min-height:0; display:flex; flex-direction:column; border:1px solid var(--hair); background:#fff; }
.ce-win.is-nodock .ce-xpanel{ grid-area:2/3/5/4; }
.ce-xpanel__body{ flex:1 1 auto; min-height:0; position:relative; display:flex; flex-direction:column; }
.ce-xpanel__pane{ flex:1 1 auto; min-height:0; overflow:auto; }
.ce-xbar{ display:inline-flex; gap:3px; flex:0 0 auto; }
.ce-xbar .ce-prop{ padding:0 7px; letter-spacing:.08em; }
.ce-xbar .ce-prop svg{ width:16px; height:16px; }
.ce-stage[data-xtool]{ cursor:var(--xcur,crosshair); } .ce-stage[data-xtool].is-space{ cursor:grab; } .ce-stage[data-xtool].is-panning{ cursor:grabbing; }
.ce-stage.is-drop::after{ content:""; position:absolute; inset:8px; border:2px dashed var(--gold); background:rgba(165,138,82,.07); pointer-events:none; }
.ce-say{ position:absolute; left:50%; bottom:14px; z-index:7; transform:translate(-50%,4px); max-width:calc(100% - 28px); padding:7px 13px; background:var(--ink); color:#fff; font-size:11.5px; line-height:1.4;
  pointer-events:none; opacity:0; transition:opacity .14s ease-out, transform .2s cubic-bezier(.16,1,.3,1); white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
.ce-say.is-in{ opacity:1; transform:translate(-50%,0); }
/* ── a short window sheds chrome before it sheds the charm (C 5099-5112, 5186-5200) ── */
@media (max-height:820px){ .ce-tool{ height:38px; } .ce-hist__b{ height:28px; } }
@media (max-height:660px){ .ce-tool{ height:33px; } .ce-tool svg{ width:16px; height:16px; } .ce-hist__b{ height:26px; } .ce-btn{ padding:9px 16px; } }
/* ── narrow: the layers fold over the room instead of beside it, and the frame goes edge to edge ── */
@media (max-width:900px){
  .ce-win,.ce-win.is-nodock{ grid-template-columns:auto minmax(0,1fr); }
  .ce-dock{ grid-area:auto; position:absolute; z-index:15; right:8px; top:52px; bottom:8px; width:min(300px,calc(100% - 64px)); box-shadow:-18px 0 44px rgba(28,29,29,.22); }
}
@media (max-width:640px){
  .ce-win{ inset:0; padding:8px 8px 8px; column-gap:7px; }
  .ce-head{ padding-bottom:7px; margin-bottom:6px; }
  .ce-id{ flex-direction:column; gap:2px; align-items:flex-start; }
  .ce-id h2{ font-size:18px; max-width:100%; }
  .ce-eyebrow{ font-size:8.5px; letter-spacing:.14em; max-width:100%; }
  .ce-head{ gap:6px; }
  .ce-head .ce-icon span{ display:none; }
  .ce-head .ce-seg button{ padding:0 8px; }
  .ce-long{ display:none; } .ce-short{ display:inline; }
  .ce-zoomb{ min-width:64px; }
  .ce-rail{ width:44px; padding:3px; }
  .ce-tool{ width:36px; height:38px; }
  .ce-hist__b{ width:36px; }
  .ce-props{ gap:5px; row-gap:5px; padding-bottom:6px; margin-bottom:6px; }
  .ce-props.is-empty{ display:none; }
  .ce-sizes{ display:contents; }
  .ce-size{ border:1px solid var(--hair); height:30px; }
  .ce-size--rot .ce-size__sep{ display:none; }
  .ce-size__f{ padding:0 5px; gap:4px; }
  .ce-size input{ width:34px; }
  .ce-size__f--rot input{ width:30px; }
  .ce-size__u{ padding:0 5px 0 2px; }
  .ce-size__sep{ width:6px; }
  .ce-hint{ display:none; }
  .ce-foot{ padding-top:8px; margin-top:6px; }
  .ce-foot{ gap:8px; }
  .ce-actions{ gap:6px; }
  .ce-btn{ padding:10px 18px; }
  .ce-root{ --rule:15px; }
  .ce-dock{ top:48px; right:0; bottom:0; width:min(300px,calc(100% - 48px)); }
}
@media (prefers-reduced-motion: reduce){ .ce-menu.is-in,.ce-menu.is-in--right{ animation:none; } .ce-tool,.ce-tool svg,.ce-tip{ transition:none; } }
@media (max-width:900px){
  .ce-win.has-xpanel,.ce-win.has-xpanel.is-nodock{ grid-template-columns:auto minmax(0,1fr); }
  .ce-xpanel,.ce-win.is-nodock .ce-xpanel{ grid-area:auto; position:absolute; z-index:16; right:8px; top:52px; bottom:8px; width:min(340px,calc(100% - 64px)); box-shadow:-18px 0 44px rgba(28,29,29,.22); }
}
@media (max-width:640px){ .ce-xpanel,.ce-win.is-nodock .ce-xpanel{ top:48px; right:0; bottom:0; width:min(340px,calc(100% - 48px)); } }
`;
  function injectCss() {
    if (doc.getElementById('ceCss')) return;
    const s = doc.createElement('style'); s.id = 'ceCss'; s.textContent = CSS; (doc.head || doc.documentElement).appendChild(s);
  }

  /** The extensions as one opening of the workspace sees them (the header's EXTENSION POINTS): checked, the first of an id kept, the editor's own tool ids left to it. */
  const BUILTIN_TOOLS = ['select', 'marquee', 'text', 'rect', 'ellipse', 'line', 'hand'];
  function extensions() {
    const arr = v => (Array.isArray(v) ? v.filter(x => x && typeof x === 'object' && x.id != null && String(x.id) !== '') : []);
    const uniq = list => { const seen = new Set(); return list.filter(x => { const k = String(x.id); if (seen.has(k)) return false; seen.add(k); return true; }); };
    const fns = v => (Array.isArray(v) ? v.filter(f => typeof f === 'function') : []);
    const cmds = [], c = api.commands && typeof api.commands === 'object' ? api.commands : {};
    for (const id of Object.keys(c)) if (c[id] && typeof c[id].run === 'function') cmds.push({ id, c: c[id] });
    return { tools: uniq(arr(api.tools)).filter(t => !BUILTIN_TOOLS.includes(String(t.id))), commands: cmds, importers: uniq(arr(api.importers)).filter(i => typeof i.run === 'function'),
      panels: uniq(arr(api.panels)).filter(p => typeof p.mount === 'function'), onOpen: fns(api.onOpen), onClose: fns(api.onClose) };
  }
  /** 'Mod+Shift+O' -> { mod, shift, alt, key: 'o' } (Mod, Cmd, Ctrl, Meta, ⌘ are all the platform's command key, as the editor's own keys take either). */
  function parseKey(s) {
    const parts = String(s || '').split('+').map(p => p.trim()).filter(Boolean); if (!parts.length) return null;
    const k = { mod: false, shift: false, alt: false, key: parts.pop().toLowerCase() };
    for (const p of parts.map(q => q.toLowerCase())) { if (/^(mod|cmd|ctrl|control|meta|⌘)$/.test(p)) k.mod = true; else if (/^(shift|⇧)$/.test(p)) k.shift = true; else if (/^(alt|opt|option|⌥)$/.test(p)) k.alt = true; }
    return k;
  }
  const keyLabel = s => { const k = parseKey(s); return k ? (k.shift ? '⇧' : '') + (k.alt ? '⌥' : '') + (k.mod ? '⌘' : '') + (k.key.length === 1 ? k.key.toUpperCase() : k.key) : ''; };

  let live = null;
  function open(opts) {
    if (!doc || !doc.body) return Promise.reject(new Error('CharmEditor.open needs a page'));
    if (live) return Promise.reject(new Error('CharmEditor is already open'));
    return new Promise(resolve => {
      live = workspace(opts || {}, v => { live = null; api.current = null; resolve(v); });
      api.current = live;
    });
  }

  function workspace(opts, done) {
    injectCss();
    const geo = createGeo(opts.textToPaths);
    const fonts = (Array.isArray(opts.fonts) ? opts.fonts : []).filter(f => f && f.key != null).map(f => ({ key: f.key, name: f.name == null ? String(f.key) : String(f.name) }));
    if (!fonts.length) fonts.push({ key: 'sans', name: 'Sans' });
    const lockRoles = Array.isArray(opts.lockRoles) ? opts.lockRoles : ['cut'];
    const S = {
      faces: {}, face: null, sel: [], tool: 'select', shape: 'rect', k: 1, ox: 0, oy: 0, fitted: false,
      hist: new History(120), clip: null, pasteN: 0, grid: true, snapGrid: false, smart: true, link: true,
      textFont: fonts[0].key, textSize: 8, textWeight: 0, textRole: 'hatch', shapeRole: 'line', lw: LW_LASER_PT, run: null,
      editing: null, g: null, space: false, hover: null, open: new Set(), dock: true, nudgeAt: 0, ask: null
    };
    for (const f of ['front', 'back']) {
      const fd = opts.faces && opts.faces[f]; if (!fd || typeof fd !== 'object') continue;
      const items = prepIn(fd.items, lockRoles);
      S.faces[f] = { face: f, frame: frameOf(fd.frame, items), items, base: items, docIn: fd };
    }
    if (!S.faces.front && !S.faces.back) S.faces.front = { face: 'front', frame: { x0: 0, y0: 0, wPt: 72, hPt: 72 }, items: [], base: [], docIn: {} };
    S.face = S.faces[opts.startFace] ? opts.startFace : S.faces.front ? 'front' : 'back';
    { const f = S.faces[S.face].frame; S.textSize = clamp(Math.round(Math.min(f.wPt, f.hPt) / 9 * 2) / 2, 3, 24); }
    try { const v = root.localStorage && root.localStorage.getItem('ce.dock'); if (v === '0') S.dock = false; } catch (e) { /* storage blocked: the panel opens */ }
    if (root.innerWidth && root.innerWidth <= 900) S.dock = false;

    function frameOf(fr, items) {
      if (fr && [fr.x0, fr.y0, fr.wPt, fr.hPt].every(fin) && fr.wPt > 0 && fr.hPt > 0) return { x0: fr.x0, y0: fr.y0, wPt: fr.wPt, hPt: fr.hPt };
      let b = null; for (const it of items) b = union(b, geo.drawn(it, I));
      b = b || [0, 0, 72, 72];
      return { x0: b[0], y0: b[1], wPt: Math.max(1, b[2] - b[0]), hPt: Math.max(1, b[3] - b[1]) };
    }

    /* ── the chrome ── */
    const prevFocus = doc.activeElement, prevOverflow = doc.documentElement.style.overflow;
    const rootEl = doc.createElement('div');
    rootEl.className = 'ce-root';
    const title = opts.title == null ? 'Charm editor' : String(opts.title), subtitle = opts.subtitle == null ? '' : String(opts.subtitle);
    const roleItems = ROLES.map(r => `<button class="ce-item" type="button" data-role="${r}" role="menuitemradio"><i class="ce-sw ce-sw--${r}"></i>${ROLE_NAME[r]}<em>${ROLE_SAYS[r]}</em></button>`).join('');
    const fontItems = fonts.map(f => `<button class="ce-item" type="button" data-font="${esc(f.key)}" role="menuitemradio">${esc(f.name)}</button>`).join('');
    /* the extensions' places in the chrome (the header's EXTENSION POINTS): their tools on the rail before More (a flyout of their own, or the Shapes menu), the Object
       menu, Import and a button per panel in the head, command buttons at the end of the property bar, the panel slot right of the Layers */
    const X = extensions();
    const xTool = id => X.tools.find(t => String(t.id) === String(id)) || null;
    const xIcon = t => (t && typeof t.icon === 'string' && /^\s*<svg/i.test(t.icon) ? t.icon : IC.tool);
    const xKeyOf = t => (t.key ? String(t.key).toUpperCase() : '');
    const xItem = t => `<button class="ce-item" type="button" data-tool="${esc(t.id)}" role="menuitem">${xIcon(t)}${esc(t.title || t.id)}${xKeyOf(t) ? `<em>${esc(xKeyOf(t))}</em>` : ''}</button>`;
    const xFly = new Map(), xRail = [];
    for (const t of X.tools) {
      const f = t.flyout ? String(t.flyout) : '';
      if (f === 'shapes') continue;
      if (!f) { xRail.push({ solo: t }); continue; }
      if (!xFly.has(f)) { xFly.set(f, []); xRail.push({ fly: f }); }
      xFly.get(f).push(t);
    }
    const xRailHtml = xRail.map(r => {
      const t = r.solo || xFly.get(r.fly)[0], label = esc(t.title || t.id) + (xKeyOf(t) ? ` (${esc(xKeyOf(t))})` : '');
      if (r.solo) return `<button class="ce-tool" type="button" data-tool="${esc(t.id)}" data-tip="${esc(t.id)}" aria-label="${label}">${xIcon(t)}</button>`;
      return `<div class="ce-drop" data-drop="x-${esc(r.fly)}"><button class="ce-tool ce-drop__b" type="button" data-tool="${esc(t.id)}" data-tip="${esc(t.id)}" aria-label="${label}">${xIcon(t)}</button>`
        + `<div class="ce-menu" role="menu" hidden><div class="ce-menu__head">${esc(r.fly)}</div>${xFly.get(r.fly).map(xItem).join('')}</div></div>`;
    }).join('');
    const xShapesHtml = X.tools.filter(t => String(t.flyout || '') === 'shapes').map(xItem).join('');
    const xAccept = [].concat(...X.importers.map(i => (Array.isArray(i.accept) ? i.accept.map(String) : [])));
    const xHeadHtml = `<div class="ce-drop" data-drop="xcmd"${X.commands.some(c => c.c.menu !== false) ? '' : ' hidden'}><button class="ce-icon ce-drop__b" type="button" aria-label="Object" title="Object: what can be done to the selection">${IC.object}<span>Object</span></button><div class="ce-menu" role="menu" hidden></div></div>`
      + `<button class="ce-icon" type="button" data-act="import" aria-label="Import a file" title="Import a file (or drop it on the charm, or paste it): ${esc(xAccept.join(' '))}"${X.importers.length ? '' : ' hidden'}>${IC.import}<span>Import</span></button>`
      + `<input class="ce-file" type="file" multiple hidden tabindex="-1" aria-hidden="true">`
      + X.panels.map(p => `<button class="ce-icon" type="button" data-xpanel="${esc(p.id)}" aria-label="${esc(p.title || p.id)}" title="${esc(p.title || p.id)}">${typeof p.icon === 'string' && /^\s*<svg/i.test(p.icon) ? p.icon : IC.panel}<span>${esc(p.title || p.id)}</span></button>`).join('');
    const xPanelHtml = !X.panels.length ? '' : `<aside class="ce-xpanel" aria-label="Panels" hidden><div class="ce-dock__tabs" role="tablist">`
      + X.panels.map(p => `<button class="ce-dock__tab" type="button" data-xtab="${esc(p.id)}" role="tab" aria-selected="false">${esc(p.title || p.id)}</button>`).join('')
      + `<button class="ce-dock__x" type="button" data-act="xpanel0" aria-label="Fold the panel away" title="Fold the panel away">&raquo;</button></div>`
      + `<div class="ce-xpanel__body">${X.panels.map(p => `<div class="ce-xpanel__pane" data-pane="${esc(p.id)}" role="tabpanel" hidden></div>`).join('')}</div></aside>`;
    rootEl.innerHTML = `
<div class="ce-win" tabindex="-1" role="dialog" aria-modal="true" aria-label="${esc(title)}">
  <header class="ce-head">
    <div class="ce-id"><h2 title="${esc(title)}">${esc(title)}</h2>${subtitle ? `<span class="ce-eyebrow" title="${esc(subtitle)}">${esc(subtitle)}</span>` : ''}</div>
    <div class="ce-seg" role="tablist" aria-label="Face">
      <button type="button" data-face="front" role="tab"${S.faces.front ? '' : ' disabled'}>Front</button><button type="button" data-face="back" role="tab"${S.faces.back ? '' : ' disabled'}>Back</button>
    </div>
    <button class="ce-icon" type="button" data-act="dock" aria-label="Layers" title="Layers">${IC.layers}<span>Layers</span></button>
    ${xHeadHtml}
    <button class="ce-icon ce-close" type="button" data-act="cancel" aria-label="Close without saving" title="Close (Esc)">×</button>
  </header>
  <div class="ce-rail" role="toolbar" aria-label="Tools" aria-orientation="vertical">
    <div class="ce-tools">
      <button class="ce-tool is-on" type="button" data-tool="select" data-tip="select" aria-label="Select (V)">${IC.select}</button>
      <button class="ce-tool" type="button" data-tool="marquee" data-tip="marquee" aria-label="Marquee (M)">${IC.marquee}</button>
      <button class="ce-tool" type="button" data-tool="text" data-tip="text" aria-label="Text (T)">${IC.text}</button>
      <div class="ce-drop" data-drop="shapes">
        <button class="ce-tool ce-drop__b" type="button" data-tool="rect" data-shapes data-tip="shapes" aria-label="Shapes: rectangle (R), ellipse (E), line (L)">${IC.rect}</button>
        <div class="ce-menu" role="menu" hidden>
          <div class="ce-menu__head">Shapes</div>
          <button class="ce-item" type="button" data-tool="rect" role="menuitem">${IC.rect}Rectangle<em>R</em></button>
          <button class="ce-item" type="button" data-tool="ellipse" role="menuitem">${IC.ellipse}Ellipse<em>E</em></button>
          <button class="ce-item" type="button" data-tool="line" role="menuitem">${IC.line}Line<em>L</em></button>
          ${xShapesHtml}
        </div>
      </div>
      <button class="ce-tool" type="button" data-tool="hand" data-tip="hand" aria-label="Hand (H, or hold Space)">${IC.hand}</button>
      ${xRailHtml}
      <div class="ce-drop" data-drop="more">
        <button class="ce-tool ce-drop__b" type="button" data-tip="more" aria-label="More">${IC.more}</button>
        <div class="ce-menu" role="menu" hidden>
          <div class="ce-menu__head">While you work</div>
          <button class="ce-item ce-item--tog" type="button" data-opt="grid" role="menuitemcheckbox"><i class="ce-tick"></i>Millimetre grid</button>
          <button class="ce-item ce-item--tog" type="button" data-opt="snapGrid" role="menuitemcheckbox"><i class="ce-tick"></i>Snap to the grid</button>
          <button class="ce-item ce-item--tog" type="button" data-opt="smart" role="menuitemcheckbox"><i class="ce-tick"></i>Smart guides<em>Alt pauses</em></button>
          <div class="ce-menu__head">Zoom</div>
          <button class="ce-item" type="button" data-zoom="fit" role="menuitem">Fit the charm<em>⌘0</em></button>
          <button class="ce-item" type="button" data-zoom="100" role="menuitem">Actual size<em>⌘1</em></button>
          <button class="ce-item" type="button" data-zoom="sel" role="menuitem">Zoom to selection<em>⌘2</em></button>
          <div class="ce-menu__head">Layers</div>
          <button class="ce-item" type="button" data-cmd="unlockAll" role="menuitem">Unlock everything</button>
          <button class="ce-item" type="button" data-cmd="showAll" role="menuitem">Show everything</button>
          <div class="ce-menu__note">Hold <b>Space</b> and drag to move around. <b>Alt</b>-click reaches inside a group. Arrows nudge 0.05 mm, <b>Shift</b> 1 mm.</div>
        </div>
      </div>
    </div>
    <div class="ce-hist">
      <button class="ce-hist__b" type="button" data-act="undo" data-tip="undo" aria-label="Undo (⌘Z)" disabled>${IC.undo}</button>
      <button class="ce-hist__b" type="button" data-act="redo" data-tip="redo" aria-label="Redo (⇧⌘Z)" disabled>${IC.redo}</button>
    </div>
  </div>
  <div class="ce-props">
    <div class="ce-sizes" data-size>
      <div class="ce-size">
        <label class="ce-size__f" title="Left edge, in mm from the charm's left edge"><span>X</span><input data-f="x" inputmode="decimal" autocomplete="off" spellcheck="false" aria-label="X position, mm" disabled></label>
        <label class="ce-size__f" title="Top edge, in mm from the charm's top edge"><span>Y</span><input data-f="y" inputmode="decimal" autocomplete="off" spellcheck="false" aria-label="Y position, mm" disabled></label>
        <span class="ce-size__sep" aria-hidden="true"></span>
        <label class="ce-size__f" title="Width, mm"><span>W</span><input data-f="w" inputmode="decimal" autocomplete="off" spellcheck="false" aria-label="Width, mm" disabled></label>
        <button class="ce-size__link is-on" type="button" data-act="link" aria-pressed="true" title="Keep the proportions">${IC.link}</button>
        <label class="ce-size__f" title="Height, mm"><span>H</span><input data-f="h" inputmode="decimal" autocomplete="off" spellcheck="false" aria-label="Height, mm" disabled></label>
        <i class="ce-size__u" aria-hidden="true">mm</i>
      </div>
      <div class="ce-size ce-size--rot">
        <span class="ce-size__sep" aria-hidden="true"></span>
        <label class="ce-size__f ce-size__f--rot" title="Angle, degrees clockwise"><span>&#8635;</span><input data-f="r" inputmode="decimal" autocomplete="off" spellcheck="false" aria-label="Angle, degrees" disabled></label>
        <i class="ce-size__u ce-size__u--deg" aria-hidden="true">&deg;</i>
      </div>
    </div>
    <div class="ce-drop" data-drop="font" data-when="text" hidden>
      <button class="ce-prop ce-drop__b" type="button" title="Font"><span class="ce-fontA">Aa</span><span class="ce-fontname" data-fontname></span><i class="ce-caret"></i></button>
      <div class="ce-menu" role="menu" hidden><div class="ce-menu__head">Font</div>${fontItems}</div>
    </div>
    <label class="ce-num" data-when="text" title="Text size, points" hidden><input data-f="pt" inputmode="decimal" autocomplete="off" spellcheck="false" aria-label="Text size, points"><i>pt</i></label>
    <label class="ce-num ce-num--thick" data-when="weight" title="Weight: how much thicker (+) or thinner (−) every stroke of the letters is than the font draws them, in mm · ↑ ↓ 0.01 mm, Shift 0.05 mm" hidden><button class="ce-num__b" type="button" data-act="wt0" title="Weight · back to the font's own weight" aria-label="Back to the font's own weight">${IC.weight}</button><input data-f="wt" inputmode="decimal" autocomplete="off" spellcheck="false" aria-label="Weight, mm thicker or thinner than the font draws it"><i>mm</i></label>
    <div class="ce-drop" data-drop="role" data-when="role" hidden>
      <button class="ce-prop ce-drop__b" type="button" title="What the laser does with it"><i class="ce-sw" data-rolesw></i><span data-rolename></span><i class="ce-caret"></i></button>
      <div class="ce-menu" role="menu" hidden><div class="ce-menu__head">What the laser does</div>${roleItems}</div>
    </div>
    <label class="ce-num ce-num--thick" data-when="stroke" title="Line width on the charm, in mm, as the laser file writes it · ↑ ↓ 0.01 mm, Shift 0.05 mm" hidden><button class="ce-num__b" type="button" data-act="lw0" title="Line width · back to the width it came with" aria-label="Back to the width it came with">${IC.stroke}</button><input data-f="lw" inputmode="decimal" autocomplete="off" spellcheck="false" aria-label="Line width, mm"><i>mm</i></label>
    <div class="ce-xbar" data-when="xbar" hidden></div>
  </div>
  <div class="ce-wrap">
    <span class="ce-rule ce-rule__corner" aria-hidden="true">mm</span>
    <canvas class="ce-rule ce-rule--x" aria-hidden="true"></canvas>
    <canvas class="ce-rule ce-rule--y" aria-hidden="true"></canvas>
    <div class="ce-stage" data-tool="select">
      <div class="ce-paper"></div>
      <svg class="ce-art" xmlns="http://www.w3.org/2000/svg" preserveAspectRatio="none" role="img" aria-label="The charm"><g class="ce-grid"></g><g class="ce-items"></g></svg>
      <svg class="ce-ui" xmlns="http://www.w3.org/2000/svg" aria-hidden="true"></svg>
      <div class="ce-say" role="status" aria-live="polite"></div>
    </div>
  </div>
  <footer class="ce-foot">
    <div class="ce-drop" data-drop="zoom">
      <button class="ce-prop ce-drop__b ce-zoomb" type="button" title="Zoom" aria-label="Zoom"><span data-zoomread>100%</span><i class="ce-caret"></i></button>
      <div class="ce-menu" role="menu" hidden>
        <div class="ce-menu__head">Zoom</div>
        <button class="ce-item" type="button" data-zoom="in" role="menuitem">Zoom in<em>⌘+</em></button>
        <button class="ce-item" type="button" data-zoom="out" role="menuitem">Zoom out<em>⌘−</em></button>
        <button class="ce-item" type="button" data-zoom="fit" role="menuitem">Fit the charm<em>⌘0</em></button>
        <button class="ce-item" type="button" data-zoom="100" role="menuitem">Actual size<em>⌘1</em></button>
        <button class="ce-item" type="button" data-zoom="sel" role="menuitem">Zoom to selection<em>⌘2</em></button>
        <div class="ce-menu__sep"></div>
        <button class="ce-item" type="button" data-zoom="400" role="menuitem">400%</button>
        <button class="ce-item" type="button" data-zoom="1000" role="menuitem">1000%</button>
        <button class="ce-item" type="button" data-zoom="2000" role="menuitem">2000%</button>
        <button class="ce-item" type="button" data-zoom="4000" role="menuitem">4000%</button>
      </div>
    </div>
    <span class="ce-hint" data-hint></span>
    <div class="ce-actions">
      <button class="ce-btn ce-btn--ghost" type="button" data-act="cancel">Cancel</button>
      <button class="ce-btn ce-btn--gold" type="button" data-act="save">Save</button>
    </div>
  </footer>
  <aside class="ce-dock" aria-label="Layers">
    <div class="ce-dock__tabs" role="tablist">
      <button class="ce-dock__tab" type="button" data-face="front" role="tab"${S.faces.front ? '' : ' disabled'}>Front<b data-count="front">0</b></button>
      <button class="ce-dock__tab" type="button" data-face="back" role="tab"${S.faces.back ? '' : ' disabled'}>Back<b data-count="back">0</b></button>
      <button class="ce-dock__x" type="button" data-act="dock" aria-label="Fold the layers away" title="Fold the layers away">&raquo;</button>
    </div>
    <div class="ce-pane__bar">
      <button class="ce-act" type="button" data-act="forward" title="Bring forward (⌘])" aria-label="Bring forward">${IC.up}</button>
      <button class="ce-act" type="button" data-act="backward" title="Send backward (⌘[)" aria-label="Send backward">${IC.down}</button>
      <span class="ce-act__sep" aria-hidden="true"></span>
      <button class="ce-act" type="button" data-act="group" title="Group (⌘G)" aria-label="Group">${IC.group}</button>
      <button class="ce-act" type="button" data-act="ungroup" title="Ungroup (⇧⌘G)" aria-label="Ungroup">${IC.ungroup}</button>
      <span class="ce-act__sep" aria-hidden="true"></span>
      <button class="ce-act ce-act--danger" type="button" data-act="delete" title="Delete (⌫)" aria-label="Delete">${IC.del}</button>
    </div>
    <div class="ce-layers" role="listbox" aria-label="Layers, topmost first" aria-multiselectable="true"></div>
  </aside>
  ${xPanelHtml}
  <div class="ce-menu ce-ctx" role="menu" hidden></div>
</div>`;
    doc.body.appendChild(rootEl);
    doc.documentElement.style.overflow = 'hidden';
    const $ = s => rootEl.querySelector(s), $$ = s => Array.from(rootEl.querySelectorAll(s));
    const win = $('.ce-win'), stage = $('.ce-stage'), art = $('.ce-art'), gridG = $('.ce-grid'), itemsG = $('.ce-items'), ui = $('.ce-ui'), paper = $('.ce-paper');
    const rx = $('.ce-rule--x'), ry = $('.ce-rule--y'), layersEl = $('.ce-layers'), dock = $('.ce-dock'), ctx = $('.ce-ctx');
    const field = f => $(`[data-f="${f}"]`);
    const SVGNS = 'http://www.w3.org/2000/svg';

    /* ── state helpers ── */
    const face = () => S.faces[S.face];
    const items = () => face().items;
    const frame = () => face().frame;
    let ixFor = null, ixMap = null;
    const ix = () => { if (ixFor !== items()) { ixMap = indexTree(items(), geo); ixFor = items(); } return ixMap; };
    const entry = id => ix().get(id) || null;
    const isSelected = id => { if (S.sel.includes(id)) return true; const e = entry(id); return !!(e && e.anc.some(a => S.sel.includes(a))); };
    const selEntries = () => S.sel.map(entry).filter(Boolean);
    const leavesOf = (it, out) => { out = out || []; if (it.kind === 'group') (it.children || []).forEach(c => leavesOf(c, out)); else out.push(it); return out; };
    const selLeaves = () => [].concat(...selEntries().map(e => leavesOf(e.it)));
    function setItems(next) { face().items = next; }
    function snapshot(f) { f = f || S.face; return { face: f, items: S.faces[f].items, sel: f === S.face ? S.sel.slice() : [] }; }
    function pushUndo() { if (xPending) xCommit(); S.hist.push(snapshot()); S.nudgeAt = 0; syncHist(); }
    function setSel(ids) {
      const ixm = ix();
      S.sel = topmost(ids.filter(id => ixm.has(id)), ixm);
      renderUI(); paintLayerSel(true); syncProps();
      xChanged();
    }

    /* ── view: px per pt (k) and where the frame's top-left sits in the room (ox, oy). The SVG's viewBox is set from these, so nothing is ever scaled as a bitmap ── */
    const sb = () => { const r = stage.getBoundingClientRect(); return { l: r.left, t: r.top, w: r.width, h: r.height }; };
    const toStage = (x, y) => { const f = frame(); return [(x - f.x0) * S.k + S.ox, (y - f.y0) * S.k + S.oy]; };
    const fromClient = (cx, cy) => { const b = sb(), f = frame(); return [f.x0 + (cx - b.l - S.ox) / S.k, f.y0 + (cy - b.t - S.oy) / S.k]; };
    const V = () => { const f = frame(); return [S.k, 0, 0, S.k, S.ox - f.x0 * S.k, S.oy - f.y0 * S.k]; };
    const zoomPct = () => S.k / PX100 * 100;
    function clampPan() {
      const b = sb(), f = frame(), fw = f.wPt * S.k, fh = f.hPt * S.k, keep = Math.min(60, b.w / 3, b.h / 3);
      S.ox = clamp(S.ox, keep - fw, b.w - keep); S.oy = clamp(S.oy, keep - fh, b.h - keep);
    }
    function zoomAtStage(k, sx, sy) {
      k = clamp(k, ZMIN * PX100, ZMAX * PX100);
      const f = frame(), px = f.x0 + (sx - S.ox) / S.k, py = f.y0 + (sy - S.oy) / S.k;
      S.k = k; S.ox = sx - (px - f.x0) * k; S.oy = sy - (py - f.y0) * k; S.fitted = false;
      clampPan(); applyView();
    }
    const zoomAtClient = (cx, cy, factor) => { const b = sb(); zoomAtStage(S.k * factor, cx - b.l, cy - b.t); };
    function zoomTo(pct) { const b = sb(); zoomAtStage(pct / 100 * PX100, b.w / 2, b.h / 2); }
    function fit() {
      const b = sb(), f = frame(); if (b.w < 4 || b.h < 4) return;
      const m = clamp(Math.min(b.w, b.h) * 0.07, 14, 44);
      S.k = clamp(Math.min((b.w - 2 * m) / f.wPt, (b.h - 2 * m) / f.hPt), ZMIN * PX100, ZMAX * PX100);
      S.ox = (b.w - f.wPt * S.k) / 2; S.oy = (b.h - f.hPt * S.k) / 2; S.fitted = true; lastStage = b;
      applyView();
    }
    function zoomToBox(bb) {
      if (!bb) return fit();
      const b = sb(), w = Math.max(bb[2] - bb[0], 0.5 * PT), h = Math.max(bb[3] - bb[1], 0.5 * PT);
      S.k = clamp(Math.min(b.w * 0.6 / w, b.h * 0.6 / h), ZMIN * PX100, ZMAX * PX100); S.fitted = false;
      const f = frame(), cx = (bb[0] + bb[2]) / 2, cy = (bb[1] + bb[3]) / 2;
      S.ox = b.w / 2 - (cx - f.x0) * S.k; S.oy = b.h / 2 - (cy - f.y0) * S.k; lastStage = b; clampPan(); applyView();
    }
    /* THE CHARM STAYS STILL WHEN THE ROOM CHANGES SIZE. The property bar wraps to a second row when a text is selected (font, size and role join it), and the room
       below it gets shorter. Re-fitting then, or leaving the drawing anchored to the room's own corner, made the charm jump under the hand at the moment of the
       click. So the drawing is held where it is ON SCREEN and the room's edge simply moves over it; only a window resize or folding the Layers re-fits a view that
       was fitted. */
    let lastStage = null;
    function zoomCmd(z) {
      if (z === 'fit') fit(); else if (z === '100') zoomTo(100); else if (z === 'sel') zoomToBox(selBox());
      else if (z === 'in' || z === 'out') { const b = sb(); zoomAtStage(S.k * (z === 'in' ? 1.5 : 1 / 1.5), b.w / 2, b.h / 2); }
      else if (fin(+z)) zoomTo(+z);
    }
    function applyView() {
      const b = sb(); if (b.w < 2 || b.h < 2) return;
      if (lastStage && (b.l !== lastStage.l || b.t !== lastStage.t)) { S.ox -= b.l - lastStage.l; S.oy -= b.t - lastStage.t; }
      lastStage = b;
      const f = frame(), vx = f.x0 - S.ox / S.k, vy = f.y0 - S.oy / S.k, vw = b.w / S.k, vh = b.h / S.k;
      art.setAttribute('viewBox', `${vx} ${vy} ${vw} ${vh}`);
      art.style.setProperty('--k', String(S.k));
      // (a line is drawn at its true width at every zoom, lwPt × px per pt; only a line thinner than one pixel of the screen is held at that pixel, so it stays in sight)
      art.style.setProperty('--hair', (1 / Math.max(1, +root.devicePixelRatio || 1)) + 'px');
      Object.assign(paper.style, { left: S.ox + 'px', top: S.oy + 'px', width: f.wPt * S.k + 'px', height: f.hPt * S.k + 'px' });
      drawGrid(vx, vy, vw, vh);
      queueRulers(); renderUI();
      $('[data-zoomread]').textContent = fmtZoom(zoomPct());
      if (S.editing) placeEditor();
    }
    const fmtZoom = p => (p < 100 ? trim(p, 0) : String(Math.round(p))) + '%';

    /* a TRUE millimetre grid: lines every mm from the charm's corner, every 0.1 mm once they are 6 px apart, every 5 mm a shade darker; only where the charm is,
       only what is on screen, one path per weight */
    function drawGrid(vx, vy, vw, vh) {
      if (!S.grid) { gridG.innerHTML = ''; return; }
      const f = frame(), pxmm = S.k * PT;
      const x0 = Math.max(f.x0, vx), x1 = Math.min(f.x0 + f.wPt, vx + vw), y0 = Math.max(f.y0, vy), y1 = Math.min(f.y0 + f.hPt, vy + vh);
      if (!(x1 > x0 && y1 > y0)) { gridG.innerHTML = ''; return; }
      const fine = pxmm * 0.1 >= 6, mm = pxmm >= 4, five = pxmm * 5 >= 4;
      const lines = (stepMm, skip) => {
        let d = ''; const st = stepMm * PT, R = v => Math.round(v * 1e4) / 1e4;
        const a = Math.ceil((x0 - f.x0) / st - 1e-9), bx = Math.floor((x1 - f.x0) / st + 1e-9);
        if (bx - a < 1500) for (let n = a; n <= bx; n++) { if (skip && n % skip === 0) continue; const x = f.x0 + n * st; d += `M${R(x)} ${R(y0)}V${R(y1)}`; }
        const c = Math.ceil((y0 - f.y0) / st - 1e-9), by = Math.floor((y1 - f.y0) / st + 1e-9);
        if (by - c < 1500) for (let n = c; n <= by; n++) { if (skip && n % skip === 0) continue; const y = f.y0 + n * st; d += `M${R(x0)} ${R(y)}H${R(x1)}`; }
        return d;
      };
      let h = '';
      if (fine) h += `<path class="g-fine" d="${lines(0.1, 10)}"/>`;
      if (mm) h += `<path class="g-mm" d="${lines(1, five ? 5 : 0)}"/>`;
      if (five) h += `<path class="g-five" d="${lines(5, 0)}"/>`; else h += `<path class="g-mm" d="${lines(10, 0)}"/>`;
      gridG.innerHTML = h;
    }

    /* ── the rulers (the studio's mkRulerDraw, J 19196-19291: one routine for both axes, ticks from a ladder of numbers a person would write, the cursor line) ── */
    const STEPS = [0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 20, 50, 100];
    let ruleRaf = 0, cursor = null;
    const queueRulers = () => { if (!ruleRaf) ruleRaf = requestAnimationFrame(drawRulers); };
    function drawRulers() {
      ruleRaf = 0;
      const dpr = Math.min(2, root.devicePixelRatio || 1);
      const pxmm = S.k * PT, ink = '#1c1d1d', muted = '#8a8784', hair = '#d9d4ca', liveC = '#b8935a';
      const step = minPx => { for (const s of STEPS) if (s * pxmm >= minPx) return s; return STEPS[STEPS.length - 1]; };
      const draw = (cv, vertical) => {
        const box = cv.getBoundingClientRect(), len = vertical ? box.height : box.width, thick = vertical ? box.width : box.height;
        if (len < 4 || thick < 2) return;
        if (cv.width !== Math.round(box.width * dpr) || cv.height !== Math.round(box.height * dpr)) { cv.width = Math.max(1, Math.round(box.width * dpr)); cv.height = Math.max(1, Math.round(box.height * dpr)); }
        const c = cv.getContext('2d'); if (!c) return;
        c.setTransform(dpr, 0, 0, dpr, 0, 0); c.clearRect(0, 0, box.width, box.height);
        c.font = '700 8px ' + (getComputedStyle(rootEl).fontFamily || 'sans-serif'); c.textBaseline = 'top';
        const s = sb(), origin = (vertical ? s.t + S.oy : s.l + S.ox) - (vertical ? box.top : box.left);
        const minor = step(6), major = step(44), u0 = -origin / pxmm, u1 = (len - origin) / pxmm;
        c.strokeStyle = hair; c.lineWidth = 1; c.beginPath();
        const first = Math.ceil(u0 / minor) * minor, count = Math.min(4000, Math.floor((u1 - first) / minor) + 1);
        for (let i = 0; i < count; i++) {
          const u = first + i * minor, p = Math.round(origin + u * pxmm) + 0.5; if (p < 0 || p > len) continue;
          const big = Math.abs(u / major - Math.round(u / major)) < 1e-6, h = big ? thick - 1 : Math.min(4, thick * 0.28);
          if (vertical) { c.moveTo(thick - h, p); c.lineTo(thick - 1, p); } else { c.moveTo(p, thick - h); c.lineTo(p, thick - 1); }
        }
        c.stroke();
        const mFirst = Math.ceil(u0 / major) * major, mCount = Math.min(400, Math.floor((u1 - mFirst) / major) + 1);
        for (let i = 0; i < mCount; i++) {
          const u = mFirst + i * major, p = Math.round(origin + u * pxmm); if (p < -20 || p > len + 20) continue;
          const txt = String(+(Math.abs(u) < 1e-9 ? 0 : u).toFixed(2));
          c.fillStyle = Math.abs(u) < 1e-9 ? ink : muted;
          if (vertical) { c.save(); c.translate(1.5, p + 2); c.rotate(-Math.PI / 2); c.textAlign = 'right'; c.fillText(txt, 0, 0); c.restore(); }
          else { c.textAlign = 'left'; c.fillText(txt, p + 2.5, 2); }
        }
        if (cursor) {
          const p = Math.round(vertical ? cursor.y - box.top : cursor.x - box.left) + 0.5;
          if (p >= 0 && p <= len) { c.strokeStyle = liveC; c.beginPath(); if (vertical) { c.moveTo(0, p); c.lineTo(thick, p); } else { c.moveTo(p, 0); c.lineTo(p, thick); } c.stroke(); }
        }
      };
      draw(rx, false); draw(ry, true);
    }

    /* ── drawing the charm: one element per item, kept by id; an item object that has not changed is not touched (the tree is immutable, so "changed" is "!==") ── */
    const els = new Map();
    function render() {
      const keep = new Set();
      const build = (list, parentEl) => {
        let i = 0;
        for (const it of list) {
          keep.add(it.id);
          const tag = it.kind === 'group' ? 'g' : 'path';
          let el = els.get(it.id);
          if (!el || el.tagName.toLowerCase() !== tag) { el = doc.createElementNS(SVGNS, tag); el.setAttribute('data-id', it.id); els.set(it.id, el); el.__it = null; }
          if (el.__it !== it) {
            const m = geo.matrix(it);
            if (isI(m)) el.removeAttribute('transform'); else el.setAttribute('transform', `matrix(${m.map(v => +v.toFixed(9)).join(' ')})`);
            if (it.hidden) el.setAttribute('display', 'none'); else el.removeAttribute('display');
            if (tag === 'path') {
              const g = geo.raw(it);
              el.setAttribute('d', dOf(g.subpaths) || 'M0 0');
              el.setAttribute('class', `ce-p ce-p--${it.role}${paintFill(it) ? ' is-fill' : ''}${paintStroke(it) ? ' is-stroke' : ''}${it.evenOdd || it.fillRule === 'evenodd' ? ' is-evenodd' : ''}`);
              el.setAttribute('vector-effect', 'non-scaling-stroke');
              el.style.setProperty('--lw', String(lwOf(it)));
              el.style.fill = greyOf(it); if (it.artGrey && it.even === true) el.classList.add('is-evenodd');   // (a traced image's grey, its holes even-odd as the laser file fills them)
            } else el.setAttribute('class', 'ce-g');
            el.__it = it;
          }
          if (parentEl.childNodes[i] !== el) parentEl.insertBefore(el, parentEl.childNodes[i] || null);
          if (it.kind === 'group') build(it.children || [], el);
          i++;
        }
        while (parentEl.childNodes.length > i) parentEl.removeChild(parentEl.lastChild);
      };
      build(items(), itemsG);
      for (const id of [...els.keys()]) if (!keep.has(id)) els.delete(id);
    }

    /* ── the selection, as a box ── */
    function selBox() {
      let b = null;
      for (const e of selEntries()) b = union(b, geo.drawn(e.it, e.P));
      return b;
    }
    /** single: one leaf (its own turned box, handles in its own axes); otherwise the box round everything selected, square to the charm. */
    function selInfo() {
      const es = selEntries(); if (!es.length) return null;
      if (es.length === 1 && es[0].it.kind !== 'group') {
        const e = es[0], W = mul(e.P, geo.matrix(e.it)), rb = geo.box(e.it);
        if (!rb) return null;
        return { single: true, e, W, rb, corners: quadOf(rb, W) };
      }
      const b = selBox(); if (!b) return null;
      return { single: false, b, corners: [[b[0], b[1]], [b[2], b[1]], [b[2], b[3]], [b[0], b[3]]] };
    }
    const HANDLES = [['nw', 0, 0], ['n', 0.5, 0], ['ne', 1, 0], ['e', 1, 0.5], ['se', 1, 1], ['s', 0.5, 1], ['sw', 0, 1], ['w', 0, 0.5]];
    function outline(it, P, cls) {
      const W = mul(V(), mul(P, geo.matrix(it)));
      const leafPath = (lf, M) => `<path class="${cls}" d="${dOf(geo.raw(lf).subpaths)}" transform="matrix(${M.map(v => +v.toFixed(6)).join(' ')})" vector-effect="non-scaling-stroke"/>`;
      if (it.kind !== 'group') return leafPath(it, W);
      let out = '', n = 0;
      const rec = (g, M) => { for (const ch of g.children || []) { if (n > 400 || ch.hidden) continue; const M2 = mul(M, geo.matrix(ch)); if (ch.kind === 'group') rec(ch, M2); else { out += leafPath(ch, M2); n++; } } };
      rec(it, W); return out;
    }
    function renderUI() {
      let h = '';
      const busy = S.g && S.g.live;
      if (S.hover && !busy && !isSelected(S.hover)) { const e = entry(S.hover); if (e && !e.it.hidden) h += outline(e.it, e.P, 'u-hover'); }
      const info = selInfo();
      if (info && !(S.g && S.g.kind === 'marquee')) {
        const es = selEntries();
        if (es.length > 1 || es[0].it.kind === 'group') { let n = 0; for (const e of es) { if (n++ > 300) break; h += outline(e.it, e.P, 'u-sel'); } }
        const sc = info.corners.map(p => toStage(p[0], p[1]));
        const poly = sc.map(p => p[0].toFixed(1) + ',' + p[1].toFixed(1)).join(' ');
        h += `<polygon class="u-box--under" points="${poly}"/><polygon class="u-box${info.single ? '' : ' is-many'}" points="${poly}"/>`;
        if (!S.editing && S.tool === 'select' && !(S.g && S.g.kind === 'move' && S.g.live)) {
          const lerp = (a, b, t) => [a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t];
          const at = (fx, fy) => lerp(lerp(sc[0], sc[1], fx), lerp(sc[3], sc[2], fx), fy);
          const sideW = Math.hypot(sc[1][0] - sc[0][0], sc[1][1] - sc[0][1]), sideH = Math.hypot(sc[3][0] - sc[0][0], sc[3][1] - sc[0][1]);
          const edges = sideW >= 28 && sideH >= 28;   // (a box too small for eight keeps its four corners, as Illustrator's does)
          const top = at(0.5, 0), mid = at(0.5, 0.5);
          let ux = top[0] - mid[0], uy = top[1] - mid[1]; const ul = Math.hypot(ux, uy) || 1; ux /= ul; uy /= ul;
          if (ul < 1) { ux = 0; uy = -1; }
          const rp = [top[0] + ux * 22, top[1] + uy * 22];
          h += `<line class="u-stem" x1="${top[0].toFixed(1)}" y1="${top[1].toFixed(1)}" x2="${rp[0].toFixed(1)}" y2="${rp[1].toFixed(1)}"/>`;
          h += `<circle class="u-h" data-h="rot" cx="${rp[0].toFixed(1)}" cy="${rp[1].toFixed(1)}" r="5"/>`;
          for (const [name, fx, fy] of HANDLES) {
            if (!edges && name.length === 1) continue;
            const p = at(fx, fy);
            h += `<rect class="u-h" data-h="${name}" x="${(p[0] - 4.5).toFixed(1)}" y="${(p[1] - 4.5).toFixed(1)}" width="9" height="9"/>`;
          }
        }
      }
      const g = S.g;
      if (g && g.kind === 'marquee' && g.live) {
        const a = toStage(g.x0, g.y0), b = toStage(g.x, g.y);
        h += `<rect class="u-marq" x="${Math.min(a[0], b[0]).toFixed(1)}" y="${Math.min(a[1], b[1]).toFixed(1)}" width="${Math.abs(b[0] - a[0]).toFixed(1)}" height="${Math.abs(b[1] - a[1]).toFixed(1)}"/>`;
      }
      if (g && g.kind === 'draw' && g.live && g.draft) h += `<path class="u-draft" d="${dOf(g.draft)}" transform="matrix(${V().join(' ')})" vector-effect="non-scaling-stroke"/>`;
      if (g && g.guides) h += g.guides;
      if (g && g.readout) h += g.readout;
      { const xt = xTool(S.tool); if (xt && typeof xt.overlay === 'function') { try { h += xt.overlay(ectx) || ''; } catch (er) { xErr(xt.id + '.overlay', er); } } }
      ui.innerHTML = h;
    }

    /* ── the Layers panel: rows topmost first; rebuilt when the items change, the selection painted on by class ── */
    let layersFor = null, layersOpenKey = '';
    const rowName = it => (it.name != null && String(it.name).trim() ? String(it.name) : it.kind === 'text' ? (String(it.text || '').trim() || 'Text') : it.kind === 'group' ? 'Group' : ROLE_NAME[it.role] || 'Path');
    function renderLayers(force) {
      const its = items(), openKey = [...S.open].join('|');
      if (!force && layersFor === its && layersOpenKey === openKey) { paintLayerSel(); return; }
      layersFor = its; layersOpenKey = openKey;
      let h = '';
      const row = (it, depth) => {
        const isG = it.kind === 'group', opn = S.open.has(it.id);
        h += `<div class="ce-layer${it.hidden ? ' is-hidden' : ''}${it.locked ? ' is-locked' : ''}" data-id="${esc(it.id)}" role="option" aria-selected="false" tabindex="-1" style="--depth:${depth}">`
          + `<span class="ce-layer__grip" aria-hidden="true"><i></i><i></i><i></i></span>`
          + (isG ? `<button class="ce-layer__tw" type="button" data-lact="twist" aria-label="${opn ? 'Collapse' : 'Expand'}">${opn ? '▾' : '▸'}</button><span class="ce-layer__fold">${IC.folder}</span>`
            : `<i class="ce-sw ce-sw--${esc(it.role)}${it.kind === 'text' ? ' is-text' : ''}${it.kind === 'path' && it.role === 'art' && !paintFill(it) ? ' is-outline' : ''}" aria-hidden="true">${it.kind === 'text' ? 'T' : ''}</i>`)
          + `<span class="ce-layer__name" title="${esc(rowName(it))} · ${esc(ROLE_NAME[it.role] || '')}">${esc(rowName(it))}</span>`
          + (it.source === 'added' ? '<i class="ce-layer__new" title="Added here"></i>' : '')
          + `<button class="ce-layer__b${it.hidden ? '' : ' is-quiet'}" type="button" data-lact="eye" aria-label="${it.hidden ? 'Show' : 'Hide'}" title="${eyeSays(it)}">${it.hidden ? IC.eyeOff : IC.eyeOn}</button>`
          + `<button class="ce-layer__b${it.locked ? '' : ' is-quiet'}" type="button" data-lact="lock" aria-label="${it.locked ? 'Unlock' : 'Lock'}" title="${it.locked ? 'Unlock' : 'Lock'}">${it.locked ? IC.lockOn : IC.lockOff}</button>`
          + '</div>';
        if (isG && opn) for (let i = it.children.length - 1; i >= 0; i--) row(it.children[i], depth + 1);
      };
      for (let i = its.length - 1; i >= 0; i--) row(its[i], 0);
      layersEl.innerHTML = h || '<div class="ce-layers__empty">Nothing on this side yet.<br>Pick the Text tool and click on the charm.</div>';
      for (const r of layersEl.querySelectorAll('.ce-layer')) { const e = entry(r.dataset.id), g = e && greyOf(e.it), sw = g && r.querySelector('.ce-sw'); if (sw) sw.style.background = g; }   // (a traced image's greys)
      for (const f of ['front', 'back']) { const c = $(`[data-count="${f}"]`); if (c) c.textContent = S.faces[f] ? String(S.faces[f].items.length) : '–'; }
      paintLayerSel();
    }
    function paintLayerSel(reveal) {
      let first = null;
      for (const r of layersEl.querySelectorAll('.ce-layer')) {
        const on = S.sel.includes(r.dataset.id);
        r.classList.toggle('is-sel', on); r.setAttribute('aria-selected', String(on));
        if (on && !first) first = r;
      }
      if (reveal && first && first.scrollIntoView) { try { first.scrollIntoView({ block: 'nearest' }); } catch (e) { /* old engines */ } }
      const n = S.sel.length, es = selEntries();
      $('[data-act="forward"]').disabled = !n; $('[data-act="backward"]').disabled = !n; $('[data-act="delete"]').disabled = !n;
      $('[data-act="group"]').disabled = n < 2; $('[data-act="ungroup"]').disabled = !es.some(e => e.it.kind === 'group');
    }

    /* ── the property bar ── */
    const fmtMm = v => (Math.abs(v) < 0.0005 ? '0.00' : v.toFixed(2));
    const fmtWt = v => (Math.abs(v) < 0.0005 ? '0.00' : (v > 0 ? '+' : '') + (Math.abs(v * 100 - Math.round(v * 100)) < 1e-6 ? v.toFixed(2) : trim(v, 3)));   // (+0.03 · 0.00 · -0.02)
    const fmtLw = pt => { const mm = pt * MM; return mm < 0.0005 ? '0' : trim(mm, 3); };                                                                         // (0.25 pt reads 0.088)
    const numIn = v => parseFloat(String(v == null ? '' : v).replace(',', '.').replace(/−/g, '-'));
    function sizeRead() {
      const info = selInfo(); if (!info) return null;
      const f = frame();
      if (info.single) {
        const it = info.e.it, b = geo.drawn(it, info.e.P), rb = info.rb;
        return { x: (b[0] - f.x0) * MM, y: (b[1] - f.y0) * MM, w: Math.abs(num(it.sx, 1)) * (rb[2] - rb[0]) * MM, h: Math.abs(num(it.sy, 1)) * (rb[3] - rb[1]) * MM, r: wrap180(num(it.rot, 0)) };
      }
      const b = info.b, leaves = selLeaves(), r0 = leaves.length ? wrap180(num(leaves[0].rot, 0)) : 0;
      return { x: (b[0] - f.x0) * MM, y: (b[1] - f.y0) * MM, w: (b[2] - b[0]) * MM, h: (b[3] - b[1]) * MM, r: leaves.every(l => Math.abs(wrap180(num(l.rot, 0)) - r0) < 1e-6) ? r0 : null };
    }
    function syncProps() {
      const s = sizeRead();
      for (const k of ['x', 'y', 'w', 'h', 'r']) {
        const el = field(k); if (doc.activeElement === el && !(S.g && S.g.live)) continue;
        el.disabled = !s;
        el.value = !s ? '' : k === 'r' ? (s.r == null ? '' : trim(s.r, 1)) : fmtMm(s[k]);
      }
      const leaves = selLeaves().filter(l => !l.locked);
      const texts = leaves.filter(l => l.kind === 'text');
      const creating = S.tool === 'text' || S.tool === 'rect' || S.tool === 'ellipse' || S.tool === 'line';
      const showText = texts.length > 0 || (S.tool === 'text' && !S.sel.length);
      $$('[data-when="text"]').forEach(el => { el.hidden = !showText; });
      if (showText) {
        const fk = texts.length ? texts[0].fontKey : S.textFont, same = texts.every(t => t.fontKey === fk);
        const fo = fonts.find(f => f.key === fk);
        $('[data-fontname]').textContent = same ? (fo ? fo.name : (fk == null ? 'Font' : String(fk))) : 'Mixed';
        $$('[data-font]').forEach(b => b.classList.toggle('is-on', same && b.dataset.font === String(fk)));
        const pt = field('pt');
        if (doc.activeElement !== pt) { const sz = texts.length ? num(texts[0].sizePt, 10) : S.textSize; pt.value = texts.every(t => Math.abs(num(t.sizePt, 10) - sz) < 1e-6) ? trim(sz, 2) : ''; }
      }
      const showRole = leaves.length > 0 || (creating && !S.sel.length);
      $('[data-when="role"]').hidden = !showRole;
      if (showRole) {
        const role = leaves.length ? (leaves.every(l => l.role === leaves[0].role) ? leaves[0].role : 'mixed') : (S.tool === 'text' ? S.textRole : S.shapeRole);
        $('[data-rolesw]').className = 'ce-sw ce-sw--' + role;
        $('[data-rolename]').innerHTML = role === 'mixed' ? 'Mixed' : `<span class="ce-long">${ROLE_NAME[role]}</span><span class="ce-short">${ROLE_SHORT[role]}</span>`;
        $$('[data-role]').forEach(b => b.classList.toggle('is-on', b.dataset.role === role));
      }
      // the thickness of what is selected, each where it means something: a filled text or hatched part has a WEIGHT (its strokes thicker or thinner than drawn), a line a WIDTH
      const newText = S.tool === 'text' && !S.sel.length, lineText = S.textRole === 'line' || S.textRole === 'cut';
      const weighs = leaves.filter(weighable);
      const showWt = canWeigh() && (weighs.length > 0 || (newText && !lineText));
      $('[data-when="weight"]').hidden = !showWt;
      if (showWt) {
        const wt = field('wt'), w = weighs.length ? weightOf(weighs[0]) : S.textWeight, same = weighs.every(l => Math.abs(weightOf(l) - w) < 1e-9);
        if (doc.activeElement !== wt) wt.value = same ? fmtWt(w) : '';
        wt.placeholder = same ? '' : 'Mixed';
        $('[data-act="wt0"]').classList.toggle('is-on', !same || Math.abs(w) > 1e-9);
      }
      const strokes = leaves.filter(paintStroke);
      const showW = strokes.length > 0 || ((S.tool === 'rect' || S.tool === 'ellipse' || S.tool === 'line') && !S.sel.length) || (newText && lineText);
      $('[data-when="stroke"]').hidden = !showW;
      if (showW) {
        const lf = field('lw'), lw = strokes.length ? lwOf(strokes[0]) : S.lw, same = strokes.every(l => Math.abs(lwOf(l) - lw) < 1e-9);
        if (doc.activeElement !== lf) lf.value = same ? fmtLw(lw) : '';
        lf.placeholder = same ? '' : 'Mixed';
      }
      $$('[data-face]').forEach(b => b.classList.toggle('is-on', b.dataset.face === S.face));
      $('[data-act="link"]').classList.toggle('is-on', S.link); $('[data-act="link"]').setAttribute('aria-pressed', String(S.link));
      $$('[data-opt]').forEach(b => { const on = !!S[b.dataset.opt]; b.classList.toggle('is-on', on); b.setAttribute('aria-checked', String(on)); });
      $('[data-act="dock"]').classList.toggle('is-on', S.dock);
      // (a phone has no height to spare for a bar describing nothing: it folds away, as the studio's size box does with nothing selected)
      $('.ce-props').classList.toggle('is-empty', !s && !showText && !showRole && !showW && !showWt);
      if (!(S.g && S.g.live)) xBar();
      fitBar();
    }
    /** ONE ROW ON A DESK (27 Oct 2026): when what the bar now shows would wrap it (a long font name, the weight beside the size), the role says itself in one word. Measured only
     *  when the bar's contents change, never on every frame of a drag. */
    let barSig = '';
    function fitBar() {
      const bar = $('.ce-props'), vw = doc.documentElement.clientWidth || root.innerWidth || 0;
      const sig = [...bar.children].map(e => (e.hidden ? 0 : 1)).join('') + '|' + $('[data-fontname]').textContent + '|' + $('[data-rolename]').textContent + '|' + vw + '|' + (S.dock ? 1 : 0);
      if (sig === barSig) return; barSig = sig;
      bar.classList.remove('is-tight');
      if (vw <= 640) return;   // (a phone's bar wraps by design)
      const shown = [...bar.children].filter(e => !e.hidden && e.offsetHeight > 0);
      if (shown.length > 1 && shown.some(e => e.offsetTop > shown[0].offsetTop + 4)) bar.classList.add('is-tight');
    }
    function syncHist() { $('[data-act="undo"]').disabled = !S.hist.canUndo; $('[data-act="redo"]').disabled = !S.hist.canRedo; }
    function setTool(t) {
      if (!['select', 'marquee', 'text', 'rect', 'ellipse', 'line', 'hand'].includes(t) && !xTool(t)) return;
      if (S.editing) closeEditor();
      const was = S.tool; xCommit();
      S.tool = t;
      if (t === 'rect' || t === 'ellipse' || t === 'line') {
        S.shape = t;
        const b = $('[data-shapes]'); b.dataset.tool = t; b.innerHTML = IC[t];
      }
      xFlyButton(t);
      $$('.ce-tool').forEach(b => b.classList.toggle('is-on', b.dataset.tool === t || (b.hasAttribute('data-shapes') && (t === 'rect' || t === 'ellipse' || t === 'line'))));
      stage.dataset.tool = t;
      xArm(was, t);
      $('[data-hint]').textContent = HINTS[t] || (xTool(t) ? String(xTool(t).hint || xTool(t).title || '') : '');
      S.hover = null; renderUI(); syncProps();
    }
    function refresh(opts2) {
      ixFor = null; render(); renderUI(); renderLayers(); syncProps(); syncHist();
      if (opts2 && opts2.reveal) paintLayerSel(true);
      xChanged();
    }

    /* ── edits ── */
    /** One step of history and a new tree. */
    function commit(next, sel) {
      if (next === items()) { if (sel) setSel(sel); return; }
      pushUndo(); setItems(next); ixFor = null; if (sel) S.sel = topmost(sel, ix()); refresh({ reveal: !!sel });
    }
    function restore(s) {
      if (S.editing) closeEditor(true);
      S.faces[s.face].items = s.items;
      if (s.face !== S.face) { S.face = s.face; applyFaceView(); }
      ixFor = null; S.sel = (s.sel || []).filter(id => ix().has(id));
      refresh({ reveal: true });
    }
    function undo() {
      if (S.editing) closeEditor();
      xCommit();
      const top = S.hist.peekBack(); if (!top) return;
      restore(S.hist.undo(snapshot(top.face)));
    }
    function redo() {
      if (S.editing) closeEditor();
      xCommit();
      const top = S.hist.peekFwd(); if (!top) return;
      restore(S.hist.redo(snapshot(top.face)));
    }
    /** The selection moved by dx, dy pt as one undo step; arrows held or tapped in a run are ONE step (a 2 mm nudge is forty taps, and forty steps would push
     *  everything else out of the history). */
    function nudge(dx, dy) {
      const ids = S.sel.filter(id => { const e = entry(id); return e && !e.it.locked; }); if (!ids.length) return;
      const now = Date.now(), run = S.nudgeAt && now - S.nudgeAt < 1200 && S.nudgeSel === ids.join('|');
      if (!run) pushUndo();
      let next = normaliseAncestors(items(), ids, geo);
      const ixm = indexTree(next, geo), M = tr(dx, dy);
      next = replaceIn(next, new Map(ids.map(id => [id, pushDown(ixm.get(id).it, M, geo)])));
      setItems(next); S.nudgeAt = now; S.nudgeSel = ids.join('|');
      refresh();
    }
    function transformSel(M, opts2) {
      const ids = S.sel.slice(); if (!ids.length) return;
      let next = normaliseAncestors(items(), ids, geo);
      const ixm = indexTree(next, geo);
      next = replaceIn(next, new Map(ids.map(id => { let n = pushDown(ixm.get(id).it, M, geo); if (opts2 && opts2.fold) n = foldAll(n); return [id, n]; })));
      commit(next);
    }
    const foldAll = it => (it.kind === 'group' ? (() => { const ch = it.children.map(foldAll); return ch.every((c, i) => c === it.children[i]) ? it : Object.assign({}, it, { children: ch }); })() : foldTextScale(it, geo));
    /** The leaves under the selection (or the selection's leaves themselves) patched by fn, as one step. */
    function patchLeaves(fn) {
      const map = new Map();
      for (const e of selEntries()) {
        const rec = it => {
          if (it.kind === 'group') { const ch = it.children.map(rec); return ch.every((c, i) => c === it.children[i]) ? it : Object.assign({}, it, { children: ch }); }
          if (it.locked) return it;
          return fn(it) || it;
        };
        const n = rec(e.it); if (n !== e.it) map.set(e.it.id, n);
      }
      if (map.size) commit(replaceIn(items(), map));
    }
    function setRole(role) {
      if (!ROLES.includes(role)) return;
      if (!S.sel.length) { if (S.tool === 'text') S.textRole = role; else S.shapeRole = role; syncProps(); return; }
      patchLeaves(it => {
        const n = Object.assign({}, it, { role });
        if (it.kind === 'path') { if (role === 'hatch') { n.fill = true; n.stroke = false; } else if (role === 'line' || role === 'cut') { n.stroke = true; n.fill = false; } }
        return n;
      });
      if (S.tool === 'text') S.textRole = role;
    }
    /** The leaves under the selection patched by fn, from where a RUN began: arrow taps on a field held or tapped in a run are ONE step of history (as the nudges are), and each tap is
     *  made from the drawing as it was when the run began, so a path's weight is grown once by the run's whole amount, never tap upon tap. */
    function patchRun(key, run, fn) {
      const now = Date.now(), r = S.run, tag = key + '|' + S.face + '|' + S.sel.join('|');
      const live = !!(run && r && r.tag === tag && r.items === items() && now - r.at < 1500);
      const from = live ? r.base : items(), ixm = indexTree(from, geo), map = new Map();
      const rec = it => {
        if (it.kind === 'group') { const ch = (it.children || []).map(rec); return ch.every((c, i) => c === it.children[i]) ? it : Object.assign({}, it, { children: ch }); }
        if (it.locked) return it;
        return fn(it) || it;
      };
      for (const id of S.sel) { const e = ixm.get(id); if (!e) continue; const n = rec(e.it); if (n !== e.it) map.set(id, n); }
      const next = map.size ? replaceIn(from, map) : from;
      if (next === items()) { syncProps(); return; }
      if (!live) pushUndo();
      setItems(next); ixFor = null;
      S.run = run ? { tag, base: from, items: next, at: now } : null;
      refresh();
    }
    /** WEIGHT (27 Oct 2026): the selection's filled words and hatched parts this much thicker (+) or thinner (−) than drawn, mm; with nothing selected, the weight of the next words. */
    function setWeightMm(w, run) {
      if (!fin(w)) { syncProps(); return; }
      w = clamp(Math.round(w * 1000) / 1000, WT_MIN_MM, WT_MAX_MM);
      if (!S.sel.length) { S.textWeight = w; syncProps(); return; }
      patchRun('wt', run, it => (weighable(it) ? weighItem(it, w, geo) : null));
    }
    /** LINE WIDTH (27 Oct 2026): every line of the selection (a text engraved as a line too) this wide on the charm, mm; with nothing selected, the width of the next line drawn. */
    function setStrokeMm(mm, run) {
      if (!fin(mm)) { syncProps(); return; }
      const lw = r9(clamp(Math.round(mm * 1000) / 1000, LW_MIN_MM, LW_MAX_MM) * PT);
      if (!S.sel.length) { S.lw = lw; syncProps(); return; }
      patchRun('lw', run, it => (paintStroke(it) && Math.abs(lwOf(it) - lw) > 1e-9 ? Object.assign({}, it, { lwPt: lw }) : null));
    }
    /** Each line back to the width it came into the editor with (a line that came with none, or a new one: the laser's own). */
    function resetStroke() {
      if (!S.sel.length) { S.lw = LW_LASER_PT; syncProps(); return; }
      const was = indexTree(face().base, geo);
      patchRun('lw0', false, it => {
        if (!paintStroke(it)) return null;
        const b = was.get(it.id), lw = b && b.it.lwPt != null ? b.it.lwPt : undefined;
        if (lw === it.lwPt) return null;
        const n = Object.assign({}, it); if (lw === undefined) delete n.lwPt; else n.lwPt = lw;
        return n;
      });
    }
    function setFont(key) {
      S.textFont = key;
      if (!S.sel.length) { syncProps(); return; }
      patchLeaves(it => (it.kind === 'text' ? retext(it, { fontKey: key }, geo) : null));
    }
    function setTextSize(pt) {
      if (!(pt > 0)) return;
      pt = clamp(pt, 0.5, 400); S.textSize = pt;
      if (!S.sel.length) { syncProps(); return; }
      patchLeaves(it => (it.kind === 'text' ? retext(it, { sizePt: pt }, geo) : null));
    }
    function deleteSel() {
      const ids = S.sel.filter(id => { const e = entry(id); return e && !e.it.locked; }); if (!ids.length) return;
      commit(removeIds(items(), ids), []);
    }
    function selectAll() {
      setSel(items().filter(it => !it.hidden && !it.locked).map(it => it.id));
      if (S.tool !== 'select') setTool('select');
    }
    function copySel() { if (!S.sel.length) return false; S.clip = copyItems(items(), S.sel, geo); S.pasteN = 0; return true; }
    function cutSel() { if (copySel()) deleteSel(); }
    function paste(inPlace) {
      if (!S.clip || !S.clip.length) return;
      if (!inPlace) S.pasteN++;
      const d = inPlace ? 0 : PASTE_MM * PT * S.pasteN;
      const add = pasteItems(S.clip, d, d, geo);
      commit(items().concat(add), add.map(a => a.id));
    }
    function duplicate() {
      const ids = S.sel.slice(); if (!ids.length) return;
      let next = normaliseAncestors(items(), ids, geo); const made = [];
      const d = PASTE_MM * PT;
      for (const id of topmost(ids, indexTree(next, geo))) {
        const cur = indexTree(next, geo), e = cur.get(id); if (!e) continue;
        const copy = pasteItems([e.it], d, d, geo)[0];
        const pid = e.parent ? e.parent.id : null;
        const list = (pid == null ? next : cur.get(pid).it.children).slice();
        list.splice(list.findIndex(x => x.id === id) + 1, 0, copy); next = setListIn(next, pid, list); made.push(copy.id);
      }
      commit(next, made);
    }
    const setListIn = (its, pid, list) => (pid == null ? list : replaceIn(its, new Map([[pid, Object.assign({}, indexTree(its, geo).get(pid).it, { children: list })]])));
    function arrangeSel(how) { if (S.sel.length) commit(arrange(items(), S.sel, how, geo)); }
    function groupSel() {
      if (S.sel.length < 2) return;
      const r = groupItems(items(), S.sel, geo);
      if (r.id) { S.open.add(r.id); commit(r.items, [r.id]); }
    }
    function ungroupSel() {
      let next = items(); const sel = [];
      for (const e of selEntries()) { if (e.it.kind !== 'group') { sel.push(e.it.id); continue; } const r = ungroupItem(next, e.it.id, geo); next = r.items; sel.push(...r.ids); }
      if (next !== items()) commit(next, sel);
    }
    function toggleFlag(id, flag) {
      const e = entry(id); if (!e) return;
      const v = !e.it[flag];
      const set = it => { const n = Object.assign({}, it, { [flag]: v }); if (flag === 'locked') { delete n._autoLock; delete n._lockedIn; } return n; };
      commit(replaceIn(items(), new Map([[id, set(e.it)]])));
      if (v && (flag === 'hidden' || flag === 'locked')) setSel(S.sel.filter(s => s !== id && !(entry(s) && entry(s).anc.includes(id))));
    }
    function setAll(flag, v) {
      const rec = list => {
        let changed = false;
        const out = list.map(it => {
          let n = it;
          if (!!it[flag] !== v) { n = Object.assign({}, it, { [flag]: v }); if (flag === 'locked') { delete n._autoLock; delete n._lockedIn; } }
          if (it.kind === 'group') { const ch = rec(it.children || []); if (ch !== it.children) n = Object.assign({}, n, { children: ch }); }
          if (n !== it) changed = true;
          return n;
        });
        return changed ? out : list;
      };
      commit(rec(items()));
    }
    function rename(id, name) {
      const e = entry(id); if (!e) return;
      const v = String(name || '').trim(); if (v === String(e.it.name || '')) return;
      commit(replaceIn(items(), new Map([[id, Object.assign({}, e.it, { name: v || undefined })]])));
    }
    /** Typed into the bar: X / Y move the selection's box there, W / H size it (the link keeps the proportions), the angle turns it. */
    function commitField(k, raw) {
      const v = parseFloat(String(raw).replace(',', '.'));
      const s = sizeRead(); if (!s || !fin(v)) { syncProps(); return; }
      const info = selInfo(), b = info.single ? geo.drawn(info.e.it, info.e.P) : info.b;
      if (k === 'x' || k === 'y') {
        const d = (v - s[k]) * PT; if (Math.abs(d) < 1e-9) return syncProps();
        return transformSel(k === 'x' ? tr(d, 0) : tr(0, d));
      }
      if (k === 'r') {
        const delta = s.r == null ? v : v - s.r; if (Math.abs(delta) < 1e-9) return syncProps();
        const c = info.single ? ap(info.W, ...centreOf(info.rb)) : centreOf(info.b);
        return transformSel(about(rotM(delta), c[0], c[1]), { fold: true });
      }
      if ((k === 'w' || k === 'h') && v > 0) {
        const cur = s[k]; if (!(cur > 1e-9) || Math.abs(v - cur) < 1e-9) return syncProps();
        const f = v / cur, kx = k === 'w' ? f : (S.link ? f : 1), ky = k === 'h' ? f : (S.link ? f : 1);
        if (info.single) {
          // in the item's own axes (W and H are its own width and height, whatever its turn); the drawn box's top-left stays where it was
          const id = info.e.it.id, next = normaliseAncestors(items(), [id], geo), en = indexTree(next, geo).get(id);
          const W = mul(en.P, geo.matrix(en.it)), rb = geo.box(en.it);
          let n = pushDown(en.it, mul(W, mul(about(scM(kx, ky), rb[0], rb[1]), inv(W) || I)), geo);
          n = foldTextScale(n, geo);
          const nb = geo.drawn(n, en.P); if (nb) n = pushDown(n, tr(b[0] - nb[0], b[1] - nb[1]), geo);
          return commit(replaceIn(next, new Map([[id, n]])));
        }
        return transformSel(about(scM(kx, ky), b[0], b[1]), { fold: true });
      }
      syncProps();
    }

    /* ── the text editor: one textarea, written to the model on every keystroke (J 13030-13101); placed under the words so the outlines stay in sight ── */
    function openEditor(id, isNew, before) {
      if (S.editing) closeEditor();
      const e = entry(id); if (!e || e.it.kind !== 'text') return;
      const ta = doc.createElement('textarea');
      ta.className = 'ce-editor'; ta.rows = 1; ta.value = e.it.text || ''; ta.spellcheck = false; ta.maxLength = 400;
      ta.placeholder = 'Type the words…'; ta.setAttribute('aria-label', 'Words');
      stage.appendChild(ta);
      S.editing = { id, ta, isNew: !!isNew, before: before || snapshot(), text0: e.it.text || '' };
      const grow = () => { ta.style.height = 'auto'; ta.style.height = Math.min(160, ta.scrollHeight + 2) + 'px'; };
      ta.addEventListener('input', () => { setText(id, ta.value); grow(); placeEditor(); });
      grow(); placeEditor(); renderUI();
      requestAnimationFrame(() => { try { ta.focus(); if (!isNew) ta.select(); } catch (er) { /* gone already */ } });
    }
    function setText(id, text) {
      const e = entry(id); if (!e) return;
      const auto = e.it._autoName || e.it.name == null || e.it.name === e.it.text;
      const n = retext(e.it, { text }, geo);
      if (auto) { n.name = text.split('\n')[0].slice(0, 40) || 'Text'; n._autoName = true; }
      setItems(replaceIn(items(), new Map([[id, n]])));
      ixFor = null; render(); renderUI(); syncProps();
      const row = layersEl.querySelector(`.ce-layer[data-id="${cssEsc(id)}"] .ce-layer__name`); if (row) row.textContent = rowName(n);
    }
    function placeEditor() {
      const ed = S.editing; if (!ed) return;
      const e = entry(ed.id); if (!e) return;
      const b = geo.drawn(e.it, e.P), s = sb(), ta = ed.ta;
      const w = ta.offsetWidth || 220, h = ta.offsetHeight || 34;
      let left, top;
      if (b) { const p0 = toStage(b[0], b[1]), p1 = toStage(b[2], b[3]); left = p0[0]; top = p1[1] + 10; if (top + h > s.h - 6) top = p0[1] - 10 - h; }
      else { const m = mul(e.P, geo.matrix(e.it)), p = toStage(...ap(m, 0, 0)); left = p[0]; top = p[1] + 10; }
      ta.style.left = clamp(left, 6, Math.max(6, s.w - w - 6)) + 'px'; ta.style.top = clamp(top, 6, Math.max(6, s.h - h - 6)) + 'px';
    }
    function closeEditor(silent) {
      const ed = S.editing; if (!ed) return;
      S.editing = null;
      if (ed.ta.parentNode) ed.ta.parentNode.removeChild(ed.ta);
      const e = entry(ed.id);
      if (e && !String(e.it.text || '').trim()) {
        // an empty text is not a text: a new one leaves no trace, an old one emptied is a deletion
        if (ed.isNew) { setItems(ed.before.face === S.face ? ed.before.items : removeIds(items(), [ed.id])); S.sel = S.sel.filter(id => id !== ed.id); }
        else { S.hist.push(ed.before); setItems(removeIds(items(), [ed.id])); S.sel = S.sel.filter(id => id !== ed.id); }
      } else if (e && !silent && (ed.isNew || e.it.text !== ed.text0)) S.hist.push(ed.before);
      ixFor = null; refresh();
      try { win.focus({ preventScroll: true }); } catch (er) { /* focus is a courtesy */ }
    }
    function createText(x, y) {
      const before = snapshot();
      const it = { id: newId(), kind: 'text', role: S.textRole, source: 'added', name: 'Text', _autoName: true, hidden: false, locked: false, text: '', fontKey: S.textFont, sizePt: S.textSize, align: 'left', tx: r9(x), ty: r9(y), sx: 1, sy: 1, rot: 0 };
      if (S.textWeight && canWeigh()) it.weightMm = S.textWeight;
      if ((S.textRole === 'line' || S.textRole === 'cut') && Math.abs(S.lw - LW_LASER_PT) > 1e-9) it.lwPt = S.lw;
      setItems(items().concat(it)); S.sel = [it.id]; ixFor = null;
      refresh();
      openEditor(it.id, true, before);
    }

    /* ── smart guides (the studio's align engine, J 16670-16957: the charm's edges and middle and the nearest sixty other things, agreement within 7 screen px, Alt
       pauses it, every agreeing line drawn and not only the winner) ── */
    function alignTargets(bb0) {
      const f = frame(), out = { x: [], y: [] };
      out.x.push({ v: f.x0, kind: 'frame' }, { v: f.x0 + f.wPt / 2, kind: 'mid' }, { v: f.x0 + f.wPt, kind: 'frame' });
      out.y.push({ v: f.y0, kind: 'frame' }, { v: f.y0 + f.hPt / 2, kind: 'mid' }, { v: f.y0 + f.hPt, kind: 'frame' });
      const ixm = ix(), skip = new Set();
      for (const id of S.sel) { const e = ixm.get(id); if (!e) continue; skip.add(id); e.anc.forEach(a => skip.add(a)); walk([e.it], it => skip.add(it.id)); }
      const cand = [], c0 = centreOf(bb0);
      const consider = (it, P) => { if (it.hidden || skip.has(it.id)) return; const b = geo.drawn(it, P); if (!b) return; const c = centreOf(b); cand.push({ b, d: Math.abs(c[0] - c0[0]) + Math.abs(c[1] - c0[1]) }); };
      for (const it of items()) consider(it, I);
      for (const id of S.sel) { const e = ixm.get(id); if (e && e.parent) for (const sib of e.parent.children) consider(sib, e.P); }
      cand.sort((a, b) => a.d - b.d);
      for (const { b } of cand.slice(0, 60)) {
        out.x.push({ v: b[0], kind: 'edge', b }, { v: (b[0] + b[2]) / 2, kind: 'edge', b }, { v: b[2], kind: 'edge', b });
        out.y.push({ v: b[1], kind: 'edge', b }, { v: (b[1] + b[3]) / 2, kind: 'edge', b }, { v: b[3], kind: 'edge', b });
      }
      return out;
    }
    function gridStepPt() { return (S.k * PT * 0.1 >= 6 ? 0.1 : 1) * PT; }
    function solve(bb, T, tol, allowX, allowY) {
      const res = { dx: 0, dy: 0, gx: [], gy: [] };
      const axis = (lo, hi, list, allowed, gridOrigin) => {
        const mine = [{ v: lo, at: 'lo' }, { v: (lo + hi) / 2, at: 'mid' }, { v: hi, at: 'hi' }].filter(m => !allowed || allowed.includes(m.at));
        const pool = list.slice();
        if (S.snapGrid) { const st = gridStepPt(); for (const m of mine) pool.push({ v: gridOrigin + Math.round((m.v - gridOrigin) / st) * st, kind: 'grid' }); }
        let best = null;
        for (const m of mine) for (const t of pool) {
          const ad = Math.abs(t.v - m.v); if (ad > tol) continue;
          const centre = t.kind === 'mid' || m.at === 'mid';
          if (!best || ad < best.ad - 1e-9 || (Math.abs(ad - best.ad) < tol * 0.06 && centre && !best.centre)) best = { ad, centre, d: t.v - m.v };
        }
        if (!best) return null;
        const hits = []; for (const m of mine) for (const t of pool) if (Math.abs((t.v - m.v) - best.d) < 1e-6) hits.push(t);
        return { d: best.d, hits };
      };
      const f = frame();
      const bx = axis(bb[0], bb[2], S.smart ? T.x : [], allowX, f.x0), by = axis(bb[1], bb[3], S.smart ? T.y : [], allowY, f.y0);
      if (bx) { res.dx = bx.d; res.gx = bx.hits; } if (by) { res.dy = by.d; res.gy = by.hits; }
      return res;
    }
    function guidesSvg(bb, hits) {
      let h = ''; const seen = new Set();
      const line = (vert, at, from, to, kind) => {
        const key = (vert ? 'v' : 'h') + at.toFixed(4) + kind; if (seen.has(key)) return; seen.add(key);
        const a = vert ? toStage(at, from) : toStage(from, at), b = vert ? toStage(at, to) : toStage(to, at);
        h += `<line class="u-gd${kind === 'mid' ? ' u-gd--mid' : kind === 'frame' ? ' u-gd--frame' : ''}" x1="${a[0].toFixed(1)}" y1="${a[1].toFixed(1)}" x2="${b[0].toFixed(1)}" y2="${b[1].toFixed(1)}"/>`;
      };
      const f = frame(), pad = 1.5 * PT;
      for (const g of hits.gx || []) { if (g.kind === 'grid') continue; const o = g.b; line(true, g.v, o ? Math.min(bb[1], o[1]) - pad : f.y0, o ? Math.max(bb[3], o[3]) + pad : f.y0 + f.hPt, g.kind); }
      for (const g of hits.gy || []) { if (g.kind === 'grid') continue; const o = g.b; line(false, g.v, o ? Math.min(bb[0], o[0]) - pad : f.x0, o ? Math.max(bb[2], o[2]) + pad : f.x0 + f.wPt, g.kind); }
      return h;
    }
    function readoutSvg(bb, text) {
      if (!bb) return '';
      const a = toStage(bb[0], bb[1]), b = toStage(bb[2], bb[3]), s = sb();
      const w = text.length * 5.9 + 12, x = clamp((a[0] + b[0]) / 2 - w / 2, 4, s.w - w - 4);
      let y = b[1] + 12; if (y + 18 > s.h) y = a[1] - 30;
      return `<g class="u-dim"><rect x="${x.toFixed(1)}" y="${y.toFixed(1)}" width="${w.toFixed(1)}" height="16"/><text x="${(x + 6).toFixed(1)}" y="${(y + 11.5).toFixed(1)}">${esc(text)}</text></g>`;
    }

    /* ── gestures on the room ── */
    const PTRS = new Map();
    function hitAt(x, y, opts2) { return hitTest(items(), x, y, 4 / S.k, geo, opts2 || {}); }
    function begin(g) {
      // the first real movement of a gesture: one step of history, the groups above the selection flattened, the starting tree kept to transform FROM every frame
      pushUndo();
      setItems(normaliseAncestors(items(), S.sel, geo)); ixFor = null;
      g.items0 = items(); g.ix0 = ix();
      g.base = new Map(S.sel.map(id => [id, g.ix0.get(id).it]));
      g.live = true;
    }
    function applyM(g, M, foldText) {
      const map = new Map();
      for (const [id, it] of g.base) { let n = pushDown(it, M, geo); if (foldText) n = foldAll(n); map.set(id, n); }
      setItems(replaceIn(g.items0, map)); ixFor = null; render(); syncProps();
    }
    function onDown(e) {
      if (S.ask) return;
      closeMenus(); hideTip();
      if (e.target.closest && e.target.closest('.ce-editor')) return;
      if (S.editing) closeEditor();
      if (e.pointerType === 'touch') {
        PTRS.set(e.pointerId, { x: e.clientX, y: e.clientY });
        if (PTRS.size === 2) { abandon(); const [a, b] = [...PTRS.values()]; S.g = { kind: 'pinch', d0: Math.hypot(a.x - b.x, a.y - b.y), k0: S.k, cx: (a.x + b.x) / 2, cy: (a.y + b.y) / 2, ox0: S.ox, oy0: S.oy }; return; }
        if (PTRS.size > 2) return;
        clearTimeout(S.lp); const lx = e.clientX, ly = e.clientY;
        S.lp = setTimeout(() => { if (S.g && S.g.live) return; abandon(); openCtx(lx, ly); }, 520);
      }
      if (e.button === 2) return;
      try { stage.setPointerCapture(e.pointerId); } catch (er) { /* fine without */ }
      const [x, y] = fromClient(e.clientX, e.clientY);
      if (e.button === 1 || S.space || S.tool === 'hand') { e.preventDefault(); S.g = { kind: 'pan', cx0: e.clientX, cy0: e.clientY, ox0: S.ox, oy0: S.oy, live: true }; stage.classList.add('is-panning'); return; }
      { const xt = xTool(S.tool); if (xt) { e.preventDefault(); S.g = { kind: 'tool', t: xt, live: true, cx0: e.clientX, cy0: e.clientY }; xCall(xt, 'onDown', e); return; } }
      const h = e.target.closest && e.target.closest('[data-h]');
      // (a handle that is pressed and never dragged was a click on whatever is under it: on a small square the handles cover the neighbours' edges)
      if (S.tool === 'select' && h && S.sel.length) { e.preventDefault(); S.g = { kind: h.dataset.h === 'rot' ? 'rotate' : 'resize', h: h.dataset.h, x0: x, y0: y, cx0: e.clientX, cy0: e.clientY, live: false, click: { additive: e.shiftKey || e.metaKey || e.ctrlKey, deep: e.altKey } }; return; }
      if (S.tool === 'select') {
        const additive = e.shiftKey || e.metaKey || e.ctrlKey;
        const hit = hitAt(x, y, { deep: e.altKey });
        if (!hit) { S.g = { kind: 'marquee', x0: x, y0: y, x, y, cx0: e.clientX, cy0: e.clientY, add: additive, base: additive ? S.sel.slice() : [], live: false }; if (!additive && S.sel.length) setSel([]); return; }
        if (additive) { setSel(S.sel.includes(hit.id) ? S.sel.filter(s => s !== hit.id) : S.sel.concat(hit.id)); return; }
        if (!isSelected(hit.id) || (e.altKey && !S.sel.includes(hit.id))) setSel([hit.id]);
        S.g = { kind: 'move', x0: x, y0: y, cx0: e.clientX, cy0: e.clientY, live: false };
        return;
      }
      if (S.tool === 'marquee') { S.g = { kind: 'marquee', x0: x, y0: y, x, y, cx0: e.clientX, cy0: e.clientY, add: e.shiftKey, base: e.shiftKey ? S.sel.slice() : [], live: false }; return; }
      if (S.tool === 'text') {
        e.preventDefault();
        const t = hitAt(x, y, { deep: true, filter: it => it.kind === 'text' });
        if (t) { setSel([t.id]); openEditor(t.id); } else createText(x, y);
        return;
      }
      if (S.tool === 'rect' || S.tool === 'ellipse' || S.tool === 'line') { S.g = { kind: 'draw', tool: S.tool, x0: x, y0: y, x, y, cx0: e.clientX, cy0: e.clientY, live: false }; }
    }
    function abandon() {
      const g = S.g; if (!g) return;
      if (g.kind === 'tool') { S.g = null; xCommit(); renderUI(); return; }
      if (g.live && g.items0 && g.kind !== 'pan' && g.kind !== 'pinch') { const s = S.hist.back.pop(); setItems(s && s.face === S.face ? s.items : g.items0); syncHist(); ixFor = null; render(); renderLayers(); syncProps(); }
      S.g = null; stage.classList.remove('is-panning'); renderUI();
    }
    function onMove(e) {
      cursor = { x: e.clientX, y: e.clientY }; queueRulers();
      if (e.pointerType === 'touch' && PTRS.has(e.pointerId)) {
        PTRS.set(e.pointerId, { x: e.clientX, y: e.clientY });
        if (S.g && S.g.kind === 'pinch' && PTRS.size >= 2) {
          const [a, b] = [...PTRS.values()], g = S.g, d = Math.hypot(a.x - b.x, a.y - b.y), cx = (a.x + b.x) / 2, cy = (a.y + b.y) / 2;
          S.ox = g.ox0; S.oy = g.oy0; S.k = g.k0;
          const s = sb(); zoomAtStage(g.k0 * d / Math.max(1, g.d0), g.cx - s.l, g.cy - s.t);
          S.ox += cx - g.cx; S.oy += cy - g.cy; clampPan(); applyView();
          return;
        }
      }
      const g = S.g;
      if (g ? g.kind === 'tool' : xTool(S.tool)) { if (!S.space) xCall(g ? g.t : xTool(S.tool), 'onMove', e); return; }
      if (!g) { hoverAt(e); return; }
      if (Math.hypot(e.clientX - (g.cx0 || e.clientX), e.clientY - (g.cy0 || e.clientY)) > 6) clearTimeout(S.lp);
      if (g.kind === 'pan') { S.ox = g.ox0 + e.clientX - g.cx0; S.oy = g.oy0 + e.clientY - g.cy0; S.fitted = false; clampPan(); applyView(); return; }
      const [x, y] = fromClient(e.clientX, e.clientY);
      const far = Math.hypot(e.clientX - g.cx0, e.clientY - g.cy0) > 3;
      if (g.kind === 'marquee') {
        g.x = x; g.y = y; if (!g.live && !far) return; g.live = true;
        const hit = touching(items(), [g.x0, g.y0, x, y], geo).map(it => it.id);
        const ids = g.add ? g.base.concat(hit.filter(id => !g.base.includes(id))) : hit;
        S.sel = topmost(ids, ix()); renderUI(); paintLayerSel(); syncProps(); return;
      }
      if (g.kind === 'draw') {
        if (!g.live && !far) return; g.live = true;
        let x0 = g.x0, y0 = g.y0, x1 = x, y1 = y;
        if (g.tool === 'line') { if (e.shiftKey) { const a = Math.round(Math.atan2(y1 - y0, x1 - x0) / (Math.PI / 4)) * (Math.PI / 4), L = Math.hypot(x1 - x0, y1 - y0); x1 = x0 + Math.cos(a) * L; y1 = y0 + Math.sin(a) * L; } g.draft = lineSP(x0, y0, x1, y1); }
        else {
          let w = x1 - x0, h = y1 - y0;
          if (e.shiftKey) { const s = Math.max(Math.abs(w), Math.abs(h)); w = Math.sign(w || 1) * s; h = Math.sign(h || 1) * s; }
          if (e.altKey) { x0 = g.x0 - w; y0 = g.y0 - h; x1 = g.x0 + w; y1 = g.y0 + h; } else { x1 = x0 + w; y1 = y0 + h; }
          const a = [Math.min(x0, x1), Math.min(y0, y1), Math.max(x0, x1), Math.max(y0, y1)];
          g.draft = g.tool === 'rect' ? rectSP(a[0], a[1], a[2], a[3]) : ellipseSP((a[0] + a[2]) / 2, (a[1] + a[3]) / 2, (a[2] - a[0]) / 2, (a[3] - a[1]) / 2);
        }
        renderUI(); return;
      }
      if (!g.live) { if (!far) return; begin(g); g.info0 = selInfo(); g.T = g.kind === 'move' && g.info0 ? alignTargets(g.info0.single ? geo.drawn(g.info0.e.it, g.info0.e.P) : g.info0.b) : null; }
      const info = g.info0; if (!info) return;
      const tol = 7 / S.k, snapOn = !e.altKey;
      const f = frame();
      if (g.kind === 'move') {
        let dx = x - g.x0, dy = y - g.y0;
        const bb0 = info.single ? geo.drawn(info.e.it, info.e.P) : info.b;
        let hits = { gx: [], gy: [] };
        if (snapOn && (S.smart || S.snapGrid)) { hits = solve([bb0[0] + dx, bb0[1] + dy, bb0[2] + dx, bb0[3] + dy], g.T, tol); dx += hits.dx; dy += hits.dy; }
        applyM(g, tr(dx, dy));
        const bb = [bb0[0] + dx, bb0[1] + dy, bb0[2] + dx, bb0[3] + dy];
        g.guides = guidesSvg(bb, hits);
        g.readout = readoutSvg(bb, `X ${fmtMm((bb[0] - f.x0) * MM)}  Y ${fmtMm((bb[1] - f.y0) * MM)} mm`);
      } else if (g.kind === 'resize') {
        const hx = /w/.test(g.h) ? -1 : /e/.test(g.h) ? 1 : 0, hy = /n/.test(g.h) ? -1 : /s/.test(g.h) ? 1 : 0;
        const centred = e.altKey;
        // one unturned, unmirrored item is a square box like a selection of several: the same arithmetic, and its edges snap
        const square = info.single && Math.abs(info.W[1]) < 1e-12 && Math.abs(info.W[2]) < 1e-12 && info.W[0] > 0 && info.W[3] > 0;
        if (info.single && !square) {
          // in the item's own axes: the opposite handle stays put (Alt: the centre does)
          const rb = info.rb, Wi = inv(info.W) || I, q = ap(Wi, x, y), cx = (rb[0] + rb[2]) / 2, cy = (rb[1] + rb[3]) / 2;
          const ax = centred ? cx : hx < 0 ? rb[2] : rb[0], ay = centred ? cy : hy < 0 ? rb[3] : rb[1];
          const ex = hx < 0 ? rb[0] : rb[2], ey = hy < 0 ? rb[1] : rb[3];
          let kx = hx ? (q[0] - ax) / ((ex - ax) || 1e-9) : 1, ky = hy ? (q[1] - ay) / ((ey - ay) || 1e-9) : 1;
          if (e.shiftKey) { const k = !hx ? ky : !hy ? kx : (Math.abs(kx - 1) > Math.abs(ky - 1) ? kx : ky); kx = k; ky = k; }
          kx = Math.sign(kx || 1) * Math.max(0.01, Math.abs(kx)); ky = Math.sign(ky || 1) * Math.max(0.01, Math.abs(ky));
          applyM(g, mul(info.W, mul(about(scM(kx, ky), ax, ay), Wi)));
        } else {
          const b = info.single ? polyBox(info.corners) : info.b, cx = (b[0] + b[2]) / 2, cy = (b[1] + b[3]) / 2;
          const ax = centred ? cx : hx < 0 ? b[2] : b[0], ay = centred ? cy : hy < 0 ? b[3] : b[1];
          let ex = hx < 0 ? b[0] : b[2], ey = hy < 0 ? b[1] : b[3];
          let px = x, py = y;
          if (snapOn && !centred && !e.shiftKey && (S.smart || S.snapGrid)) {
            g.T = g.T || alignTargets(b);
            const hits = solve([hx ? px : b[0], hy ? py : b[1], hx ? px : b[2], hy ? py : b[3]], g.T, tol, hx ? ['lo'] : [], hy ? ['lo'] : []);
            px += hits.dx; py += hits.dy; g.guides = guidesSvg(b, hits);
          } else g.guides = '';
          let kx = hx ? (px - ax) / ((ex - ax) || 1e-9) : 1, ky = hy ? (py - ay) / ((ey - ay) || 1e-9) : 1;
          if (e.shiftKey) { const k = !hx ? ky : !hy ? kx : (Math.abs(kx - 1) > Math.abs(ky - 1) ? kx : ky); kx = k; ky = k; }
          kx = Math.sign(kx || 1) * Math.max(0.01, Math.abs(kx)); ky = Math.sign(ky || 1) * Math.max(0.01, Math.abs(ky));
          applyM(g, about(scM(kx, ky), ax, ay));
        }
        const nb = selBox(); g.readout = nb ? readoutSvg(nb, `${fmtMm((nb[2] - nb[0]) * MM)} × ${fmtMm((nb[3] - nb[1]) * MM)} mm`) : '';
      } else if (g.kind === 'rotate') {
        const c = info.single ? ap(info.W, ...centreOf(info.rb)) : centreOf(info.b);
        let d = (Math.atan2(y - c[1], x - c[0]) - Math.atan2(g.y0 - c[1], g.x0 - c[0])) * 180 / Math.PI;
        const r0 = info.single ? num(info.e.it.rot, 0) : 0;
        if (e.shiftKey) d = Math.round((r0 + d) / 15) * 15 - r0;
        else if (snapOn) { const a = r0 + d, n = Math.round(a / 90) * 90; if (Math.abs(a - n) < 1.5) d = n - r0; }
        applyM(g, about(rotM(d), c[0], c[1]));
        g.readout = readoutSvg(selBox(), `${trim(wrap180(r0 + d), 1)}°`);
      }
      renderUI();
    }
    function onUp(e) {
      if (e.pointerType === 'touch') { PTRS.delete(e.pointerId); clearTimeout(S.lp); if (S.g && S.g.kind === 'pinch') { if (PTRS.size < 2) S.g = null; return; } }
      try { stage.releasePointerCapture(e.pointerId); } catch (er) { /* not captured */ }
      const g = S.g; if (!g) return;
      S.g = null; stage.classList.remove('is-panning');
      if (g.kind === 'tool') { xCall(g.t, 'onUp', e); xCommit(); renderUI(); return; }
      if (g.kind === 'draw' && g.live && g.draft) {
        const b = bboxOf(g.draft), small = !b || Math.max(b[2] - b[0], b[3] - b[1]) * S.k < 3;
        if (!small) {
          const it = { id: newId(), kind: 'path', role: S.shapeRole, source: 'added', name: { rect: 'Rectangle', ellipse: 'Ellipse', line: 'Line' }[g.tool], hidden: false, locked: false,
            subpaths: g.draft.map(sub => sub.map(op => op.map(v => (Array.isArray(v) ? [r9(v[0]), r9(v[1])] : v)))), stroke: S.shapeRole !== 'hatch', fill: S.shapeRole === 'hatch', lwPt: S.lw, tx: 0, ty: 0, sx: 1, sy: 1, rot: 0 };
          commit(items().concat(it), [it.id]); setTool('select');
        }
        renderUI(); return;
      }
      if (g.kind === 'marquee' && !g.live) { renderUI(); return; }
      if (!g.live && g.click) {
        const hit = hitAt(g.x0, g.y0, { deep: g.click.deep });
        if (hit && g.click.additive) setSel(S.sel.includes(hit.id) ? S.sel.filter(s => s !== hit.id) : S.sel.concat(hit.id));
        else if (hit && !isSelected(hit.id)) setSel([hit.id]);
        else renderUI();
        return;
      }
      if (g.live && (g.kind === 'resize' || g.kind === 'rotate')) {
        const map = new Map(); for (const id of g.base.keys()) { const en = ix().get(id); if (en) { const n = foldAll(en.it); if (n !== en.it) map.set(id, n); } }
        if (map.size) { setItems(replaceIn(items(), map)); ixFor = null; }
      }
      refresh();
    }
    let hoverRaf = 0, hoverEv = null;
    function hoverAt(e) {
      hoverEv = e; if (hoverRaf) return;
      hoverRaf = requestAnimationFrame(() => {
        hoverRaf = 0; const ev = hoverEv; if (!ev || S.g) return;
        let id = null;
        if ((S.tool === 'select' || S.tool === 'text') && !S.space && !(ev.target.closest && ev.target.closest('[data-h]'))) {
          const [x, y] = fromClient(ev.clientX, ev.clientY);
          const h = hitAt(x, y, S.tool === 'text' ? { deep: true, filter: it => it.kind === 'text' } : { deep: ev.altKey });
          id = h ? h.id : null;
        }
        if (id !== S.hover) { S.hover = id; renderUI(); }
      });
    }
    function onWheel(e) {
      e.preventDefault();
      if (S.ask) return;
      // (the studio, J 17201: a bare wheel zooms about the cursor, Shift pans across, ⌘ pans down; a trackpad's two-finger scroll pans, its pinch arrives as Ctrl and zooms)
      if (e.ctrlKey) { zoomAtClient(e.clientX, e.clientY, Math.pow(1.01, -clamp(e.deltaY, -60, 60))); return; }
      const lineMode = e.deltaMode === 1 ? 16 : e.deltaMode === 2 ? 400 : 1;
      const dx = e.deltaX * lineMode, dy = e.deltaY * lineMode;
      S.fitted = false;
      if (e.shiftKey) { S.ox -= dy || dx; clampPan(); applyView(); return; }
      if (e.metaKey) { S.oy -= dy; clampPan(); applyView(); return; }
      const wheelLike = e.deltaMode !== 0 || (Math.abs(e.deltaX) < 0.01 && fin(e.wheelDeltaY) && e.wheelDeltaY !== 0 && Math.abs(e.wheelDeltaY) % 120 === 0);
      if (wheelLike) { zoomAtClient(e.clientX, e.clientY, Math.pow(1.0018, -clamp(dy, -400, 400))); return; }
      S.ox -= dx; S.oy -= dy; clampPan(); applyView();
    }
    function onDbl(e) {
      if (S.tool !== 'select') return;
      const [x, y] = fromClient(e.clientX, e.clientY);
      const t = hitAt(x, y, { deep: true });
      if (!t) return;
      if (t.kind === 'text') { setSel([t.id]); openEditor(t.id); }
      else if (!S.sel.includes(t.id)) setSel([t.id]);
    }

    /* ── menus: the studio's wireDrops (J 18674-18857): placed by script, a rail flyout opens sideways, four ways in for a rail tool (the corner, a press and hold,
       a right-click, a second click on the tool in hand) ── */
    function closeMenus() {
      $$('.ce-menu').forEach(m => { if (!m.hidden) { m.hidden = true; m.classList.remove('is-in', 'is-in--right'); } });
      $$('.ce-drop__b').forEach(b => b.setAttribute('aria-expanded', 'false'));
    }
    function placeMenu(menu, r, rail) {
      const vw = doc.documentElement.clientWidth || root.innerWidth, vh = doc.documentElement.clientHeight || root.innerHeight;
      menu.hidden = false; menu.style.visibility = 'hidden'; menu.style.left = '0px'; menu.style.top = '0px'; menu.style.maxHeight = (vh - 16) + 'px';
      const mr = menu.getBoundingClientRect();
      let left = rail ? r.right + 6 : (r.left > vw * 0.62 ? r.right - mr.width : r.left), top = rail ? r.top - 6 : r.bottom + 5;
      if (left + mr.width > vw - 8) left = rail ? Math.max(8, r.left - 6 - mr.width) : Math.max(8, vw - 8 - mr.width);
      if (left < 8) left = 8;
      // no room under the button (the zoom in the foot): it opens upward from it, as a menu at the bottom of a window does
      if (!rail && top + mr.height > vh - 8 && r.top - 5 - mr.height >= 8) top = r.top - 5 - mr.height;
      if (top + mr.height > vh - 8) top = Math.max(8, vh - 8 - mr.height);
      menu.style.left = left + 'px'; menu.style.top = top + 'px'; menu.style.visibility = '';
      menu.classList.add(rail ? 'is-in--right' : 'is-in');
    }
    function wireDrops() {
      $$('.ce-drop').forEach(drop => {
        const btn = drop.querySelector('.ce-drop__b'), menu = drop.querySelector('.ce-menu'); if (!btn || !menu) return;
        btn.setAttribute('aria-haspopup', 'true'); btn.setAttribute('aria-expanded', 'false');
        const railBtn = !!btn.closest('.ce-rail');
        const inCorner = e => { if (!railBtn || typeof e.clientX !== 'number') return false; const b = btn.getBoundingClientRect(); return b.right - e.clientX <= 13 && b.bottom - e.clientY <= 13; };
        const openMenu = e => {
          if (e) { e.preventDefault(); e.stopPropagation(); }
          const was = !menu.hidden; closeMenus(); if (was) return;
          hideTip(); btn.setAttribute('aria-expanded', 'true');
          syncProps(); if (menu.__fill) menu.__fill(); placeMenu(menu, btn.getBoundingClientRect(), railBtn);
        };
        let hold = 0, heldOpen = false;
        btn.addEventListener('click', e => {
          if (heldOpen) { heldOpen = false; e.preventDefault(); e.stopPropagation(); return; }
          const isTool = btn.classList.contains('ce-tool') && !!btn.dataset.tool;
          if (isTool && !inCorner(e) && !btn.classList.contains('is-on')) return;   // picks the tool (the root's click does it)
          openMenu(e);
        });
        btn.addEventListener('pointerdown', e => { if (!railBtn || e.button) return; clearTimeout(hold); heldOpen = false; hold = setTimeout(() => { hold = 0; heldOpen = true; openMenu(null); }, 340); });
        const drop2 = () => { clearTimeout(hold); hold = 0; };
        btn.addEventListener('pointerup', drop2); btn.addEventListener('pointerleave', drop2); btn.addEventListener('pointercancel', drop2);
        btn.addEventListener('contextmenu', e => { if (railBtn) openMenu(e); });
      });
    }
    /* the object menu: one, raised by a right-click (or a long press) on the object or its row (S 2763-2793) */
    function openCtx(cx, cy) {
      closeMenus();
      const es = selEntries(), n = es.length, anyText = n === 1 && es[0].it.kind === 'text';
      const lockable = n > 0, anyGroup = es.some(e => e.it.kind === 'group');
      const it = (act, label, key, on, cls) => `<button class="ce-item${on ? '' : ' is-off'}${cls ? ' ' + cls : ''}" type="button" data-ctx="${act}" role="menuitem"${on ? '' : ' aria-disabled="true"'}>${label}${key ? `<em>${key}</em>` : ''}</button>`;
      const name = n === 1 ? rowName(es[0].it) : n > 1 ? `${n} things` : 'Nothing selected';
      ctx.innerHTML = `<div class="ce-menu__head">${esc(name)}</div>`
        + (anyText ? it('edit', 'Edit the words…', 'double-click', true) : '')
        + it('cut', 'Cut', '⌘X', n > 0) + it('copy', 'Copy', '⌘C', n > 0) + it('paste', 'Paste', '⌘V', !!(S.clip && S.clip.length)) + it('pasteInPlace', 'Paste in place', '⇧⌘V', !!(S.clip && S.clip.length))
        + it('dup', 'Duplicate', '⌘D', n > 0) + it('del', 'Delete', '⌫', n > 0, 'ce-item--danger')
        + '<div class="ce-menu__head">Order</div>'
        + it('front', 'Bring to front', '⇧⌘]', n > 0) + it('forward', 'Bring forward', '⌘]', n > 0) + it('backward', 'Send backward', '⌘[', n > 0) + it('back', 'Send to back', '⇧⌘[', n > 0)
        + '<div class="ce-menu__head">Layer</div>'
        + it('group', 'Group', '⌘G', n > 1) + it('ungroup', 'Ungroup', '⇧⌘G', anyGroup)
        + it('lock', 'Lock', '', lockable) + it('hide', 'Hide', '', lockable);
      ctx.innerHTML += xMenuHtml(true);
      placeMenu(ctx, { left: cx, right: cx, top: cy, bottom: cy }, false);
    }
    function ctxAct(a) {
      closeMenus();
      const ids = S.sel.slice();
      if (a === 'edit') { if (ids[0]) openEditor(ids[0]); }
      else if (a === 'cut') cutSel(); else if (a === 'copy') copySel(); else if (a === 'paste') paste(false); else if (a === 'pasteInPlace') paste(true);
      else if (a === 'dup') duplicate(); else if (a === 'del') deleteSel();
      else if (a === 'front' || a === 'forward' || a === 'backward' || a === 'back') arrangeSel(a);
      else if (a === 'group') groupSel(); else if (a === 'ungroup') ungroupSel();
      else if (a === 'lock' || a === 'hide') {
        const flag = a === 'lock' ? 'locked' : 'hidden';
        commit(replaceIn(items(), new Map(ids.map(id => { const n = Object.assign({}, entry(id).it, { [flag]: true }); delete n._autoLock; delete n._lockedIn; return [id, n]; }))), []);
      }
    }

    /* ── hover cards and the wave on the rail (J 18861-19041) ── */
    let tipEl = null, tipTimer = 0;
    function hideTip() { clearTimeout(tipTimer); if (tipEl) tipEl.classList.remove('is-in'); }
    function showTip(btn) {
      const t = TIPS[btn.dataset.tip] || xTip(btn.dataset.tip); if (!t || rootEl.querySelector('.ce-menu:not([hidden])')) return;
      if (!tipEl) { tipEl = doc.createElement('div'); tipEl.className = 'ce-tip'; rootEl.appendChild(tipEl); }
      const src = btn.querySelector('svg'), filled = src && (src.getAttribute('fill') || 'none') !== 'none';
      const paint = filled ? 'fill:#9a7c3e;stroke:none' : 'fill:none;stroke:#9a7c3e;stroke-width:' + ((src && src.getAttribute('stroke-width')) || '1.6') + ';stroke-linecap:round;stroke-linejoin:round';
      tipEl.innerHTML = `<svg class="ce-tip__ico" viewBox="0 0 56 56" aria-hidden="true"><circle class="b" cx="28" cy="9" r="4.4"/><circle class="c" cx="28" cy="32" r="18.5"/><g transform="translate(15 19) scale(1.08)" style="${paint}">${src ? src.innerHTML : ''}</g></svg>`
        + `<span><b>${esc(t[0])}${t[2] ? `<i>${esc(t[2])}</i>` : ''}</b><span>${esc(t[1])}</span></span>`;
      tipEl.style.left = '0px'; tipEl.style.top = '0px';
      const r = btn.getBoundingClientRect(), c = tipEl.getBoundingClientRect(), vw = doc.documentElement.clientWidth || root.innerWidth, vh = doc.documentElement.clientHeight || root.innerHeight;
      let left = r.right + 9; if (left + c.width > vw - 8) left = Math.max(8, r.left - 9 - c.width);
      tipEl.style.left = left + 'px'; tipEl.style.top = Math.min(vh - 8 - c.height, Math.max(8, r.top + r.height / 2 - c.height / 2)) + 'px';
      tipEl.classList.add('is-in');
    }
    function wireRail() {
      const rail = $('.ce-rail');
      const fromMenu = e => !!(e.target.closest && e.target.closest('.ce-menu'));
      rail.addEventListener('pointerover', e => {
        if (fromMenu(e) || e.pointerType === 'touch') { hideTip(); return; }
        const b = e.target.closest('[data-tip]'); if (!b) { hideTip(); return; }
        clearTimeout(tipTimer); tipTimer = setTimeout(() => showTip(b), 260);
      });
      rail.addEventListener('pointerleave', hideTip); rail.addEventListener('pointerdown', hideTip);
      let raf = 0, y = null;
      const paint = () => { raf = 0; rail.querySelectorAll('.ce-tool svg').forEach(g => { if (y == null) { g.style.transform = ''; return; } const r = g.getBoundingClientRect(), d = (y - (r.top + r.height / 2)) / 58, k = Math.exp(-d * d); g.style.transform = k > 0.012 ? `scale(${(1 + 0.78 * k).toFixed(4)})` : ''; }); };
      const q = () => { if (!raf) raf = requestAnimationFrame(paint); };
      rail.addEventListener('pointermove', e => { if (e.pointerType === 'touch') return; if (fromMenu(e) || rootEl.querySelector('.ce-menu:not([hidden])')) { if (y !== null) { y = null; q(); } return; } y = e.clientY; q(); });
      rail.addEventListener('pointerleave', () => { y = null; q(); });
    }

    /* ── the Layers panel's own gestures: a row selects (⌘ / Shift add), eye and lock, a twisty, a double-click renames, a drag reorders ── */
    function wireLayers() {
      let drag = null;
      layersEl.addEventListener('pointerdown', e => {
        const row = e.target.closest('.ce-layer'); if (!row || e.button) return;
        if (e.target.closest('[data-lact]') || e.target.closest('input')) return;
        drag = { id: row.dataset.id, y0: e.clientY, row, live: false, pid: e.pointerId };
      });
      layersEl.addEventListener('pointermove', e => {
        if (!drag) return;
        if (!drag.live) { if (Math.abs(e.clientY - drag.y0) < 5) return; drag.live = true; drag.row.classList.add('is-dragging'); try { layersEl.setPointerCapture(drag.pid); } catch (er) { /* fine */ } }
        layersEl.querySelectorAll('.is-drop-above,.is-drop-below').forEach(r => r.classList.remove('is-drop-above', 'is-drop-below'));
        const over = doc.elementFromPoint(e.clientX, e.clientY), r = over && over.closest && over.closest('.ce-layer');
        drag.target = null;
        if (r && r !== drag.row) { const b = r.getBoundingClientRect(), above = e.clientY < b.top + b.height / 2; r.classList.add(above ? 'is-drop-above' : 'is-drop-below'); drag.target = { id: r.dataset.id, above }; }
        const lb = layersEl.getBoundingClientRect(); if (e.clientY < lb.top + 18) layersEl.scrollTop -= 8; else if (e.clientY > lb.bottom - 18) layersEl.scrollTop += 8;
      });
      const end = e => {
        const d = drag; drag = null; if (!d) return;
        try { layersEl.releasePointerCapture(d.pid); } catch (er) { /* fine */ }
        layersEl.querySelectorAll('.is-drop-above,.is-drop-below,.is-dragging').forEach(r => r.classList.remove('is-drop-above', 'is-drop-below', 'is-dragging'));
        if (!d.live) { rowClick(d.id, e); return; }
        if (d.target) moveLayer(d.id, d.target.id, d.target.above);
      };
      layersEl.addEventListener('pointerup', end); layersEl.addEventListener('pointercancel', () => { drag = null; renderLayers(true); });
      layersEl.addEventListener('click', e => {
        const b = e.target.closest('[data-lact]'); if (!b) return;
        const id = b.closest('.ce-layer').dataset.id;
        e.stopPropagation();
        if (b.dataset.lact === 'eye') toggleFlag(id, 'hidden');
        else if (b.dataset.lact === 'lock') toggleFlag(id, 'locked');
        else if (b.dataset.lact === 'twist') { if (S.open.has(id)) S.open.delete(id); else S.open.add(id); renderLayers(); }
      });
      layersEl.addEventListener('dblclick', e => {
        const nameEl = e.target.closest('.ce-layer__name'); if (!nameEl) return;
        const id = nameEl.closest('.ce-layer').dataset.id, en = entry(id); if (!en) return;
        nameEl.innerHTML = `<input type="text" value="${esc(rowName(en.it))}" aria-label="Layer name" maxlength="60">`;
        const inp = nameEl.querySelector('input'); inp.focus(); inp.select();
        let done2 = false;
        const fin2 = keep => { if (done2) return; done2 = true; if (keep) rename(id, inp.value); renderLayers(true); };
        inp.addEventListener('blur', () => fin2(true));
        inp.__ceKey = ev => { if (ev.key === 'Enter') { ev.preventDefault(); fin2(true); } else if (ev.key === 'Escape') { ev.preventDefault(); fin2(false); } };
      });
      layersEl.addEventListener('contextmenu', e => {
        const row = e.target.closest('.ce-layer'); if (!row) return;
        e.preventDefault(); const id = row.dataset.id, en = entry(id);
        if (en && !en.it.locked && !S.sel.includes(id)) setSel([id]);
        openCtx(e.clientX, e.clientY);
      });
    }
    function rowClick(id, e) {
      const en = entry(id); if (!en) return;
      if (S.editing) closeEditor();
      if (en.it.locked) { setSel([]); return; }
      if (e.metaKey || e.ctrlKey || e.shiftKey) setSel(S.sel.includes(id) ? S.sel.filter(s => s !== id) : S.sel.concat(id));
      else setSel([id]);
      if (S.tool !== 'select' && S.tool !== 'marquee') setTool('select');
    }
    /** A row dropped above or below another: the item moves next to it, in that row's group (a row is drawn above the one under it: above means later in the list). */
    function moveLayer(id, targetId, above) {
      if (id === targetId) return;
      let next = normaliseAncestors(items(), [id, targetId], geo);
      let ixm = indexTree(next, geo);
      const me = ixm.get(id), te = ixm.get(targetId); if (!me || !te) return;
      if (te.anc.includes(id)) return;   // not into itself
      const moving = pushDown(me.it, null, geo, me.P, true);
      next = removeIds(next, [id], true); ixm = indexTree(next, geo);
      const t2 = ixm.get(targetId); if (!t2) return;
      const pid = t2.parent ? t2.parent.id : null;
      const list = (pid == null ? next : t2.parent.children).slice();
      const at = list.findIndex(x => x.id === targetId);
      list.splice(above ? at + 1 : at, 0, moving);
      commit(pruneEmpty(setListIn(next, pid, list)), [id]);
    }

    /* ── faces ── */
    function applyFaceView() { S.hover = null; ixFor = null; layersFor = null; els.clear(); itemsG.innerHTML = ''; fit(); }
    function switchFace(f) {
      if (!S.faces[f] || f === S.face) return;
      if (S.editing) closeEditor();
      xCommit();
      abandon();
      S.face = f; S.sel = []; applyFaceView(); refresh();
    }

    /* ── keyboard: ONE listener, on the window, in the capture phase, so the page under the editor never sees a key while it is open (the Engraving card's own A,
       S and arrows must not fire behind it), and every key the editor uses is handled here ── */
    const isField = t => t && (/^(INPUT|TEXTAREA|SELECT)$/.test(t.tagName) || t.isContentEditable);
    function onKey(e) {
      if (!rootEl.isConnected) return;
      e.stopPropagation();
      const t = e.target, meta = e.metaKey || e.ctrlKey, k = e.key;
      if (S.ask) { if (k === 'Escape') { e.preventDefault(); S.ask.done(false); } return; }
      if (S.editing && t === S.editing.ta) {
        if (k === 'Escape' || (k === 'Enter' && !e.shiftKey)) { e.preventDefault(); closeEditor(); }
        return;
      }
      if (t && t.__ceKey) { t.__ceKey(e); return; }
      if (isField(t)) { fieldKey(e); return; }
      if (k === 'Escape' && rootEl.querySelector('.ce-menu:not([hidden])')) { e.preventDefault(); closeMenus(); return; }
      { const xt = xTool(S.tool); if (xt && typeof xt.onKey === 'function' && k !== ' ' && e.code !== 'Space') { const r = xCall(xt, 'onKey', e); if (r === true || e.defaultPrevented) { e.preventDefault(); xCommit(); return; } } }
      if (k === ' ' || e.code === 'Space') { e.preventDefault(); if (!S.space) { S.space = true; stage.classList.add('is-space'); } return; }
      const kl = k.length === 1 ? k.toLowerCase() : k;
      if (meta) {
        if (kl === 'z') { e.preventDefault(); if (e.shiftKey) redo(); else undo(); return; }
        if (kl === 'y') { e.preventDefault(); redo(); return; }
        if (kl === 'a') { e.preventDefault(); selectAll(); return; }
        if (kl === 'c') { if (!X.importers.length) e.preventDefault(); if (copySel()) xCopied(); return; }
        if (kl === 'x') { if (!X.importers.length) e.preventDefault(); const c0 = S.clip; cutSel(); if (S.clip !== c0) xCopied(); return; }
        if (kl === 'v') { if (X.importers.length) { xPasteSoon(e.shiftKey); return; } e.preventDefault(); paste(e.shiftKey); return; }
        if (kl === 'd') { e.preventDefault(); duplicate(); return; }
        if (kl === 'g') { e.preventDefault(); if (e.shiftKey) ungroupSel(); else groupSel(); return; }
        if (k === ']' || k === '}' || e.code === 'BracketRight') { e.preventDefault(); arrangeSel(e.shiftKey ? 'front' : 'forward'); return; }
        if (k === '[' || k === '{' || e.code === 'BracketLeft') { e.preventDefault(); arrangeSel(e.shiftKey ? 'back' : 'backward'); return; }
        if (k === '0' || e.code === 'Digit0' || e.code === 'Numpad0') { e.preventDefault(); fit(); return; }
        if (k === '1' || e.code === 'Digit1' || e.code === 'Numpad1') { e.preventDefault(); zoomTo(100); return; }
        if (k === '2' || e.code === 'Digit2' || e.code === 'Numpad2') { e.preventDefault(); zoomToBox(selBox()); return; }
        if (k === '=' || k === '+' || e.code === 'NumpadAdd') { e.preventDefault(); zoomCmd('in'); return; }
        if (k === '-' || k === '_' || e.code === 'NumpadSubtract') { e.preventDefault(); zoomCmd('out'); return; }
        if (kl === 's') { e.preventDefault(); save(); return; }
        xKeyRun(e);
        return;
      }
      if (/^Arrow(Left|Right|Up|Down)$/.test(k)) {
        if (!S.sel.length) return;
        e.preventDefault();
        const st = (e.shiftKey ? NUDGE_BIG_MM : NUDGE_MM) * PT;
        nudge(k === 'ArrowLeft' ? -st : k === 'ArrowRight' ? st : 0, k === 'ArrowUp' ? -st : k === 'ArrowDown' ? st : 0);
        return;
      }
      if (k === 'Delete' || k === 'Backspace') { if (S.sel.length) { e.preventDefault(); deleteSel(); } return; }
      if (k === 'Enter') { const es = selEntries(); if (es.length === 1 && es[0].it.kind === 'text') { e.preventDefault(); openEditor(es[0].it.id); } return; }
      if (k === 'Escape') { e.preventDefault(); if (S.g) { abandon(); return; } if (S.sel.length) { setSel([]); return; } if (S.tool !== 'select') { setTool('select'); return; } requestClose(); return; }
      if (e.altKey) { xKeyRun(e); return; }
      const map = { v: 'select', m: 'marquee', t: 'text', r: 'rect', e: 'ellipse', l: 'line', h: 'hand' };
      if (map[kl]) { e.preventDefault(); setTool(map[kl]); return; }
      if (k === '!' || (e.shiftKey && e.code === 'Digit1')) { e.preventDefault(); fit(); return; }
      if (k === '@' || (e.shiftKey && e.code === 'Digit2')) { e.preventDefault(); zoomToBox(selBox()); return; }
      xKeyRun(e);
    }
    function onKeyUp(e) {
      if (!rootEl.isConnected) return;
      e.stopPropagation();
      if (e.key === ' ' || e.code === 'Space') { S.space = false; stage.classList.remove('is-space'); }
    }
    function fieldKey(e) {
      const el = e.target, f = el.dataset && el.dataset.f; if (!f) return;
      const k = e.key;
      if (k === 'Enter') { e.preventDefault(); el.blur(); return; }
      if (k === 'Escape') { e.preventDefault(); el.__revert = true; el.blur(); return; }
      if (k === 'ArrowUp' || k === 'ArrowDown') {
        e.preventDefault();
        const up = k === 'ArrowUp' ? 1 : -1;
        const thick = f === 'wt' || f === 'lw';
        const step = f === 'r' ? (e.shiftKey ? 15 : 1) : f === 'pt' ? (e.shiftKey ? 2 : 0.5) : thick ? (e.shiftKey ? 0.05 : WT_STEP_MM) : (e.shiftKey ? 1 : 0.1);
        const base = fin(numIn(el.value)) ? numIn(el.value) : f === 'wt' ? 0 : f === 'lw' ? LW_LASER_PT * MM : 0; let v = Math.round((base + up * step) * 1000) / 1000;
        if (f === 'w' || f === 'h' || f === 'pt') v = Math.max(step, v);
        if (f === 'wt') v = clamp(v, WT_MIN_MM, WT_MAX_MM); if (f === 'lw') v = clamp(v, LW_MIN_MM, LW_MAX_MM);
        el.value = f === 'wt' ? fmtWt(v) : trim(v, 3);
        if (f === 'pt') setTextSize(v); else if (f === 'wt') setWeightMm(v, true); else if (f === 'lw') setStrokeMm(v, true); else commitField(f, v);
        el.__v0 = el.value;
      }
    }

    /* ── closing ── */
    function dirty() {
      for (const f of Object.keys(S.faces)) { const F = S.faces[f]; if (F.items === F.base) continue; if (changeCount(diffFace(exportItems(F.base, geo), exportItems(F.items, geo)))) return true; }
      return false;
    }
    function result() {
      const faces = {}, changes = {};
      for (const f of ['front', 'back']) {
        const F = S.faces[f]; if (!F || !(opts.faces && opts.faces[f])) continue;
        const out = exportItems(F.items, geo);
        faces[f] = Object.assign({}, F.docIn, { face: f, unitPt: true, frame: Object.assign({}, F.frame), items: out });
        changes[f] = diffFace(exportItems(F.base, geo), out);
      }
      return { faces, changes };
    }
    function ask(o) {
      return new Promise(res => {
        const box = doc.createElement('div'); box.className = 'ce-ask';
        box.innerHTML = `<div class="ce-ask__card" role="alertdialog" aria-modal="true" aria-label="${esc(o.title)}"><h3>${esc(o.title)}</h3><p>${esc(o.text)}</p><div class="ce-ask__b">`
          + `<button class="ce-btn ce-btn--ghost" type="button" data-ask="no">${esc(o.no)}</button><button class="ce-btn ce-btn--ink" type="button" data-ask="yes">${esc(o.yes)}</button></div></div>`;
        win.appendChild(box);
        S.ask = { done: v => { if (!S.ask) return; S.ask = null; box.remove(); try { win.focus({ preventScroll: true }); } catch (e) { /* fine */ } res(v); } };
        box.addEventListener('click', ev => { const b = ev.target.closest('[data-ask]'); if (b) S.ask.done(b.dataset.ask === 'yes'); else if (ev.target === box) S.ask.done(false); });
        requestAnimationFrame(() => { const nb = box.querySelector('[data-ask="no"]'); if (nb) nb.focus(); });
      });
    }
    async function requestClose() {
      if (S.ask) return;
      if (S.editing) closeEditor();
      abandon();
      if (!dirty()) { finish(null); return; }
      const yes = await ask({ title: 'Leave without saving?', text: 'The changes you made to this charm will be lost. Nothing is kept until you press Save.', yes: 'Discard changes', no: 'Keep editing' });
      if (yes) finish(null);
    }
    function save() {
      if (S.ask) return;
      if (S.editing) closeEditor();
      abandon();
      finish(result());
    }
    let finished = false;
    function finish(v) {
      if (finished) return; finished = true;
      root.removeEventListener('keydown', onKey, true); root.removeEventListener('keyup', onKeyUp, true); root.removeEventListener('resize', onResize); root.removeEventListener('blur', onBlur);
      xClose(v);
      if (ro) ro.disconnect();
      if (rootEl.parentNode) rootEl.parentNode.removeChild(rootEl);
      doc.documentElement.style.overflow = prevOverflow;
      try { if (prevFocus && prevFocus.focus) prevFocus.focus({ preventScroll: true }); } catch (e) { /* the page moved on */ }
      done(v);
    }

    /* ── the extension points (the header's EXTENSION POINTS, 28 Oct 2026): the ctx every tool, command, importer and panel is handed, the one undo step their changes
       gather into, and the chrome that calls them ── */
    let xPending = null, xStale = false;   // (the work as it was before an extension's first change since its last step; the Layers not yet redrawn for live changes)
    const xErr = (where, er) => { try { root.console && root.console.error('CharmEditor extension ' + where + ':', er); } catch (e2) { /* fine */ } };
    function xCall(t, name, e) {
      const fn = t && t[name]; if (typeof fn !== 'function') return undefined;
      const p = e && typeof e.clientX === 'number' ? fromClient(e.clientX, e.clientY) : null;
      try { return fn.call(t, ectx, e, p ? { x: p[0], y: p[1] } : null); } catch (er) { xErr(t.id + '.' + name, er); return undefined; }
    }
    function xApply(next, sel) {
      if (S.editing) closeEditor();
      if (!xPending) xPending = snapshot();
      setItems(next); ixFor = null;
      const ixm = ix(); S.sel = topmost((sel || S.sel).filter(id => ixm.has(id)), ixm);
      render(); renderUI(); syncProps(); xStale = true;
    }
    /** The extensions' changes since the last step become ONE step of history (nothing changed: no step). */
    function xCommit(label) {
      const was = xPending; xPending = null;
      const changed = !!was && was.items !== (S.faces[was.face] || {}).items;
      if (changed) { if (label) was.label = String(label); S.hist.push(was); S.nudgeAt = 0; }
      if (changed || xStale) { xStale = false; refresh(); }
      return changed;
    }
    function xAdd(list, o) {
      o = o || {};
      const add = prepIn((Array.isArray(list) ? list : [list]).filter(Boolean).map(it => Object.assign({ source: 'added', hidden: false, locked: false }, it)), []);
      if (!add.length) return [];
      let next = items();
      const pe = o.parent != null ? entry(String(o.parent)) : null;
      if (pe && pe.it.kind === 'group') {
        // inside a group, in face coordinates: the group's transform (and those above it) pushed into its children first
        const pid = pe.it.id; next = normaliseAncestors(next, [pid], geo);
        const g = pushDown(indexTree(next, geo).get(pid).it, null, geo, I, true);
        next = replaceIn(next, new Map([[pid, Object.assign({}, g, { children: (g.children || []).concat(add) })]]));
      } else next = next.concat(add);
      xApply(next, o.select === false ? null : add.map(a => a.id));
      return add.map(a => a.id);
    }
    function xReplace(map) {
      const m = map instanceof Map ? map : new Map(Object.entries(map || {})), clean = new Map();
      for (const [id, it] of m) { const k = String(id); if (!entry(k)) continue; clean.set(k, it ? Object.assign({}, it, { id: it.id != null ? String(it.id) : k }) : null); }
      if (!clean.size) return false;
      xApply(replaceIn(items(), clean), S.sel.filter(id => clean.get(id) !== null)); return true;
    }
    function xRemove(ids) {
      const list = (Array.isArray(ids) ? ids : [ids]).map(String).filter(id => entry(id));
      if (!list.length) return false;
      xApply(removeIds(items(), list), S.sel.filter(id => !list.includes(id))); return true;
    }
    let sayT = 0, sayN = 0;
    /** A short line over the work area (ms 0: until the next one). */
    function say(text, ms) {
      const el = $('.ce-say'); if (!el) return; sayN++; clearTimeout(sayT);
      if (!text) { el.classList.remove('is-in'); return; }
      el.textContent = String(text); el.classList.add('is-in');
      if (ms !== 0) sayT = setTimeout(() => el.classList.remove('is-in'), fin(ms) && ms > 0 ? ms : 2600);
    }
    const ectx = {
      get doc() { return { face: S.face, unitPt: true, frame: Object.assign({}, frame()), items: items() }; },
      face: () => S.face, items: () => items(), selection: () => S.sel.slice(), frame: () => Object.assign({}, frame()), zoom: () => zoomPct(), tool: () => S.tool,
      select: ids => setSel((Array.isArray(ids) ? ids : ids == null ? [] : [ids]).map(String)),
      addItems: (list, o) => xAdd(list, o), replaceItems: map => xReplace(map), removeItems: ids => xRemove(ids), commit: label => xCommit(label),
      clientOf: (x, y) => { const b = sb(), p = toStage(x, y); return { x: b.l + p[0], y: b.t + p[1] }; },
      pointOf: (cx, cy) => fromClient(cx, cy), stageOf: (x, y) => toStage(x, y), view: () => V(),
      setZoom: (pct, about) => { if (!fin(+pct)) return; const b = sb(); if (about && fin(about.x) && fin(about.y)) zoomAtStage(+pct / 100 * PX100, about.x - b.l, about.y - b.t); else zoomTo(+pct); },
      geo, model, entry: id => { const e = entry(String(id)); return e ? { it: e.it, P: e.P, parent: e.parent } : null; },
      say: (text, ms) => say(text, ms), redraw: () => renderUI(), setTool: id => setTool(String(id)), root: rootEl, stage
    };
    // tools
    const xTip = id => { const t = xTool(id); return t ? [String(t.title || t.id), String(t.tip || t.hint || ''), xKeyOf(t)] : null; };
    function xArm(was, now) {
      if (was === now) return;
      const a = xTool(was), b = xTool(now);
      if (a) xCall(a, 'onDisarm');
      if (b) { stage.dataset.xtool = String(b.id); stage.style.setProperty('--xcur', b.cursor ? String(b.cursor) : 'crosshair'); xCall(b, 'onArm'); } else delete stage.dataset.xtool;
    }
    /** A flyout's rail button shows the tool of it last picked (the Shapes button does this for its own three). */
    function xFlyButton(t) {
      const xt = xTool(t); if (!xt) return;
      for (const drop of $$('.ce-rail .ce-drop')) {
        const btn = drop.querySelector('.ce-drop__b'), item = drop.querySelector(`.ce-menu [data-tool="${cssEsc(String(t))}"]`);
        if (!btn || !item || btn.dataset.tool === String(t)) continue;
        btn.dataset.tool = String(t); btn.innerHTML = xIcon(xt);
        if (/^x-/.test(drop.dataset.drop || '')) btn.dataset.tip = String(t);
      }
    }
    // commands
    const xKeys = X.commands.map(c => ({ c, k: c.c.key ? parseKey(c.c.key) : null })).filter(x => x.k);
    const xEnabled = c => { if (typeof c.c.enabled !== 'function') return true; try { return !!c.c.enabled(ectx); } catch (er) { xErr(c.id + '.enabled', er); return false; } };
    function xRun(id) {
      const c = X.commands.find(x => x.id === id); if (!c || !xEnabled(c)) return;
      if (S.editing) closeEditor();
      abandon(); xCommit();
      const end = () => xCommit(c.c.title || c.id);
      let r; try { r = c.c.run.call(c.c, ectx); } catch (er) { xErr(c.id + '.run', er); }
      if (r && typeof r.then === 'function') r.then(end, er => { xErr(c.id + '.run', er); end(); }); else end();
    }
    function xKeyRun(e) {
      const kl = String(e.key || '').toLowerCase(), code = String(e.code || ''), mod = e.metaKey || e.ctrlKey;
      for (const { c, k } of xKeys) {
        if (k.mod !== mod || k.shift !== !!e.shiftKey || k.alt !== !!e.altKey) continue;
        if (kl !== k.key && code !== 'Key' + k.key.toUpperCase() && code !== 'Digit' + k.key) continue;
        e.preventDefault(); xRun(c.id); return true;
      }
      if (!mod && !e.altKey) for (const t of X.tools) if (t.key && String(t.key).toLowerCase() === kl) { e.preventDefault(); setTool(String(t.id)); return true; }
      return false;
    }
    function xMenuHtml(onlyOn) {
      const groups = new Map();
      for (const c of X.commands) {
        if (c.c.menu === false) continue;
        const on = xEnabled(c); if (onlyOn && !on) continue;
        const gn = c.c.group ? String(c.c.group) : 'More'; if (!groups.has(gn)) groups.set(gn, []); groups.get(gn).push({ c, on });
      }
      let h = '';
      for (const [gn, list] of groups) {
        h += `<div class="ce-menu__head">${esc(gn)}</div>`;
        for (const { c, on } of list) h += `<button class="ce-item${on ? '' : ' is-off'}" type="button" data-xcmd="${esc(c.id)}" role="menuitem"${on ? '' : ' aria-disabled="true"'}>${typeof c.c.icon === 'string' ? c.c.icon : ''}${esc(c.c.title || c.id)}${c.c.key ? `<em>${esc(keyLabel(c.c.key))}</em>` : ''}</button>`;
      }
      return h;
    }
    let xBarKey = null, xBarFor = null;
    function xBar() {
      const el = $('.ce-xbar'), bar = X.commands.filter(c => c.c.bar && typeof c.c.icon === 'string');
      if (!el || !bar.length) return;
      const key = S.face + '#' + S.sel.join('|'); if (key === xBarKey && items() === xBarFor) return;
      xBarKey = key; xBarFor = items();
      const on = bar.filter(xEnabled);
      el.innerHTML = on.map(c => `<button class="ce-prop" type="button" data-xcmd="${esc(c.id)}" title="${esc(c.c.title || c.id)}${c.c.key ? ' (' + esc(keyLabel(c.c.key)) + ')' : ''}" aria-label="${esc(c.c.title || c.id)}">${c.c.icon}</button>`).join('');
      el.hidden = !on.length;
    }
    // importers: the Import button, a file dropped on the work area, a file pasted
    function xImporterFor(file) {
      const name = String((file && file.name) || '').toLowerCase(), type = String((file && file.type) || '').toLowerCase();
      return X.importers.find(i => (Array.isArray(i.accept) ? i.accept : []).some(a => {
        a = String(a).toLowerCase(); if (!a) return false;
        if (a[0] === '.') return name.endsWith(a);
        const pre = a.replace(/\*$/, ''); return !!type && (pre.endsWith('/') ? type.startsWith(pre) : type === pre);
      })) || null;
    }
    async function xImport(files, info) {
      let any = false;
      for (const f of Array.from(files || []).filter(Boolean)) {
        const imp = xImporterFor(f), nm = f.name || 'the file';
        if (!imp) { say(`${nm} cannot be opened here`); continue; }
        any = true;
        if (S.editing) closeEditor();
        abandon(); xCommit();
        say(`Importing ${nm}…`, 0); const n0 = sayN;
        try { await imp.run(ectx, f, Object.assign({ via: 'button' }, info)); xCommit(imp.title || 'Import'); if (sayN === n0) say(''); }
        catch (er) { xErr(imp.id + '.run', er); xCommit(imp.title || 'Import'); say(`Could not import ${nm}: ${String((er && er.message) || er).slice(0, 120)}`, 5000); }
      }
      return any;
    }
    function xPick() { const inp = $('.ce-file'); if (!inp || !X.importers.length) return; inp.accept = xAccept.join(','); inp.value = ''; inp.click(); }
    let xPasteT = 0, xPasteAt = 0, xCopyArm = false;
    function xCopied() { if (!X.importers.length) return; S.clipMark = 'charm-editor:' + Date.now().toString(36) + ':' + (++idSeq).toString(36); xCopyArm = true; }
    /** ⌘V with importers: the paste event says what the system clipboard holds; the editor's own copy wins when it is the newest (the copy event wrote its mark there),
     *  a file or image is imported, and with no paste event at all (a browser that sends none) the editor's own copy is pasted a moment later. */
    function xPasteSoon(shift) { clearTimeout(xPasteT); xPasteT = setTimeout(() => { xPasteT = 0; xPasteAt = Date.now(); paste(shift); }, 150); S.pasteShift = shift; }
    const onCopy = e => {
      if (!rootEl.isConnected || !xCopyArm || isField(e.target)) return;
      xCopyArm = false; e.stopPropagation();
      try { e.clipboardData.setData('text/plain', S.clipMark); e.preventDefault(); } catch (er) { /* fine: the editor's copy still pastes */ }
    };
    const onPaste = e => {
      if (!rootEl.isConnected || S.ask || isField(e.target)) return;
      e.stopPropagation();
      const waited = !!xPasteT; clearTimeout(xPasteT); xPasteT = 0;
      if (!waited && Date.now() - xPasteAt < 1000) return;   // (the editor's own copy was pasted already)
      const cd = e.clipboardData, txt = cd ? cd.getData('text/plain') : '', files = [];
      if (cd) { for (const f of Array.from(cd.files || [])) files.push(f); if (!files.length && cd.items) for (const it of Array.from(cd.items)) if (it.kind === 'file') { const f = it.getAsFile(); if (f) files.push(f); } }
      e.preventDefault();
      const mine = !!(S.clip && S.clip.length) && (txt === S.clipMark || !files.some(f => xImporterFor(f)));
      if (mine) { paste(!!S.pasteShift); return; }
      if (files.length) xImport(files, { via: 'paste' });
    };
    const hasFiles = e => !!(e.dataTransfer && Array.from(e.dataTransfer.types || []).includes('Files'));
    // panels
    let xPanelOn = null; const xMounted = new Set();
    function xPanel(id) {
      const p = id != null ? X.panels.find(q => String(q.id) === String(id)) : null, el = $('.ce-xpanel'); if (!el) return;
      xPanelOn = p ? String(p.id) : null;
      el.hidden = !p; win.classList.toggle('has-xpanel', !!p);
      $$('[data-xtab]').forEach(b => { const on = b.dataset.xtab === xPanelOn; b.classList.toggle('is-on', on); b.setAttribute('aria-selected', String(on)); });
      $$('[data-xpanel]').forEach(b => b.classList.toggle('is-on', b.dataset.xpanel === xPanelOn));
      $$('.ce-xpanel__pane').forEach(pn => { pn.hidden = pn.dataset.pane !== xPanelOn; });
      if (p && !xMounted.has(xPanelOn)) { xMounted.add(xPanelOn); const pane = $$('.ce-xpanel__pane').find(pn => pn.dataset.pane === xPanelOn); try { p.mount(ectx, pane); } catch (er) { xErr(p.id + '.mount', er); } }
      try { if (root.localStorage && (root.innerWidth || 1000) > 900) root.localStorage.setItem('ce.xpanel', xPanelOn || '0'); } catch (er) { /* fine */ }
      requestAnimationFrame(() => { if (S.fitted) fit(); else applyView(); });
    }
    let xRaf = 0;
    function xChanged() {
      if (xRaf || !xMounted.size) return;
      xRaf = requestAnimationFrame(() => { xRaf = 0; for (const p of X.panels) if (xMounted.has(String(p.id)) && typeof p.update === 'function') { try { p.update(ectx); } catch (er) { xErr(p.id + '.update', er); } } });
    }
    function xClose(v) {
      clearTimeout(xPasteT); clearTimeout(sayT); if (xRaf) cancelAnimationFrame(xRaf);
      root.removeEventListener('paste', onPaste, true); root.removeEventListener('copy', onCopy, true);
      for (const fn of X.onClose) { try { fn(ectx, v); } catch (er) { xErr('onClose', er); } }
      for (const p of X.panels) if (xMounted.has(String(p.id)) && typeof p.unmount === 'function') { try { p.unmount(); } catch (er) { xErr(p.id + '.unmount', er); } }
    }

    /* ── wiring ── */
    const cssEsc =s => (root.CSS && root.CSS.escape ? root.CSS.escape(s) : String(s).replace(/["\\]/g, '\\$&'));
    rootEl.addEventListener('pointerdown', e => {
      if (!e.target.closest('.ce-menu') && !e.target.closest('.ce-drop__b')) closeMenus();
      // (J 13078: the click away is what closes the words, not a blur)
      if (S.editing && !e.target.closest('.ce-editor') && !e.target.closest('.ce-ctx')) closeEditor();
      // on a narrow screen the Layers are a drawer over the room: a press anywhere else folds it away
      if (S.dock && getComputedStyle(dock).position === 'absolute' && !e.target.closest('.ce-dock') && !e.target.closest('[data-act="dock"]') && !e.target.closest('.ce-menu') && !S.ask) { S.dock = false; layoutDock(); }
      if (xPanelOn && !e.target.closest('.ce-xpanel') && !e.target.closest('[data-xpanel]') && !e.target.closest('.ce-menu') && !S.ask && getComputedStyle($('.ce-xpanel')).position === 'absolute') xPanel(null);
    }, true);
    rootEl.addEventListener('click', e => {
      const t = e.target;
      // a pressed button gives the keys back to the editor (Space or Enter must not press it again)
      if (t.closest('button') && !t.closest('.ce-menu:not([hidden])') && !t.closest('.ce-ask')) requestAnimationFrame(() => { if (!S.editing && !isField(doc.activeElement)) try { win.focus({ preventScroll: true }); } catch (er) { /* fine */ } });
      const tool = t.closest('button[data-tool]'); if (tool) { setTool(tool.dataset.tool); if (tool.closest('.ce-menu')) closeMenus(); return; }
      const xcm = t.closest('[data-xcmd]'); if (xcm) { closeMenus(); if (!xcm.classList.contains('is-off')) xRun(xcm.dataset.xcmd); return; }
      const xp = t.closest('[data-xpanel]'); if (xp) { xPanel(xPanelOn === xp.dataset.xpanel ? null : xp.dataset.xpanel); return; }
      const xtab = t.closest('[data-xtab]'); if (xtab) { xPanel(xtab.dataset.xtab); return; }
      const act = t.closest('[data-act]');
      if (act && !act.disabled) {
        const a = act.dataset.act;
        if (a === 'undo') undo(); else if (a === 'redo') redo(); else if (a === 'cancel') requestClose(); else if (a === 'save') save();
        else if (a === 'link') { S.link = !S.link; syncProps(); }
        else if (a === 'wt0') setWeightMm(0); else if (a === 'lw0') resetStroke();
        else if (a === 'dock') { S.dock = !S.dock; try { if (root.localStorage && (root.innerWidth || 1000) > 900) root.localStorage.setItem('ce.dock', S.dock ? '1' : '0'); } catch (er) { /* fine */ } layoutDock(); }
        else if (a === 'forward' || a === 'backward') arrangeSel(a); else if (a === 'group') groupSel(); else if (a === 'ungroup') ungroupSel(); else if (a === 'delete') deleteSel();
        else if (a === 'import') xPick(); else if (a === 'xpanel0') xPanel(null);
        return;
      }
      const fc = t.closest('[data-face]'); if (fc && !fc.disabled) { switchFace(fc.dataset.face); return; }
      const z = t.closest('[data-zoom]'); if (z) { closeMenus(); zoomCmd(z.dataset.zoom); return; }
      const op = t.closest('[data-opt]'); if (op) { S[op.dataset.opt] = !S[op.dataset.opt]; syncProps(); applyView(); return; }
      const cm = t.closest('[data-cmd]'); if (cm) { closeMenus(); if (cm.dataset.cmd === 'unlockAll') setAll('locked', false); else if (cm.dataset.cmd === 'showAll') setAll('hidden', false); return; }
      const rl = t.closest('[data-role]'); if (rl) { closeMenus(); setRole(rl.dataset.role); return; }
      const fo = t.closest('[data-font]'); if (fo) { closeMenus(); const f = fonts.find(x => String(x.key) === fo.dataset.font); if (f) setFont(f.key); return; }
      const c = t.closest('[data-ctx]'); if (c) { if (!c.classList.contains('is-off')) ctxAct(c.dataset.ctx); return; }
    });
    for (const f of ['x', 'y', 'w', 'h', 'r', 'pt', 'wt', 'lw']) {
      const el = field(f);
      // the number is selected the moment the field is entered (J 16648), so typing replaces it; the click's own mouseup must not collapse that selection to a
      // caret, and a select-all put off to a later frame would land in the middle of whatever is being typed
      el.addEventListener('focus', () => { el.__v0 = el.value; el.__fresh = true; try { el.select(); } catch (e) { /* fine */ } });
      el.addEventListener('mouseup', e => { if (el.__fresh) { el.__fresh = false; e.preventDefault(); } });
      el.addEventListener('input', () => { el.__fresh = false; });
      el.addEventListener('blur', () => {
        S.run = null;   // (a run of arrow taps ends when the field is left: the next visit is a step of its own)
        if (el.__revert) { el.__revert = false; syncProps(); return; }
        if (el.value === el.__v0) return;
        if (f === 'pt') setTextSize(numIn(el.value)); else if (f === 'wt') setWeightMm(numIn(el.value)); else if (f === 'lw') setStrokeMm(numIn(el.value)); else commitField(f, el.value);
      });
    }
    stage.addEventListener('pointerdown', onDown);
    stage.addEventListener('pointermove', onMove);
    stage.addEventListener('pointerup', onUp);
    stage.addEventListener('pointercancel', e => { PTRS.delete(e.pointerId); abandon(); });
    stage.addEventListener('pointerleave', () => { cursor = null; queueRulers(); if (S.hover && !S.g) { S.hover = null; renderUI(); } });
    stage.addEventListener('wheel', onWheel, { passive: false });
    stage.addEventListener('dblclick', onDbl);
    stage.addEventListener('contextmenu', e => {
      e.preventDefault();
      const [x, y] = fromClient(e.clientX, e.clientY);
      const hit = hitAt(x, y, { deep: e.altKey });
      if (hit && !isSelected(hit.id)) setSel([hit.id]);
      openCtx(e.clientX, e.clientY);
    });
    rootEl.addEventListener('contextmenu', e => { if (!e.target.closest('.ce-stage') && !e.target.closest('.ce-layers') && !e.target.closest('.ce-drop__b')) e.preventDefault(); });
    const onResize = () => { closeMenus(); const was = S.fitted; layoutDock(); if (was) fit(); else { clampPan(); applyView(); } };
    const onBlur = () => { S.space = false; stage.classList.remove('is-space'); };
    root.addEventListener('keydown', onKey, true);
    root.addEventListener('keyup', onKeyUp, true);
    root.addEventListener('resize', onResize);
    root.addEventListener('blur', onBlur);
    // the extension points' own wiring: paste and copy (with importers), a file held over the work area, the Import picker, the Object menu filled as it opens
    root.addEventListener('paste', onPaste, true);
    root.addEventListener('copy', onCopy, true);
    stage.addEventListener('dragover', e => { if (!X.importers.length || !hasFiles(e)) return; e.preventDefault(); e.dataTransfer.dropEffect = 'copy'; if (!stage.classList.contains('is-drop')) { stage.classList.add('is-drop'); say('Drop to import', 0); } });
    stage.addEventListener('dragleave', e => { if (e.relatedTarget && stage.contains(e.relatedTarget)) return; if (stage.classList.contains('is-drop')) { stage.classList.remove('is-drop'); say(''); } });
    stage.addEventListener('drop', e => { if (!X.importers.length || !hasFiles(e)) return; e.preventDefault(); e.stopPropagation(); stage.classList.remove('is-drop'); say(''); xImport(e.dataTransfer.files, { via: 'drop', at: fromClient(e.clientX, e.clientY) }); });
    rootEl.addEventListener('dragover', e => { if (hasFiles(e)) e.preventDefault(); });   // (a file let go anywhere else in the editor must not open in the tab and lose the work)
    rootEl.addEventListener('drop', e => { if (hasFiles(e)) e.preventDefault(); });
    { const inp = $('.ce-file'); if (inp) inp.addEventListener('change', () => { if (inp.files && inp.files.length) xImport(inp.files, { via: 'button' }); }); }
    { const m = $('[data-drop="xcmd"] .ce-menu'); if (m) m.__fill = () => { m.innerHTML = xMenuHtml(false) || '<div class="ce-menu__note">Nothing to do here yet.</div>'; }; }
    let ro = null;
    if (root.ResizeObserver) { let first = true; ro = new root.ResizeObserver(() => { if (first) { first = false; return; } applyView(); }); ro.observe(stage); }
    // (any change to the view by hand ends "fitted": a resize then keeps the zoom and the place instead of fitting again)
    function layoutDock() { dock.hidden = !S.dock; win.classList.toggle('is-nodock', !S.dock); syncProps(); requestAnimationFrame(() => { if (S.fitted) fit(); else applyView(); }); }
    wireDrops(); wireRail(); wireLayers();
    dock.hidden = !S.dock; win.classList.toggle('is-nodock', !S.dock);
    setTool('select');
    render(); renderLayers(true); syncHist();
    fit(); syncProps();
    requestAnimationFrame(() => { fit(); try { win.focus({ preventScroll: true }); } catch (e) { /* fine */ } });
    if (X.panels.length) {
      let want = null; try { want = root.localStorage && root.localStorage.getItem('ce.xpanel'); } catch (e) { /* storage blocked */ }
      const p = want && want !== '0' ? X.panels.find(q => String(q.id) === want) : want === '0' ? null : X.panels.find(q => q.open);
      if (p && !(root.innerWidth && root.innerWidth <= 900)) xPanel(p.id);
    }
    Promise.resolve().then(() => { for (const fn of X.onOpen) { if (finished) break; try { fn(ectx); } catch (er) { xErr('onOpen', er); } } });

    /* read-outs for tests and the caller; nothing here changes the work except through the same calls the buttons make */
    return {
      face: () => S.face,
      items: f => (S.faces[f || S.face] || {}).items || [],
      selection: () => S.sel.slice(),
      zoom: () => zoomPct(),
      tool: () => S.tool,
      clientOf: (x, y) => { const b = sb(), p = toStage(x, y); return { x: b.l + p[0], y: b.t + p[1] }; },
      ptOf: (cx, cy) => fromClient(cx, cy),
      box: id => { const e = entry(id); return e ? geo.drawn(e.it, e.P) : null; },
      frame: () => Object.assign({}, frame()),
      result: () => result(),
      dirty: () => dirty(),
      history: () => ({ undo: S.hist.back.length, redo: S.hist.fwd.length }),
      setZoom: pct => zoomTo(pct),
      editing: () => (S.editing ? S.editing.id : null),
      ctx: ectx
    };
  }

  const api = { open, model, builtinTextToPaths, current: null, version: 1 };
  // the extension points (see the header): empty lists the other files push onto; anything registered before this file loaded is taken in
  Object.assign(api, { tools: [], commands: {}, importers: [], panels: [], onOpen: [], onClose: [] });
  { const pre = root && root.CharmEditor;
    if (pre && typeof pre === 'object') { for (const k of ['tools', 'importers', 'panels', 'onOpen', 'onClose']) if (Array.isArray(pre[k])) api[k].push(...pre[k]); if (pre.commands && typeof pre.commands === 'object') Object.assign(api.commands, pre.commands); } }
  return api;
});
