/* charm-nest-ce-masters.js — DESIGNING A CHARM: one opened from the charm library, or one started from nothing (DESIGN & ENGRAVE, Paul, 28 Oct 2026, items 16 and 10).
 *
 *   Paul: "Users also need the ability to search and open any charm existing in our charm repository and have full control over manipulating that charm, and then send that charm to the next
 *   given sheet color, chosen by the user and market as handmade custom." ... "I wanna make this as capable of a software as required to manipulate any need for a charm that includes designing
 *   custom charms".
 *
 * What a person does here makes a DESIGN (charm-nest-designs.js, CharmNestDesigns.put: a new version at every Save), never a change to a master or to any piece: the master's drawing is read as
 * it is (Pool.masterCharm, the .ai read, parsed and traced once per page by CharmNestPDF) and copied into the editor; nothing here reads or writes a piece's artwork record (charm-nest-art-edit.js).
 *
 *   search(q, o)   -> [{ sku, name, thumbUrl, widthMm, heightMm, sizes, file }]   the charm library AS THE PAGE HOLDS IT (B.master.entries: Master.load's copy, kept on this computer by
 *                     charm-nest-master-cache.js), searched in memory over an index made once per library read: no call to the cloud per keystroke. A page that holds no library yet asks
 *                     Master.load once (load()), and onChange(fn) says when it has come. o.limit (default 60). Every word typed must be in the SKU or its master file's name; the SKU itself
 *                     first, then SKUs that start with it, then a word that starts with it, then the rest, each in the library's order. An empty search: the masters opened last, then A to Z.
 *                     widthMm / heightMm: the drawing's own size (the index keeps its outer box, a margin of SIZE_PAD_PT round it). thumbUrl: the stored picture through the page's asset
 *                     address (the page's COEP blocks the Storage address itself), "" when the library has none.
 *   open(sku, o)   -> Promise<{ designId, version } | null>   the master opened in the Charm Editor as a NEW design: every part of its drawing a layer (the outline, its holes, every engraving),
 *                     where the .ai draws it, NOTHING locked unless the person locks it. o.size: one of a sized design's sizes (the first when none is named). An editor already open (the
 *                     library panel inside it) is closed first, through its own Cancel: changes there are asked about before anything is thrown away; "Keep editing" keeps it, and null
 *                     comes back.
 *   newDesign(o)   the start picker on its "New design" side: a size and an outline (circle, rounded rectangle, heart, tag, or one drawn by hand; the outline is the cut), a jump ring hole
 *                     or none (its diameter, a wall of WALL_MM to the edge), a name.
 *   openPicker(o)  the picker on its library side: the search and the charms as tiles (their stored pictures; a design with none is drawn by Pool.masterPreview as the Master tab draws it,
 *                     kept on this computer by charm-nest-thumbs.js, and only for the tiles on screen).
 *   openDesign(d)  a saved design opened again (CharmNestDesigns.get's design): its Save makes the next version of the same design.
 *   o.orderId / o.customName on any of them ride with the design (a custom order's design: charm-nest-custom-piece.js).
 *
 * The open itself is the bridge's design-mode path (window.CharmEditDesign.open, charm-nest-bridge.js: the engraving fonts and their outlines as a piece's open has them, lockRoles none); a page
 * without it opens the editor plainly. The editor's Save hands back the faces; they are saved as a version (by: the signed-in person), and a small card offers what is next: Send to sheet
 * (CharmNestCustomPiece.open, the pop-up of charm-nest-custom-piece.js: the metal, the order or a name), Keep editing (the editor again on this version), Done. A save that fails keeps the work:
 * "Back to the editor" opens it again as it was. Every step is told to the shared log (CNDesignLog.event: designOpen, designSave).
 *
 * The back: a design's back starts with the front's cut lines seen from behind, locked, as a guide (the back of a piece shows only its cut, charm-nest-art-edit.js); what the person adds on the
 * back is the design's back, and the guide is never saved (a back with nothing added is no back).
 *
 * UMD: window.CharmNestCeMasters in the page; module.exports in node (the model: the starting shapes, a master's face, the guide, the design made from the editor's result, the search). */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === 'object' && module.exports) module.exports = api;
  if (root && root.document) root.CharmNestCeMasters = api;
})(typeof self !== 'undefined' ? self : typeof globalThis !== 'undefined' ? globalThis : this, function (root) {
  'use strict';
  const doc = root && root.document ? root.document : null;
  const MM = 25.4 / 72, PT = 72 / 25.4;
  const KAPPA = 0.5522847498;
  const ID = [1, 0, 0, 1, 0, 0];
  const LW_CUT_PT = 0.25;                     // (the hairline charm-nest-art-edit.js paintOf writes a cut line with)
  const WALL_MM = 1, HOLE_MM = 1.5;           // a jump ring hole's diameter and the metal kept between it and the edge
  const SIZE_MIN_MM = 4, SIZE_MAX_MM = 80, HOLE_MIN_MM = 0.6, HOLE_MAX_MM = 5;
  const SHAPES = ['circle', 'rounded', 'heart', 'tag', 'free'];
  const SHAPE_WORD = { circle: 'Circle', rounded: 'Rounded rectangle', heart: 'Heart', tag: 'Tag', free: 'Your own outline' };
  const fin = v => typeof v === 'number' && Number.isFinite(v);
  const clamp = (v, a, b) => Math.min(b, Math.max(a, v));
  const r3 = v => Math.round(v * 1000) / 1000;
  const r1 = v => Math.round(v * 10) / 10;
  const mmTxt = v => String(+(+v).toFixed(1));
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const isMat = m => Array.isArray(m) && m.length === 6 && m.every(Number.isFinite);
  const editor = () => (root && root.CharmEditor) || (typeof require === 'function' ? require('./charm-nest-charm-editor.js') : null);
  const model = () => { const E = editor(); return E && E.model; };

  /* ═══ 1 · the starting outlines (points, y down, the frame 0,0 → w,h) ═══ */
  function ellipseSP(cx, cy, rx, ry) {
    const kx = rx * KAPPA, ky = ry * KAPPA;
    return [[['m', [cx + rx, cy]], ['c', [cx + rx, cy + ky], [cx + kx, cy + ry], [cx, cy + ry]], ['c', [cx - kx, cy + ry], [cx - rx, cy + ky], [cx - rx, cy]],
      ['c', [cx - rx, cy - ky], [cx - kx, cy - ry], [cx, cy - ry]], ['c', [cx + kx, cy - ry], [cx + rx, cy - ky], [cx + rx, cy]], ['h']]];
  }
  function roundRectSP(w, h, r) {
    r = Math.min(r, w / 2, h / 2); const k = r * KAPPA;
    return [[['m', [r, 0]], ['l', [w - r, 0]], ['c', [w - r + k, 0], [w, r - k], [w, r]], ['l', [w, h - r]], ['c', [w, h - r + k], [w - r + k, h], [w - r, h]], ['l', [r, h]],
      ['c', [r - k, h], [0, h - r + k], [0, h - r]], ['l', [0, r]], ['c', [0, r - k], [r - k, 0], [r, 0]], ['h']]];
  }
  /* the heart: two lobes whose tops touch the frame's top, their sides its sides, a point at the bottom; every control point inside the frame, so its tight box IS the size asked for.
     HEART_CLEFT: how far down the dip between the lobes is (a share of the height) */
  const HEART_CLEFT = 0.2;
  function heartSP(w, h) {
    const P = (x, y) => [x * w, y * h];
    return [[['m', P(0.5, 1)], ['c', P(0.38, 0.86), P(0, 0.62), P(0, 0.32)], ['c', P(0, 0.12), P(0.13, 0), P(0.27, 0)], ['c', P(0.4, 0), P(0.5, 0.09), P(0.5, HEART_CLEFT)],
      ['c', P(0.5, 0.09), P(0.6, 0), P(0.73, 0)], ['c', P(0.87, 0), P(1, 0.12), P(1, 0.32)], ['c', P(1, 0.62), P(0.62, 0.86), P(0.5, 1)], ['h']]];
  }
  /* the tag: its top corners cut at 45° (a luggage tag's), its bottom corners rounded */
  function tagSP(w, h) {
    const m = Math.min(w, h), c = m * 0.3, r = m * 0.1, k = r * KAPPA;
    return [[['m', [c, 0]], ['l', [w - c, 0]], ['l', [w, c]], ['l', [w, h - r]], ['c', [w, h - r + k], [w - r + k, h], [w - r, h]], ['l', [r, h]], ['c', [r - k, h], [0, h - r + k], [0, h - r]],
      ['l', [0, c]], ['h']]];
  }
  function outlineSP(shape, w, h) {
    if (shape === 'circle') return ellipseSP(w / 2, h / 2, w / 2, h / 2);
    if (shape === 'rounded') return roundRectSP(w, h, Math.min(w, h) * 0.15);
    if (shape === 'heart') return heartSP(w, h);
    if (shape === 'tag') return tagSP(w, h);
    return null;
  }
  /** Where a jump ring hole of radius r goes (pt): on the outline's middle line, a wall below its top edge there (the heart's: the dip between its lobes). */
  function holeFor(shape, w, h, r, wall) {
    const top = shape === 'heart' ? HEART_CLEFT * h : 0;
    return { cx: w / 2, cy: top + wall + r, r };
  }
  /** A start as asked, made sound: a known outline, a size within reach (a circle as wide as it is high), a hole that fits. */
  function startSpec(s) {
    s = s || {};
    const shape = SHAPES.includes(s.shape) ? s.shape : 'circle';
    let w = clamp(fin(+s.wMm) && +s.wMm > 0 ? +s.wMm : 15, SIZE_MIN_MM, SIZE_MAX_MM), h = clamp(fin(+s.hMm) && +s.hMm > 0 ? +s.hMm : w, SIZE_MIN_MM, SIZE_MAX_MM);
    if (shape === 'circle') h = w;
    const holeMm = clamp(fin(+s.holeMm) && +s.holeMm > 0 ? +s.holeMm : HOLE_MM, HOLE_MIN_MM, HOLE_MAX_MM), wallMm = fin(+s.wallMm) && +s.wallMm > 0 ? +s.wallMm : WALL_MM;
    const hole = shape !== 'free' && s.hole !== false && !!(s.hole || s.hole == null);
    return { shape, wMm: r3(w), hMm: r3(h), hole, holeMm: r3(holeMm), wallMm, name: String(s.name || '').trim().slice(0, 80) };
  }
  /** Why this start cannot be made, or "": a hole that would come out of the bottom of a small charm. */
  function startWhy(spec) {
    const s = startSpec(spec);
    if (!s.hole) return '';
    const top = s.shape === 'heart' ? HEART_CLEFT * s.hMm : 0;
    if (top + 2 * s.wallMm + s.holeMm > s.hMm * 0.6 || s.holeMm + 2 * s.wallMm > s.wMm * 0.6) return `A ${mmTxt(s.holeMm)} mm hole needs a bigger charm: make it larger, the hole smaller, or leave the hole off`;
    return '';
  }
  const defaultName = s => `${SHAPE_WORD[s.shape]} ${s.shape === 'circle' ? `Ø ${mmTxt(s.wMm)}` : `${mmTxt(s.wMm)} × ${mmTxt(s.hMm)}`} mm`;
  const cutItem = (id, name, subpaths) => ({ id, kind: 'path', role: 'cut', source: 'added', name, subpaths, stroke: [0, 0, 0], fill: null, lwPt: LW_CUT_PT, tx: 0, ty: 0, sx: 1, sy: 1, rot: 0 });
  /** The front a new design starts with: its outline (the cut) and its hole, framed by the size asked for. "Your own outline": the frame alone. */
  function startFace(spec) {
    const s = startSpec(spec), w = s.wMm * PT, h = s.hMm * PT, items = [], sp = outlineSP(s.shape, w, h);
    if (sp) items.push(cutItem('outline', 'Outline', sp));
    if (sp && s.hole) { const o = holeFor(s.shape, w, h, s.holeMm * PT / 2, s.wallMm * PT); items.push(cutItem('hole', 'Jump ring hole', ellipseSP(o.cx, o.cy, o.r, o.r))); }
    return { face: 'front', unitPt: true, frame: { x0: 0, y0: 0, wPt: w, hPt: h }, items };
  }

  /* ═══ 2 · a master's drawing as the editor's front, and the guide on the back ═══ */
  /** Every part of the master's charm a layer (charm-nest-art-edit.js itemsOf: its ids, roles and paint), as the .ai draws it (no turn, no mirror: the front's frame with y down), moved so
   *  the outline's box starts at 0,0 and framed by it. A part the editor cannot hold (live text, a picture: no path) is left out and counted. The master's charm is never touched. */
  function masterFace(charm, AE, M) {
    M = M || model();
    const all = AE.itemsOf(charm, { M: ID, R: ID, face: 'front' }, 'front');
    const outId = AE.memberId(charm, charm.members.indexOf(charm.outline));
    const keep = all.filter(it => it && it.id != null && Array.isArray(it.subpaths) && it.subpaths.length);
    const out = keep.find(i => i.id === outId) || keep.find(i => i.role === 'cut');
    let b = out ? M.bboxOf(out.subpaths) : null;
    if (!b) for (const it of keep) b = M.union(b, M.bboxOf(it.subpaths));
    b = b || [0, 0, 72, 72];
    const T = [1, 0, 0, 1, -b[0], -b[1]];
    const items = keep.map(it => {
      const o = { id: String(it.id), kind: 'path', role: it.role, source: 'master', subpaths: M.transformSubpaths(it.subpaths, T), stroke: it.stroke || null, fill: it.fill || null, tx: 0, ty: 0, sx: 1, sy: 1, rot: 0 };
      if (it.lwPt) o.lwPt = it.lwPt;
      if (it.even) o.evenOdd = true;
      if (it.layer) o.layer = it.layer;
      if (it.role === 'cut') o.name = it.id === outId ? 'Outline' : 'Cut line';
      return o;
    });
    return { face: 'front', unitPt: true, frame: { x0: 0, y0: 0, wPt: b[2] - b[0], hPt: b[3] - b[1] }, items, skipped: all.length - keep.length };
  }
  /** The cut lines of a face as drawn (each part's own transform and its groups' put on its geometry), in face points. */
  function cutOutlines(items, M, geo, P) {
    M = M || model(); geo = geo || M.createGeo(null); P = P || ID;
    const out = [];
    for (const it of items || []) {
      if (!it || it.guide) continue;
      const W = M.mul(P, isMat(it.matrix) ? it.matrix : geo.matrix(it));
      if (it.kind === 'group') out.push(...cutOutlines(it.children, M, geo, W));
      else if (it.role === 'cut' && it.kind !== 'text' && Array.isArray(it.subpaths) && it.subpaths.length) out.push(M.transformSubpaths(it.subpaths, W));
    }
    return out;
  }
  /** The box of a face's cut (the design's size), or null when it has none. */
  function cutBox(items, M) {
    M = M || model(); let b = null;
    for (const sp of cutOutlines(items, M)) b = M.union(b, M.bboxOf(sp));
    return b;
  }
  /** The back a design opens with: the front's cut lines seen from behind (mirrored about the frame's middle), locked, marked `guide` (never saved), under whatever the back already holds. */
  function backFace(front, backItems, M) {
    M = M || model();
    const f = front.frame, cx = f.x0 + f.wPt / 2, mir = [-1, 0, 0, 1, 2 * cx, 0], cuts = cutOutlines(front.items, M);
    const areas = cuts.map(sp => { const b = M.bboxOf(sp); return b ? (b[2] - b[0]) * (b[3] - b[1]) : 0; }), big = areas.indexOf(Math.max(...areas, 0));
    const guide = cuts.map((sp, i) => ({ id: 'guide-' + i, kind: 'path', role: 'cut', source: 'guide', guide: true, locked: true, name: (i === big ? 'Outline' : 'Cut line') + ' · seen from the back',
      subpaths: M.transformSubpaths(sp, mir), stroke: [0, 0, 0], fill: null, lwPt: LW_CUT_PT, tx: 0, ty: 0, sx: 1, sy: 1, rot: 0 }));
    return { face: 'back', unitPt: true, frame: { x0: f.x0, y0: f.y0, wPt: f.wPt, hPt: f.hPt }, items: guide.concat((backItems || []).filter(it => it && !it.guide)) };
  }

  /* ═══ 3 · the design made from what the editor hands back ═══ */
  const faceOut = (f, items) => ({ face: f.face, unitPt: true, frame: { x0: f.frame.x0, y0: f.frame.y0, wPt: f.frame.wPt, hPt: f.frame.hPt }, items: items || f.items || [] });
  /** The design to save (CharmNestDesigns.put) from the editor's result: the front as edited, the back only when the person put something on it (the guide left out), the size of the cut
   *  as drawn (the frame's when there is no cut line yet: noCut), where it came from, who. */
  function designOf(res, start, by, M) {
    M = M || model();
    const F = (res && res.faces) || {}, front = F.front || start.faces.front;
    const faces = { front: faceOut(front) };
    const back = F.back ? (F.back.items || []).filter(it => it && !it.guide) : [];
    if (back.length) faces.back = faceOut(F.back, back);
    const box = cutBox(faces.front.items, M), fr = faces.front.frame;
    const wPt = box ? box[2] - box[0] : fr.wPt, hPt = box ? box[3] - box[1] : fr.hPt;
    const d = { name: start.name, faces, widthMm: r3(wPt * MM), heightMm: r3(hPt * MM), source: Object.assign({}, start.source), by: by || '' };
    if (start.id) d.id = start.id;
    if (start.orderId) d.orderId = String(start.orderId);
    if (start.customName) d.customName = String(start.customName);
    if (!box) d.noCut = true;
    return d;
  }
  const changed = (res, M) => { M = M || model(); return Object.values((res && res.changes) || {}).reduce((n, d) => n + (d ? M.changeCount(d) : 0), 0); };

  /* ═══ 4 · the search over the library the page holds ═══ */
  const norm = s => String(s == null ? '' : s).toUpperCase().replace(/[^A-Z0-9]+/g, ' ').trim();
  const fileWord = f => String(f || '').replace(/\.(ai|pdf|svg|dxf)$/i, '');
  /** A design file the entry holds: its own, or (a design drawn in sizes) its first size's. */
  const sizesOf = e => Object.keys((e && e.sizes) || {}).filter(k => e.sizes[k] && e.sizes[k].aiPath);
  const geomOf = (e, size) => { const ks = sizesOf(e); return ks.length ? e.sizes[size && ks.includes(size) ? size : ks[0]] : e; };
  const hasDrawing = e => !!(e && e.sku && (e.aiPath || sizesOf(e).length));
  let IX = { map: null, size: -1, rows: [] };
  function indexOf(entries) {
    if (!entries) return [];
    if (IX.map === entries && IX.size === entries.size) return IX.rows;
    const rows = [];
    for (const e of entries.values()) {
      if (!hasDrawing(e)) continue;
      const s = norm(e.sku);
      rows.push({ e, s, k: ' ' + s + ' ' + norm(fileWord(e.masterName)) + ' ', c: s.replace(/ /g, '') });
    }
    let inOrder = true; for (let i = 1; i < rows.length && inOrder; i++) if (rows[i - 1].e.sku > rows[i].e.sku) inOrder = false;   // (the library comes A to Z: no sort then)
    if (!inOrder) rows.sort((a, b) => (a.e.sku < b.e.sku ? -1 : a.e.sku > b.e.sku ? 1 : 0));
    IX = { map: entries, size: entries.size, rows };
    return rows;
  }
  /** The rows that match `q`, best first (see the header), at most `limit`. */
  function rank(rows, q, limit, recent) {
    limit = limit > 0 ? limit : 60;
    const qn = norm(q);
    if (!qn) {
      const first = (recent || []).map(sku => rows.find(r => r.e.sku === sku)).filter(Boolean), seen = new Set(first);
      return first.concat(rows.filter(r => !seen.has(r)).slice(0, limit)).slice(0, limit);
    }
    const toks = qn.split(' '), qc = qn.replace(/ /g, ''), b0 = [], b1 = [], b2 = [], b3 = [];
    for (const r of rows) {
      let ok = true; for (const t of toks) if (!r.k.includes(t)) { ok = false; break; }
      if (!ok && !(qc.length > 2 && r.c.includes(qc))) continue;
      if (r.s === qn || r.c === qc) b0.push(r);
      else if (r.s.startsWith(qn) || r.c.startsWith(qc)) b1.push(r);
      else if (r.k.includes(' ' + toks[0])) b2.push(r);
      else b3.push(r);
    }
    return b0.concat(b1, b2, b3).slice(0, limit);
  }
  const padPt = () => { const O = root && root.CharmNestOrders; return O && fin(O.SIZE_PAD_PT) ? O.SIZE_PAD_PT : 2.6; };
  const cors = u => { const A = root && root.CharmNestAssets; try { return u && A && typeof A.url === 'function' ? A.url(u) : u || ''; } catch (_) { return u || ''; } };
  const thumbOf = e => { const Ms = root && root.Master; return (Ms && typeof Ms.thumbOf === 'function' ? Ms.thumbOf(e) : e.thumbUrl || ((Object.values(e.sizes || {}).find(s => s && s.thumbUrl) || {}).thumbUrl)) || ''; };
  function rowOut(e) {
    const g = geomOf(e) || e, pad = padPt(), w = +g.widthPt, h = +g.heightPt;
    return { sku: e.sku, name: e.sku, thumbUrl: cors(thumbOf(e)), widthMm: w > pad ? r1((w - pad) * MM) : null, heightMm: h > pad ? r1((h - pad) * MM) : null, sizes: sizesOf(e), file: e.masterName || '' };
  }
  const entries = () => { const B = root && root.B; return B && B.master && B.master.entries instanceof Map ? B.master.entries : null; };
  const RECENT_KEY = 'cem.recent';
  const recent = () => { try { const a = JSON.parse((root.localStorage && root.localStorage.getItem(RECENT_KEY)) || '[]'); return Array.isArray(a) ? a.filter(x => typeof x === 'string').slice(0, 12) : []; } catch (_) { return []; } };
  const remember = sku => { try { if (root.localStorage) root.localStorage.setItem(RECENT_KEY, JSON.stringify([sku].concat(recent().filter(x => x !== sku)).slice(0, 12))); } catch (_) { /* a per-computer convenience */ } };
  function search(q, o) {
    o = o || {};
    const ents = entries();
    if (!ents || !ents.size) { load(); return []; }
    return rank(indexOf(ents), q, o.limit, o.recent === false ? null : recent()).map(r => rowOut(r.e));
  }
  const count = () => indexOf(entries()).length;
  /* the library read once when the page holds none (Master.load: the copy on this computer first, the cloud's signatures, what changed): never per keystroke */
  let loading = null, loadError = '', tried = false;
  const listeners = new Set();
  const changedLib = () => { for (const fn of [...listeners]) { try { fn(); } catch (e) { console.warn('masters: a listener', e); } } };
  function load(again) {
    const ents = entries(), Ms = root && root.Master;
    if (ents && ents.size) return Promise.resolve(true);
    if (loading) return loading;
    if (!Ms || typeof Ms.load !== 'function' || (tried && !again)) return Promise.resolve(false);   // (asked once by itself; a person's "Try again" asks again)
    tried = true;
    loading = Promise.resolve().then(() => Ms.load(again ? { force: true } : undefined)).then(() => { loadError = ''; }, e => { loadError = (e && e.message) || String(e); })
      .then(() => { loading = null; changedLib(); const n = entries(); return !!(n && n.size); });
    return loading;
  }

  /* ═══ 5 · the page: the editor in design mode, the save, what comes next ═══ */
  const log = (kind, data) => { try { if (root.CNDesignLog && typeof root.CNDesignLog.event === 'function') root.CNDesignLog.event(kind, data); } catch (e) { console.warn('masters: log', e); } };
  async function who(action) {
    const E = root && root.CNEmployee; if (!E) return '';
    const now = typeof E.name === 'function' ? E.name() : ''; if (now) return now;
    return typeof E.need === 'function' ? (await E.need('Kept with the designs you save.', { action })) || '' : '';
  }

  /* the look: the editor's (charm-nest-charm-editor.js, after the website's studio): its tokens, its serif title, its square buttons, the gold of its Save */
  const CSS = `
.cem-root{ --ink:#1c1d1d; --paper:#fff; --band:#faf9f7; --hair:#e6e4e0; --muted:#6d6a66; --gold:#a58a52; --gold-lt:#cdb98a; --hov:#f2eee4; --hov-line:#c6bfaf; --press:#e6dfcd;
  --sans:'Nunito Sans','Helvetica Neue',Helvetica,Arial,sans-serif; --serif:'Cormorant','Hoefler Text',Georgia,serif; --in:.11s cubic-bezier(.22,.9,.28,1);
  position:fixed; inset:0; z-index:2147482550; background:rgba(28,29,29,.36); display:flex; align-items:center; justify-content:center; padding:14px 16px;
  font-family:var(--sans); font-size:13px; line-height:1.4; color:var(--ink); -webkit-font-smoothing:antialiased; text-align:left; }
.cem-root *,.cem-root *::before,.cem-root *::after{ box-sizing:border-box; }
.cem-root button{ font-family:inherit; border-radius:0; margin:0; box-shadow:none; text-transform:none; letter-spacing:normal; line-height:1.2; min-width:0; min-height:0; }
.cem-root input{ border-radius:0; box-shadow:none; margin:0; font-family:inherit; }
.cem-root [hidden]{ display:none !important; }
.cem-root :focus-visible{ outline:2px solid var(--gold); outline-offset:1px; }
.cem-win{ width:min(1000px,100%); height:min(740px,100%); background:#fff; display:grid; grid-template-rows:auto minmax(0,1fr) auto; padding:12px 18px 12px; box-shadow:0 30px 80px rgba(28,29,29,.30); outline:none; }
.cem-head{ display:flex; align-items:center; gap:12px; min-width:0; padding:0 0 10px; margin-bottom:10px; border-bottom:1px solid var(--hair); }
.cem-id{ flex:1 1 auto; min-width:0; display:flex; align-items:baseline; gap:14px; white-space:nowrap; overflow:hidden; }
.cem-id h2{ font-family:var(--serif); font-weight:500; font-size:24px; line-height:1.1; margin:0; letter-spacing:.01em; color:var(--ink); flex:none; }
.cem-eyebrow{ font-size:10px; font-weight:700; letter-spacing:.2em; text-transform:uppercase; color:#8a8784; overflow:hidden; text-overflow:ellipsis; min-width:0; }
.cem-seg{ display:inline-flex; flex:none; border:1px solid var(--hair); height:32px; }
.cem-seg button{ border:0; background:#fff; padding:0 13px; cursor:pointer; font-size:9.5px; font-weight:700; letter-spacing:.14em; text-transform:uppercase; color:var(--muted); }
.cem-seg button + button{ border-left:1px solid var(--hair); }
.cem-seg button.is-on{ background:var(--ink); color:#fff; }
.cem-seg button:not(.is-on):hover{ background:var(--hov); color:var(--ink); }
.cem-x{ flex:none; width:32px; height:32px; border:1px solid transparent; background:#fff; cursor:pointer; font-size:20px; color:var(--muted); }
.cem-x:hover{ background:var(--hov); border-color:var(--hov-line); color:var(--ink); }
.cem-pane{ min-height:0; display:flex; flex-direction:column; }
.cem-search{ flex:none; display:flex; align-items:center; gap:9px; height:40px; padding:0 12px; border:1px solid var(--hair); background:#fff; margin-bottom:10px; }
.cem-search:focus-within{ border-color:var(--ink); }
.cem-search svg{ width:17px; height:17px; flex:none; color:var(--muted); }
.cem-search input{ flex:1 1 auto; min-width:0; border:0; outline:none; background:none; font-size:14px; color:var(--ink); height:100%; padding:0; }
.cem-count{ flex:none; font-size:10px; font-weight:700; letter-spacing:.12em; text-transform:uppercase; color:var(--muted); white-space:nowrap; font-variant-numeric:tabular-nums; }
.cem-grid{ flex:1 1 auto; min-height:0; overflow-y:auto; display:grid; grid-template-columns:repeat(auto-fill,minmax(128px,1fr)); grid-auto-rows:min-content; gap:8px; padding:1px 2px 8px; align-content:start; overscroll-behavior:contain; }
.cem-tile{ position:relative; display:flex; flex-direction:column; align-items:stretch; gap:6px; padding:7px; border:1px solid var(--hair); background:#fff; cursor:pointer; text-align:left; transition:background var(--in), border-color var(--in); }
.cem-tile:hover,.cem-tile:focus-visible{ background:var(--hov); border-color:var(--hov-line); outline:none; }
.cem-tile:hover::before,.cem-tile:focus-visible::before{ content:""; position:absolute; left:-1px; top:-1px; bottom:-1px; width:2px; background:var(--gold); }
.cem-tile:active{ background:var(--press); }
.cem-tile[aria-busy=true]{ cursor:progress; opacity:.7; }
.cem-pic{ display:flex; align-items:center; justify-content:center; aspect-ratio:1; background:#fff; border:1px solid var(--hair); overflow:hidden; font-family:var(--serif); font-size:26px; color:#c9c3b8; }
.cem-pic img{ width:100%; height:100%; object-fit:contain; display:block; }
.cem-tile b{ display:block; font-size:11.5px; font-weight:700; letter-spacing:.02em; color:var(--ink); overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
.cem-tile i{ display:block; font-style:normal; font-size:10.5px; color:var(--muted); overflow:hidden; text-overflow:ellipsis; white-space:nowrap; font-variant-numeric:tabular-nums; }
.cem-szs{ display:flex; flex-wrap:wrap; gap:3px; }
.cem-sz{ border:1px solid var(--hair); background:#fff; padding:2px 6px; font-size:9.5px; font-weight:700; letter-spacing:.06em; color:var(--ink); cursor:pointer; }
.cem-sz:hover{ border-color:var(--ink); }
.cem-tile__why{ font-size:10.5px; line-height:1.4; color:#8a3a26; white-space:normal; }
.cem-empty{ grid-column:1/-1; padding:40px 12px; text-align:center; font-size:12.5px; line-height:1.7; color:var(--muted); }
.cem-more{ flex:none; display:flex; justify-content:center; padding-top:6px; }
.cem-new{ flex:1 1 auto; min-height:0; overflow-y:auto; display:grid; grid-template-columns:minmax(0,1.15fr) minmax(0,1fr); gap:22px; align-content:start; }
.cem-label{ font-size:9.5px; font-weight:700; letter-spacing:.18em; text-transform:uppercase; color:var(--muted); margin:2px 0 8px; }
.cem-label + .cem-label{ margin-top:0; }
.cem-shapes{ display:grid; grid-template-columns:repeat(5,minmax(0,1fr)); gap:6px; margin-bottom:18px; }
.cem-shape{ display:flex; flex-direction:column; align-items:center; gap:6px; padding:12px 4px 9px; border:1px solid var(--hair); background:#fff; cursor:pointer; color:var(--muted); font-size:10.5px; font-weight:600; text-align:center; line-height:1.25; }
.cem-shape svg{ width:30px; height:30px; }
.cem-shape:not(.is-on):hover{ background:var(--hov); color:var(--ink); border-color:var(--hov-line); }
.cem-shape.is-on{ border-color:var(--ink); background:var(--band); color:var(--ink); box-shadow:inset 0 -2px 0 0 var(--gold); }
.cem-sizes{ display:flex; align-items:stretch; height:34px; border:1px solid var(--hair); width:max-content; margin-bottom:8px; }
.cem-f{ display:inline-flex; align-items:center; gap:7px; padding:0 10px; cursor:text; }
.cem-f + .cem-f{ border-left:1px solid var(--hair); }
.cem-f > span{ font-size:12px; font-weight:800; letter-spacing:.06em; color:var(--ink); opacity:.62; }
.cem-f input{ width:52px; border:0; outline:none; background:none; font-size:13px; font-weight:600; color:var(--ink); text-align:right; padding:0; -moz-appearance:textfield; }
.cem-f input:disabled{ color:var(--muted); }
.cem-f:focus-within{ background:var(--band); }
.cem-u{ padding:0 10px 0 2px; font-style:normal; font-size:10px; font-weight:800; letter-spacing:.1em; text-transform:uppercase; color:var(--muted); align-self:center; }
.cem-chips{ display:flex; flex-wrap:wrap; gap:5px; margin-bottom:18px; }
.cem-chip{ height:26px; padding:0 10px; border:1px solid var(--hair); background:#fff; cursor:pointer; font-size:10.5px; font-weight:700; letter-spacing:.06em; color:var(--ink); font-variant-numeric:tabular-nums; }
.cem-chip:hover{ background:var(--hov); border-color:var(--hov-line); }
.cem-chip.is-on{ border-color:var(--ink); background:var(--band); }
.cem-hole{ display:flex; align-items:center; gap:12px; flex-wrap:wrap; margin-bottom:18px; }
.cem-tog{ display:inline-flex; align-items:center; gap:9px; cursor:pointer; font-size:12.5px; color:var(--ink); }
.cem-tog input{ width:16px; height:16px; accent-color:var(--ink); margin:0; }
.cem-hole .cem-sizes{ margin:0; height:30px; }
.cem-name{ display:flex; align-items:center; height:36px; border:1px solid var(--hair); padding:0 11px; }
.cem-name:focus-within{ border-color:var(--ink); }
.cem-name input{ flex:1; border:0; outline:none; background:none; font-size:13.5px; color:var(--ink); padding:0; }
.cem-prev{ display:flex; flex-direction:column; align-items:stretch; gap:8px; }
.cem-prev__box{ position:relative; aspect-ratio:1; background:var(--band); border:1px solid var(--hair); display:flex; align-items:center; justify-content:center; }
.cem-prev__box svg{ width:82%; height:82%; overflow:visible; }
.cem-prev__box .o{ fill:#fff; stroke:var(--ink); stroke-width:1.3; vector-effect:non-scaling-stroke; stroke-linejoin:round; }
.cem-prev__box .hl{ fill:var(--band); stroke:var(--ink); stroke-width:1.3; vector-effect:non-scaling-stroke; }
.cem-prev__box .fr{ fill:none; stroke:var(--gold); stroke-width:1; stroke-dasharray:5 4; vector-effect:non-scaling-stroke; }
.cem-prev__box .dm{ stroke:var(--gold); stroke-width:1; vector-effect:non-scaling-stroke; }
.cem-prev__dims{ font-size:11px; font-weight:700; letter-spacing:.12em; text-transform:uppercase; color:var(--muted); text-align:center; font-variant-numeric:tabular-nums; }
.cem-why{ font-size:11.5px; line-height:1.5; color:#8a3a26; min-height:1.5em; }
.cem-foot{ display:flex; align-items:center; gap:14px; padding:10px 0 0; margin-top:8px; border-top:1px solid var(--hair); min-width:0; }
.cem-hint{ flex:1 1 0; min-width:0; font-size:10px; letter-spacing:.12em; text-transform:uppercase; color:var(--muted); white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
.cem-actions{ display:flex; gap:10px; flex:none; margin-left:auto; }
.cem-btn{ display:inline-flex; align-items:center; justify-content:center; gap:8px; border:1px solid var(--ink); background:var(--ink); color:#fff; cursor:pointer; font-weight:700; font-size:11.5px; text-transform:uppercase !important; letter-spacing:.24em !important; padding:11px 22px; transition:background var(--in), border-color var(--in), color var(--in); }
.cem-btn:active{ transform:translateY(1px); }
.cem-btn:disabled{ opacity:.4; cursor:default; transform:none; }
.cem-btn--ghost{ background:transparent; border-color:transparent; color:var(--muted); padding:11px 12px; }
.cem-btn--ghost:hover,.cem-btn--ghost:focus-visible{ background:var(--hov); border-color:var(--hov-line); color:var(--ink); }
.cem-btn--gold{ background:var(--gold); border-color:var(--gold); }
.cem-btn--gold:not(:disabled):hover{ background:#a2854b; border-color:#a2854b; }
.cem-btn--ink:not(:disabled):hover{ background:#33342f; border-color:#33342f; }
.cem-btn--sm{ padding:8px 14px; font-size:10.5px; letter-spacing:.18em !important; }
.cem-root.cem-root--card{ z-index:2147482450; background:rgba(28,29,29,.30); }
.cem-card{ width:min(440px,100%); background:#fff; border:1px solid var(--ink); box-shadow:0 18px 44px rgba(28,29,29,.22); padding:22px 22px 18px; outline:none; }
.cem-card h3{ margin:0 0 8px; font-family:var(--serif); font-weight:500; font-size:24px; line-height:1.15; color:var(--ink); }
.cem-card p{ margin:0 0 6px; font-size:12.5px; line-height:1.6; color:var(--muted); }
.cem-card p b{ color:var(--ink); }
.cem-card p.is-bad{ color:#8a3a26; }
.cem-card__b{ display:flex; justify-content:flex-end; flex-wrap:wrap; gap:8px; margin-top:16px; }
.cem-note{ position:fixed; left:50%; bottom:22px; transform:translateX(-50%); z-index:2147482600; max-width:min(560px,calc(100vw - 32px)); padding:9px 16px; background:var(--ink,#1c1d1d); color:#fff;
  font:600 12px/1.45 'Nunito Sans','Helvetica Neue',Helvetica,Arial,sans-serif; letter-spacing:.02em; box-shadow:0 14px 34px rgba(28,29,29,.25); pointer-events:auto; cursor:default; }
.cem-note.is-bad{ background:#8a3a26; cursor:pointer; }
@media (max-width:720px){
  .cem-root{ padding:0; }
  .cem-win{ width:100%; height:100%; padding:10px 10px; }
  .cem-head{ flex-wrap:wrap; gap:8px; }
  .cem-id{ flex-basis:100%; flex-direction:column; gap:2px; order:-1; }
  .cem-id h2{ font-size:20px; }
  .cem-new{ grid-template-columns:minmax(0,1fr); }
  .cem-prev{ order:-1; } .cem-prev__box{ aspect-ratio:auto; height:180px; }
  .cem-shapes{ grid-template-columns:repeat(3,minmax(0,1fr)); }
  .cem-grid{ grid-template-columns:repeat(auto-fill,minmax(104px,1fr)); }
  .cem-hint{ display:none; }
}
@media (prefers-reduced-motion: reduce){ .cem-tile,.cem-btn{ transition:none; } }
`;
  function injectCss() {
    if (!doc || doc.getElementById('cemCss')) return;
    const s = doc.createElement('style'); s.id = 'cemCss'; s.textContent = CSS; (doc.head || doc.documentElement).appendChild(s);
  }
  /* the icons, drawn in the editor's hand (24-unit box, 1.6 stroke, round ends) */
  const SV = d => `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${d}</svg>`;
  const dOf = (sps, k, ox, oy) => { const f = v => Math.round(v * 100) / 100; let d = ''; for (const sp of sps || []) for (const o of sp) { if (o[0] === 'h') d += 'Z'; else d += o[0].toUpperCase() + o.slice(1).map(p => f(ox + p[0] * k) + ' ' + f(oy + p[1] * k)).join(' '); } return d; };
  const IC = {
    search: SV('<circle cx="10.5" cy="10.5" r="6.2"/><path d="m15.2 15.2 5 5"/>'),
    circle: SV('<circle cx="12" cy="12" r="8.5"/><circle cx="12" cy="6.3" r="1.2"/>'),
    rounded: SV('<rect x="4.5" y="3.5" width="15" height="17" rx="3"/><circle cx="12" cy="7" r="1.2"/>'),
    heart: SV(`<path d="${dOf(heartSP(17, 15.5), 1, 3.5, 4.5)}"/><circle cx="12" cy="10.2" r="1.2"/>`),
    tag: SV('<path d="M8.5 3.5h7l4 4V19a1.5 1.5 0 0 1-1.5 1.5H6A1.5 1.5 0 0 1 4.5 19V7.5Z"/><circle cx="12" cy="7" r="1.2"/>'),
    free: SV('<path d="M12 20.5 6 13.8l2.2-7L12 4l3.8 2.8 2.2 7Z"/><circle cx="12" cy="11.6" r="1.5"/><path d="M12 20.5v-7.4"/>')
  };

  /* ── a short line on the page while a master is read (the library panel inside the editor has no window of its own to say it in) ── */
  let noteEl = null, noteTimer = 0;
  function note(text, bad) {
    if (!doc) return;
    clearTimeout(noteTimer);
    if (!text) { if (noteEl) { noteEl.remove(); noteEl = null; } return; }
    injectCss();
    if (!noteEl) { noteEl = doc.createElement('div'); noteEl.className = 'cem-note'; noteEl.setAttribute('role', 'status'); noteEl.addEventListener('click', () => { if (noteEl && noteEl.classList.contains('is-bad')) note(''); }); }
    noteEl.textContent = text; noteEl.classList.toggle('is-bad', !!bad); noteEl.title = bad ? 'Click to put this away' : '';
    if (!noteEl.isConnected) doc.body.appendChild(noteEl);
    if (bad) noteTimer = setTimeout(() => note(''), 9000);
  }

  /** The editor that is open now closed through its own Cancel (it asks before throwing changes away). → true when it is gone, false when the person kept editing. */
  function closeEditor() {
    const E = root && root.CharmEditor;
    if (!E || !E.current) return Promise.resolve(true);
    const rootEl = doc.querySelector('.ce-root'), btn = rootEl && rootEl.querySelector('[data-act="cancel"]');
    if (!btn) return Promise.resolve(!E.current);
    return new Promise(res => {
      let asked = false, over = false;
      const end = v => { if (over) return; over = true; mo.disconnect(); res(v); };
      const look = () => {
        if (!E.current || !rootEl.isConnected) { end(true); return; }
        if (rootEl.querySelector('.ce-ask')) asked = true;
        else if (asked) setTimeout(() => end(!E.current), 0);   // (the question answered: "Discard" closes the editor within the same task, "Keep editing" leaves it open)
      };
      const mo = new MutationObserver(look);
      mo.observe(doc.body, { childList: true }); mo.observe(rootEl, { childList: true, subtree: true });
      btn.click(); look();
    });
  }

  let session = null;      // the design in the editor now, or on its way there: { token, start }
  /** THE DESIGN-MODE OPEN: the editor on `start` ({ name, title, subtitle, faces, source, id?, version?, orderId?, customName?, log }), its Save made a version, then what is next.
   *  The fonts and their outlines are the bridge's (window.CharmEditDesign: the engraving fonts, as a piece's open has them); nothing is locked. An editor already open is closed first
   *  (its own Cancel, which asks about changes); "Keep editing" there, or a later open while the fonts load, and this one gives way (null). */
  async function edit(start, by) {
    const E = root && root.CharmEditor;
    if (!E || typeof E.open !== 'function') { note('The charm editor did not load: reload the page', true); return null; }
    const token = {}; session = { token, start };
    const mine = () => !!session && session.token === token, drop = () => { if (mine()) session = null; };
    if (!(await closeEditor())) { drop(); note(''); return null; }   // (the person kept the charm they were editing)
    const host = root.CharmEditDesign;
    let fonts = null;
    if (host && typeof host.fonts === 'function') { note('Opening the editor…'); try { fonts = await host.fonts(); } catch (e) { console.warn('masters: fonts', e); } }
    if (!mine() || E.current) { drop(); return null; }   // (another design was asked for meanwhile, or an editor opened under us)
    note('');
    const opts = Object.assign({ title: start.title || start.name, subtitle: start.subtitle || '', faces: start.faces, startFace: 'front', lockRoles: [] },
      Array.isArray(fonts) && fonts.length ? { fonts } : {}, host && typeof host.textToPaths === 'function' ? { textToPaths: host.textToPaths } : {});
    let shown;
    try { shown = E.open(opts); }
    catch (e) { drop(); note(`The charm editor could not open · ${(e && e.message) || e}`, true); return null; }
    log('designOpen', Object.assign({ name: start.name, by }, start.log || {}, start.id ? { designId: start.id, version: start.version } : {}, start.orderId ? { orderId: start.orderId } : {}));
    let res;
    try { res = await shown; } catch (e) { drop(); note(`The charm editor could not open · ${(e && e.message) || e}`, true); return null; }
    if (!res) { drop(); return null; }   // (Cancel, the ×, Esc: nothing is saved; a master or a design picked in the library panel closes it this way too)
    try { return await saved(start, res, by, token); }
    finally { drop(); }
  }
  /** The editor's result saved as a version of the design (none when nothing changed since the version it was opened on), then the card: Send to sheet, Keep editing, Done. */
  async function saved(start, res, by, token) {
    const M = model(), design = designOf(res, start, by, M), D = root.CharmNestDesigns;
    const card = cardOpen();
    let got = null;
    if (start.id && start.version && !changed(res, M)) got = { id: start.id, version: start.version, same: true };
    else if (!D || typeof D.put !== 'function') { card.fail('The design library did not load on this page, so the design could not be kept.'); }
    else {
      card.saving(design);
      try { got = await D.put(design); if (!got || !got.id) throw new Error('the design library did not say it was kept'); }
      catch (e) { card.fail((e && e.message) || String(e)); got = null; }
    }
    if (!got) {
      const again = await card.choice();
      if (again !== 'again') return null;
      return edit(Object.assign({}, start, { faces: facesAgain(design) }), by);
    }
    if (!got.same) log('designSave', { designId: got.id, version: got.version, name: design.name, source: design.source, widthMm: design.widthMm, heightMm: design.heightMm, by, ...(design.orderId ? { orderId: design.orderId } : {}), changes: changed(res, M) });
    if (design.source && design.source.kind === 'master' && design.source.sku) remember(design.source.sku);
    card.done(design, got);
    const next = await card.choice();
    if (next === 'again') return edit(Object.assign({}, start, { id: got.id, version: got.version, faces: facesAgain(design), subtitle: subtitleFor(Object.assign({}, start, { id: got.id, version: got.version })) }), by);
    if (next === 'send') {
      const C = root.CharmNestCustomPiece;
      if (C && typeof C.open === 'function') { try { await C.open(Object.assign({ designId: got.id, version: got.version }, design.orderId ? { orderId: design.orderId } : {})); } catch (e) { note(`Send to sheet · ${(e && e.message) || e}`, true); } }
    }
    return { designId: got.id, version: got.version };
  }
  /** The faces a design opens with again: its front as saved, its back with the guide made afresh from that front. */
  const facesAgain = d => ({ front: d.faces.front, back: backFace(d.faces.front, d.faces.back ? d.faces.back.items : [], model()) });
  function subtitleFor(st) {
    const bits = [];
    if (st.id) bits.push(`Design · version ${st.version || 1}`); else bits.push('A new design');
    if (st.source && st.source.kind === 'master') bits.push(`from ${st.source.sku}${st.source.size ? ' · ' + st.source.size : ''} in the charm library (the master never changes)`);
    else if (st.source && st.source.kind === 'new') bits.push(st.source.shape === 'free' ? 'draw the outline, then make it a Cut line' : 'the outline is the cut');
    if (st.orderId) bits.push(`order ${st.orderId}`); else if (st.customName) bits.push(st.customName);
    return bits.join(' · ');
  }

  /* ── the card after Save ── */
  function cardOpen() {
    injectCss();
    const prev = doc.activeElement, wrap = doc.createElement('div');
    wrap.className = 'cem-root cem-root--card';
    wrap.innerHTML = '<div class="cem-card" role="alertdialog" aria-modal="true" aria-label="Design saved" tabindex="-1"><h3>Saving…</h3><p>The design is being kept in the design library.</p><div class="cem-card__b"></div></div>';
    doc.body.appendChild(wrap);
    const card = wrap.querySelector('.cem-card'), h = card.querySelector('h3'), bar = card.querySelector('.cem-card__b');
    let pick = null, picked = null;
    const choose = v => { if (picked) return; picked = v; wrap.remove(); root.removeEventListener('keydown', onKey, true); try { if (prev && prev.focus && prev.isConnected) prev.focus({ preventScroll: true }); } catch (_) { /* fine */ } if (pick) pick(v); };
    const onKey = e => { if (!wrap.isConnected) return; if (e.key === 'Escape' && bar.children.length) { e.preventDefault(); e.stopPropagation(); choose(bar.querySelector('[data-c="discard"]') ? 'discard' : 'done'); } };
    root.addEventListener('keydown', onKey, true);
    wrap.addEventListener('click', e => { const b = e.target.closest('[data-c]'); if (b && !b.disabled) choose(b.dataset.c); });
    const buttons = list => { bar.innerHTML = list.map(([c, cls, text, dis, title]) => `<button class="cem-btn ${cls}" type="button" data-c="${c}"${dis ? ' disabled' : ''}${title ? ` title="${esc(title)}"` : ''}>${esc(text)}</button>`).join(''); const last = bar.querySelector('button:not(:disabled):last-child') || bar.querySelector('button'); requestAnimationFrame(() => { try { (last || card).focus({ preventScroll: true }); } catch (_) { /* fine */ } }); };
    const para = (html, bad) => { card.querySelectorAll('p').forEach(p => p.remove()); h.insertAdjacentHTML('afterend', html.map(x => `<p${bad ? ' class="is-bad"' : ''}>${x}</p>`).join('')); };
    return {
      saving(d) { h.textContent = 'Saving…'; para([`<b>${esc(d.name)}</b> · ${mmTxt(d.widthMm)} × ${mmTxt(d.heightMm)} mm is being kept in the design library.`]); },
      fail(why) { h.textContent = 'Not saved'; para([esc(why), 'Nothing is lost: go back to the editor and save again.'], true); buttons([['discard', 'cem-btn--ghost', 'Discard'], ['again', 'cem-btn--ink', 'Back to the editor']]); },
      done(d, got) {
        const C = root.CharmNestCustomPiece, canSend = !!(C && typeof C.open === 'function') && !d.noCut;
        h.textContent = got.same ? `Version ${got.version} · nothing changed` : `Saved · version ${got.version}`;
        para([`<b>${esc(d.name)}</b> · ${mmTxt(d.widthMm)} × ${mmTxt(d.heightMm)} mm${d.orderId ? ` · order ${esc(d.orderId)}` : ''} is in the design library${d.by ? `, saved by ${esc(d.by)}` : ''}.${d.source && d.source.kind === 'master' ? ` The master ${esc(d.source.sku)} in the charm library is unchanged.` : ''}`]
          .concat(d.noCut ? ['It has no cut line yet: draw its outline and make it a Cut line before it can go on a sheet.'] : []));
        buttons([['done', 'cem-btn--ghost', 'Done'], ['again', 'cem-btn--ink', 'Keep editing'],
          ['send', 'cem-btn--gold', 'Send to sheet', !canSend, d.noCut ? 'A design goes on a sheet once it has its outline (a Cut line)' : !canSend ? 'Send to sheet comes with the custom piece window, which is not on this page yet' : 'Choose the metal and the order (or a name), and it goes on the next sheet of that metal']]);
      },
      choice() { return picked ? Promise.resolve(picked) : new Promise(r => { pick = r; }); }
    };
  }

  /* ── opening a master ── */
  /** The start of a design from the master `sku` (a size of a sized one): its drawing read through the pool's one copy per page. Throws with words a person can read. */
  async function masterStart(sku, size, o) {
    const Ms = root.Master, Pl = root.Pool, AE = root.CharmNestArtEdit, M = model();
    if (!Ms || !Pl || !AE || !M) throw new Error('the page has not finished loading: reload it');
    const entry = Ms.entryFor(sku) || await Ms.fetchEntry(sku);
    if (!entry) throw new Error('it is not in the charm library');
    const ks = sizesOf(entry), sz = size && ks.includes(size) ? size : ks[0] || null;
    const src = await Pl.masterCharm(entry, sz), charm = src && src.charms && src.charms[0];
    if (!charm || !Array.isArray(charm.members) || !charm.members.length || !charm.outline) throw new Error('its drawing could not be read');
    const front = masterFace(charm, AE, M);
    if (!front.items.length) throw new Error('its drawing has no part the editor can hold');
    const skipped = front.skipped; delete front.skipped;
    const st = { name: entry.sku, title: entry.sku, source: Object.assign({ kind: 'master', sku: entry.sku }, sz ? { size: sz } : {}), faces: { front, back: backFace(front, [], M) },
      log: { source: 'master', sku: entry.sku, size: sz, layers: front.items.length }, ...(o && o.orderId ? { orderId: String(o.orderId) } : {}), ...(o && o.customName ? { customName: String(o.customName) } : {}) };
    st.subtitle = subtitleFor(st) + (skipped ? ` · ${skipped} part${skipped === 1 ? ' is' : 's are'} live text or a picture in the master and not here` : '');
    return st;
  }
  let opening = null;      // { key, p }: the master being read, so a second press does not read it twice
  function open(sku, o) {
    o = o || {};
    const key = String(sku || '').trim().toUpperCase(); if (!key) return Promise.resolve(null);
    if (opening && opening.key === key) return opening.p;
    const me = { key, p: null }, done = () => { if (opening === me) opening = null; };
    opening = me;
    me.p = (async () => {
      const by = await who('open a charm from the library as a new design'); if (!by) { done(); return null; }
      note(`Opening ${key}…`);
      let st;
      try { st = await masterStart(key, o.size, o); }
      catch (e) { done(); note(`${key} could not be opened · ${(e && e.message) || e}`, true); if (o.throws) throw e; return null; }
      done();
      return edit(st, by);
    })();
    return me.p;
  }

  /* ── a new design from nothing ── */
  function newStart(spec, o) {
    const s = startSpec(spec), front = startFace(s), name = s.name || defaultName(s);
    const st = { name, title: name, source: { kind: 'new', shape: s.shape }, faces: { front, back: backFace(front, [], model()) },
      log: { source: 'new', shape: s.shape, widthMm: s.wMm, heightMm: s.hMm, holeMm: s.hole ? s.holeMm : 0 }, ...(o && o.orderId ? { orderId: String(o.orderId) } : {}), ...(o && o.customName ? { customName: String(o.customName) } : {}) };
    st.subtitle = subtitleFor(st) + ` · ${mmTxt(s.wMm)} × ${mmTxt(s.hMm)} mm${s.hole ? ` · jump ring hole Ø ${mmTxt(s.holeMm)} mm` : ''}`;
    return st;
  }
  /** A saved design opened again: its Save makes the next version of it. */
  async function openDesign(d, o) {
    o = o || {};
    if (!d || !d.faces || !d.faces.front) return null;
    const by = await who('edit a design'); if (!by) return null;
    const st = { id: d.id || null, version: d.version || null, name: d.name || 'Design', title: d.name || 'Design', source: Object.assign({}, d.source || { kind: 'new' }), faces: facesAgain(d),
      log: { source: 'design' }, ...(d.orderId || o.orderId ? { orderId: String(d.orderId || o.orderId) } : {}), ...(d.customName ? { customName: d.customName } : {}) };
    st.subtitle = subtitleFor(st);
    return edit(st, by);
  }

  /* ═══ 6 · the picker: the library's charms, or a new start ═══ */
  const START_KEY = 'cem.start';
  const lastStart = () => { try { return startSpec(JSON.parse((root.localStorage && root.localStorage.getItem(START_KEY)) || 'null') || {}); } catch (_) { return startSpec({}); } };
  const keepStart = s => { try { if (root.localStorage) root.localStorage.setItem(START_KEY, JSON.stringify({ shape: s.shape, wMm: s.wMm, hMm: s.hMm, hole: s.hole, holeMm: s.holeMm })); } catch (_) { /* a per-computer convenience */ } };
  let picker = null;
  function openPicker(o) { return showPicker(Object.assign({ pane: 'lib' }, o || {})); }
  function newDesign(o) { return showPicker(Object.assign({ pane: 'new' }, o || {})); }
  async function showPicker(o) {
    if (!doc) return null;
    if (picker) { picker.pane(o.pane); return picker.result; }
    const by = await who(o.pane === 'new' ? 'start a new charm design' : 'open a charm from the library as a new design'); if (!by) return null;
    if (picker) { picker.pane(o.pane); return picker.result; }
    injectCss();
    const prev = doc.activeElement, wrap = doc.createElement('div');
    wrap.className = 'cem-root';
    const spec = lastStart();
    const shapeBtns = SHAPES.map(s => `<button class="cem-shape" type="button" data-shape="${s}" aria-pressed="false">${IC[s]}<span>${esc(SHAPE_WORD[s])}</span></button>`).join('');
    const chips = [10, 12, 15, 18, 20, 25].map(v => `<button class="cem-chip" type="button" data-chip="${v}">${v}</button>`).join('');
    wrap.innerHTML = `
<div class="cem-win" role="dialog" aria-modal="true" aria-label="Design a charm" tabindex="-1">
  <header class="cem-head">
    <div class="cem-id"><h2>Design a charm</h2><span class="cem-eyebrow">From the charm library, or a new one · saved as a design</span></div>
    <div class="cem-seg" role="tablist" aria-label="Start from"><button type="button" data-pane="lib" role="tab">From the library</button><button type="button" data-pane="new" role="tab">New design</button></div>
    <button class="cem-x" type="button" data-act="close" aria-label="Close" title="Close (Esc)">×</button>
  </header>
  <section class="cem-pane" data-panel="lib">
    <label class="cem-search">${IC.search}<input type="search" data-cf="q" placeholder="Search the charm library: SKU or file name" autocomplete="off" spellcheck="false" aria-label="Search the charm library"><span class="cem-count" data-count></span></label>
    <div class="cem-grid" role="listbox" aria-label="Charms in the library"></div>
    <div class="cem-more" hidden><button class="cem-btn cem-btn--ghost cem-btn--sm" type="button" data-act="more">Show more</button></div>
  </section>
  <section class="cem-pane" data-panel="new" hidden>
    <div class="cem-new">
      <div>
        <div class="cem-label">Outline · the laser cuts along it</div>
        <div class="cem-shapes" role="radiogroup" aria-label="Outline">${shapeBtns}</div>
        <div class="cem-label">Size</div>
        <div class="cem-sizes"><label class="cem-f" title="Width, mm"><span data-wlabel>W</span><input data-cf="w" inputmode="decimal" autocomplete="off" aria-label="Width, mm"></label><label class="cem-f" data-hbox title="Height, mm"><span>H</span><input data-cf="h" inputmode="decimal" autocomplete="off" aria-label="Height, mm"></label><i class="cem-u">mm</i></div>
        <div class="cem-chips" aria-label="Sizes">${chips}</div>
        <div class="cem-label">Jump ring</div>
        <div class="cem-hole"><label class="cem-tog"><input type="checkbox" data-cf="hole"> A hole for the jump ring</label><div class="cem-sizes"><label class="cem-f" title="The hole's diameter, mm"><span>Ø</span><input data-cf="dia" inputmode="decimal" autocomplete="off" aria-label="Hole diameter, mm"></label><i class="cem-u">mm</i></div></div>
        <div class="cem-label">Name</div>
        <label class="cem-name"><input data-cf="name" maxlength="80" autocomplete="off" spellcheck="false" aria-label="Name of the design"></label>
      </div>
      <div class="cem-prev"><div class="cem-prev__box" aria-hidden="true"><svg data-prev></svg></div><div class="cem-prev__dims" data-dims></div><div class="cem-why" data-why role="alert"></div></div>
    </div>
  </section>
  <footer class="cem-foot">
    <span class="cem-hint" data-hint></span>
    <div class="cem-actions"><button class="cem-btn cem-btn--ghost" type="button" data-act="close">Cancel</button><button class="cem-btn cem-btn--gold" type="button" data-act="start" hidden>Start designing</button></div>
  </footer>
</div>`;
    doc.body.appendChild(wrap);
    const $ = s => wrap.querySelector(s), $$ = s => Array.from(wrap.querySelectorAll(s));
    const grid = $('.cem-grid'), q = $('[data-cf="q"]'), cnt = $('[data-count]'), more = $('.cem-more'), hint = $('[data-hint]'), startB = $('[data-act="start"]');
    const fW = $('[data-cf="w"]'), fH = $('[data-cf="h"]'), fHole = $('[data-cf="hole"]'), fDia = $('[data-cf="dia"]'), fName = $('[data-cf="name"]'), prevSvg = $('[data-prev]'), dims = $('[data-dims]'), why = $('[data-why]');
    let pane = 'lib', limit = 60, shown = [], busy = false, resolve = null, picMount = null;
    const result = new Promise(r => { resolve = r; });
    const close = v => {
      if (!wrap.isConnected) return;
      wrap.remove(); root.removeEventListener('keydown', onKey, true); listeners.delete(onLib);
      try { if (picMount) picMount.stop(); } catch (_) { /* fine */ }
      if (picker && picker.wrap === wrap) picker = null;
      if (v == null) { try { if (prev && prev.focus && prev.isConnected) prev.focus({ preventScroll: true }); } catch (_) { /* fine */ } resolve(null); }
      else v.then(resolve, () => resolve(null));
    };

    /* the library side: what the page holds, searched in memory at every keystroke */
    function tile(r) {
      const Ms = root.Master, e = entries() && entries().get(r.sku);
      const pic = r.thumbUrl && Ms && typeof Ms.pictureTag === 'function' && e ? Ms.pictureTag(e) : r.thumbUrl ? `<img alt="" crossorigin="anonymous" src="${esc(r.thumbUrl)}">` : '';
      const size = r.widthMm && r.heightMm ? `${mmTxt(r.widthMm)} × ${mmTxt(r.heightMm)} mm` : '';
      const szs = r.sizes.length > 1 ? `<span class="cem-szs">${r.sizes.map(s => `<span class="cem-sz" data-size="${esc(s)}" role="button" title="Open it at ${esc(s)}">${esc(s)}</span>`).join('')}</span>` : '';
      return `<button class="cem-tile" type="button" role="option" data-sku="${esc(r.sku)}" title="Open ${esc(r.sku)} as a new design${r.file ? ` · from ${esc(r.file)}` : ''}">`
        + `<span class="cem-pic"${pic ? '' : ` data-cem-preview="${esc(r.sku)}"`}>${pic || esc(r.sku.replace(/[^A-Z0-9]/gi, '').slice(0, 2))}</span><b>${esc(r.sku)}</b><i>${esc(size || r.file || ' ')}${r.sizes.length > 1 ? ` · ${r.sizes.length} sizes` : ''}</i>${szs}</button>`;
    }
    function renderLib() {
      const ents = entries(), total = ents && ents.size ? count() : 0, qs = q.value;
      if (!total) {
        grid.innerHTML = `<div class="cem-empty">${loadError ? `The charm library could not be read · ${esc(loadError)}<br><button class="cem-btn cem-btn--ghost cem-btn--sm" type="button" data-act="reload">Try again</button>` : loading ? 'The charm library is being read…' : root.Master ? 'The charm library is empty on this page.<br><button class="cem-btn cem-btn--ghost cem-btn--sm" type="button" data-act="reload">Read it again</button>' : 'There is no charm library on this page.'}</div>`;
        cnt.textContent = ''; more.hidden = true; mark(); if (!tried) { load(); renderLib(); } return;
      }
      shown = search(qs, { limit: limit + 1 });
      const extra = shown.length > limit; if (extra) shown = shown.slice(0, limit);
      grid.innerHTML = shown.length ? shown.map(tile).join('') : `<div class="cem-empty">No charm in the library matches “${esc(qs)}”.<br>Try fewer letters, or start a new design.</div>`;
      cnt.textContent = qs.trim() ? `${shown.length}${extra ? '+' : ''} of ${total.toLocaleString()}` : `${total.toLocaleString()} charms`;
      more.hidden = !extra;
      mark(); mountPics();
    }
    /* (the editor, when it is open under the picker, hears every key first: a picker element says the key is not the editor's, charm-nest-charm-editor.js onKey's __ceKey) */
    const NOT_MINE = () => {};
    const mark = () => { for (const el of wrap.querySelectorAll('input,button,[tabindex]')) el.__ceKey = NOT_MINE; };
    function mountPics() {
      const Ms = root.Master, T = root.CharmNestThumbs, Pl = root.Pool;
      try { if (Ms && typeof Ms.mountPictures === 'function') Ms.mountPictures(grid); } catch (_) { /* the pictures stay as they are */ }
      // a charm with no stored picture is drawn as the Master tab draws it (Pool.masterPreview, kept on this computer under the same key), only when it is on screen
      try { if (picMount) picMount.stop(); } catch (_) { /* fine */ } picMount = null;
      if (!T || typeof T.mount !== 'function' || !Pl || typeof Pl.masterPreview !== 'function' || !grid.querySelector('[data-cem-preview]')) return;
      const ent = h => entries() && entries().get(h.dataset.cemPreview), sizeOf = e => sizesOf(e)[0] || null;
      picMount = T.mount(grid, {
        selector: '[data-cem-preview]', concurrency: 3, keep: false,
        keyOf: h => { const e = ent(h), g = e && Pl.sizeEntry(e, sizeOf(e)); return g && g.aiPath && !(root.B && root.B.pool && root.B.pool.sources.has(g.aiPath)) ? T.designKey(g.aiPath, g.charmHash || e.charmHash, e.indexedAt) : ''; },
        produce: h => { const e = ent(h); return e ? Pl.masterPreview(e, sizeOf(e), false) : null; },
        paint: (h, url) => { const img = doc.createElement('img'); img.alt = ''; img.src = url; h.replaceChildren(img); },
        fail: () => { /* the letters stay */ }
      });
    }
    const onLib = () => { if (pane === 'lib' && wrap.isConnected) renderLib(); };
    listeners.add(onLib);
    async function pickMaster(t, size) {
      if (busy) return;
      const sku = t.dataset.sku; busy = true; t.setAttribute('aria-busy', 'true');
      const was = t.querySelector('i').textContent; t.querySelector('i').textContent = 'Opening…'; t.querySelector('.cem-tile__why')?.remove();
      hint.textContent = `Reading ${sku}…`;
      let st;
      try { st = await masterStart(sku, size, o); }
      catch (e) { busy = false; t.removeAttribute('aria-busy'); t.querySelector('i').textContent = was; t.insertAdjacentHTML('beforeend', `<span class="cem-tile__why">Could not be opened · ${esc((e && e.message) || e)}</span>`); hint.textContent = ''; return; }
      busy = false;
      close(edit(st, by));   // (an editor open under the picker is closed by edit(), through its own Cancel: the picker is gone before its question shows)
    }

    /* the new side: the start, drawn as it is set */
    let S = Object.assign({}, spec);
    function syncNew(from) {
      for (const b of $$('.cem-shape')) { const on = b.dataset.shape === S.shape; b.classList.toggle('is-on', on); b.setAttribute('aria-pressed', on ? 'true' : 'false'); }
      const circle = S.shape === 'circle';
      $('[data-wlabel]').textContent = circle ? 'Ø' : 'W'; $('[data-hbox]').hidden = circle;
      if (from !== 'w') fW.value = mmTxt(S.wMm); if (from !== 'h') fH.value = mmTxt(S.hMm); if (from !== 'dia') fDia.value = mmTxt(S.holeMm);
      fHole.checked = S.hole; fHole.disabled = S.shape === 'free'; fDia.disabled = !S.hole || S.shape === 'free';
      fName.placeholder = defaultName(S);
      for (const c of $$('.cem-chip')) c.classList.toggle('is-on', +c.dataset.chip === S.wMm && (circle || +c.dataset.chip === S.hMm));
      // the preview: the outline and the hole at their true proportions, the frame dashed for an outline drawn by hand
      const w = S.wMm * PT, h = S.hMm * PT, pad = Math.max(w, h) * 0.04, sp = outlineSP(S.shape, w, h);
      let svg = '';
      if (sp) svg += `<path class="o" d="${dOf(sp, 1, 0, 0)}"/>`; else svg += `<rect class="fr" x="0" y="0" width="${w.toFixed(2)}" height="${h.toFixed(2)}"/>`;
      if (sp && S.hole) { const hl = holeFor(S.shape, w, h, S.holeMm * PT / 2, S.wallMm * PT); svg += `<path class="hl" d="${dOf(ellipseSP(hl.cx, hl.cy, hl.r, hl.r), 1, 0, 0)}"/>`; }
      prevSvg.setAttribute('viewBox', `${(-pad).toFixed(2)} ${(-pad).toFixed(2)} ${(w + 2 * pad).toFixed(2)} ${(h + 2 * pad).toFixed(2)}`); prevSvg.innerHTML = svg;
      dims.textContent = `${circle ? `Ø ${mmTxt(S.wMm)} mm` : `${mmTxt(S.wMm)} × ${mmTxt(S.hMm)} mm`}${S.hole && S.shape !== 'free' ? ` · hole Ø ${mmTxt(S.holeMm)} mm` : ''}`;
      const w8 = startWhy(S); why.textContent = w8; startB.disabled = !!w8;
    }
    const numIn = v => { const n = parseFloat(String(v).replace(',', '.')); return Number.isFinite(n) ? n : NaN; };
    function fieldIn(el, commit) {
      const f = el.dataset.cf, v = numIn(el.value);
      if (!Number.isFinite(v) || v <= 0) { if (commit) syncNew(); return; }
      if (f === 'w') S = startSpec(Object.assign({}, S, { wMm: v, hMm: S.shape === 'circle' ? v : S.hMm }));
      else if (f === 'h') S = startSpec(Object.assign({}, S, { hMm: v }));
      else if (f === 'dia') S = startSpec(Object.assign({}, S, { holeMm: v }));
      syncNew(commit ? null : f);
    }
    function start() {
      if (startWhy(S)) return;
      const s = startSpec(Object.assign({}, S, { name: fName.value }));
      keepStart(s);
      close(edit(newStart(s, o), by));
    }

    function setPane(p) {
      pane = p === 'new' ? 'new' : 'lib';
      for (const b of $$('[data-pane]')) { const on = b.dataset.pane === pane; b.classList.toggle('is-on', on); b.setAttribute('aria-selected', on ? 'true' : 'false'); }
      $('[data-panel="lib"]').hidden = pane !== 'lib'; $('[data-panel="new"]').hidden = pane !== 'new'; startB.hidden = pane !== 'new';
      hint.textContent = pane === 'lib' ? 'Click a charm to open it in the editor as a new design · the master never changes' : 'The outline is the cut · change anything in the editor';
      if (pane === 'lib') { renderLib(); requestAnimationFrame(() => { try { q.focus({ preventScroll: true }); } catch (_) { /* fine */ } }); }
      else { syncNew(); requestAnimationFrame(() => { try { (wrap.querySelector('.cem-shape.is-on') || fW).focus({ preventScroll: true }); } catch (_) { /* fine */ } }); }
    }
    /* the keys stay in the picker (as the editor keeps its own): Esc closes, Enter opens the first charm, the arrows walk the tiles */
    function onKey(e) {
      if (!wrap.isConnected) return;
      e.stopPropagation();
      if (e.key === 'Escape') { e.preventDefault(); if (!busy) close(null); return; }
      const t = e.target;
      if (pane === 'lib' && t === q && (e.key === 'Enter' || e.key === 'ArrowDown')) { const first = grid.querySelector('.cem-tile'); if (first) { e.preventDefault(); if (e.key === 'Enter') pickMaster(first); else first.focus(); } return; }
      if (pane === 'lib' && t.classList && t.classList.contains('cem-tile') && /^Arrow/.test(e.key)) {
        const tiles = $$('.cem-tile'), i = tiles.indexOf(t), top = tiles[0].offsetTop, cols = Math.max(1, tiles.filter(x => x.offsetTop === top).length);
        const j = e.key === 'ArrowRight' ? i + 1 : e.key === 'ArrowLeft' ? i - 1 : e.key === 'ArrowDown' ? i + cols : i - cols;
        e.preventDefault(); if (j < 0) q.focus(); else if (tiles[j]) tiles[j].focus();
        return;
      }
      if (pane === 'new' && e.key === 'Enter' && t && t.tagName === 'INPUT') { e.preventDefault(); if (t.dataset.cf !== 'name') fieldIn(t, true); start(); }
    }
    root.addEventListener('keydown', onKey, true);
    wrap.addEventListener('click', e => {
      const t = e.target;
      if (t === wrap) { if (!busy) close(null); return; }
      const a = t.closest('[data-act]');
      if (a) { const k = a.dataset.act; if (k === 'close') { if (!busy) close(null); } else if (k === 'more') { limit += 120; renderLib(); } else if (k === 'start') start(); else if (k === 'reload') { load(true); renderLib(); } return; }
      const pb = t.closest('[data-pane]'); if (pb) { setPane(pb.dataset.pane); return; }
      const sz = t.closest('[data-size]'); if (sz) { const tl = sz.closest('.cem-tile'); if (tl) pickMaster(tl, sz.dataset.size); return; }
      const tl = t.closest('.cem-tile'); if (tl) { pickMaster(tl); return; }
      const sh = t.closest('[data-shape]'); if (sh) { S = startSpec(Object.assign({}, S, { shape: sh.dataset.shape, hMm: sh.dataset.shape === 'circle' ? S.wMm : S.hMm, hole: sh.dataset.shape === 'free' ? false : S.shape === 'free' ? true : S.hole })); syncNew(); return; }
      const ch = t.closest('[data-chip]'); if (ch) { const v = +ch.dataset.chip, r = S.wMm > 0 ? S.hMm / S.wMm : 1; S = startSpec(Object.assign({}, S, { wMm: v, hMm: S.shape === 'circle' ? v : r1(v * r) })); syncNew(); }
    });
    q.addEventListener('input', () => { limit = 60; renderLib(); });
    for (const el of [fW, fH, fDia]) { el.addEventListener('input', () => fieldIn(el, false)); el.addEventListener('change', () => fieldIn(el, true)); el.addEventListener('focus', () => { try { el.select(); } catch (_) { /* fine */ } }); }
    fHole.addEventListener('change', () => { S = startSpec(Object.assign({}, S, { hole: fHole.checked })); syncNew(); });
    picker = { wrap, pane: setPane, result };
    mark(); setPane(o.pane);
    if (o.q) { q.value = String(o.q); renderLib(); }
    return result;
  }

  const api = {
    search, load, count, onChange: fn => { if (typeof fn === 'function') listeners.add(fn); return () => listeners.delete(fn); },
    open, openPicker, newDesign, openDesign,
    current: () => (session ? { name: session.start.name, source: Object.assign({}, session.start.source), designId: session.start.id || null, version: session.start.version || null, orderId: session.start.orderId || null } : null),
    model: { startSpec, startWhy, startFace, outlineSP, holeFor, masterFace, backFace, cutOutlines, cutBox, designOf, changed, indexOf, rank, norm, newStart, SHAPES, WALL_MM, HOLE_MM, HEART_CLEFT },
    version: 1
  };
  return api;
});
