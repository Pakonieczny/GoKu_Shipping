/* The ENGRAVING PREVIEW: a charm's drawing with its engraving words laid on it in the font that will really be engraved (ENGFONT, Paul, 12 Oct 2026: "the font doesn't actually
 * change on the screen in real-time when I select a different font option" and "I couldn't see the front engraving for the charm in real time").
 * Plan: plans/engrave-ui-1012 (CONTRACT.md section E, ENGFONT-findings.md, INTERFACES.md).
 *
 * ONE pure render component. It knows nothing of the options panel, the card or a pop-up: give it an element, a drawing, the words and a font, and it draws; tell it a change
 * (update) and it draws again, synchronously, in the same breath. The card thumbs and the panel's live preview are this one component, so they look exactly alike.
 * Why it exists: the page never had an engraving font as a CSS font (the files in vendor/fonts/ were only read as binaries for opentype.js, which turns the words into outline
 * paths), so no HTML could ever show one. ensure() registers each as a FontFace of this page, from the very file the engraving fits with.
 *
 *   render(el, o) -> el           fills `el` with the preview (a second render of the same row / drawing / face keeps the drawing and only repaints the words)
 *   update(el, partial) -> el     merges `partial` into what `el` shows and repaints at once; never throws
 *   ensure(fontId) -> Promise<boolean>   loads the installed font ('CNEng <id>': vendor/fonts/<Regular file>, added to document.fonts). Memoised, never blocks, true when it can be drawn
 *   ready(fontId) -> boolean      the font is loaded
 *   fontFamilyOf(fontId) -> "'CNEng <id>', serif"     a CSS font-family (draws the font once ready(fontId))
 *   fontIdOf(row, face, fontWord) -> id   the installed font a face is engraved in: the shop's word (Orders.fontOfPhrase), else the line's font (Orders.pieceFont)
 *   warm(ids?)                    loads the shop's fonts when the browser is idle (a panel that offers the choice calls it when it opens), so a pick is drawn at once
 *   link(el, { text, font }, { fallbackFontId }) -> { unlink(), sync() }   (extra) wires a words box and a font drop-down to `el`: input, keyup and change call update at once
 *   positionOf(job, o?) -> { x, y, size, angle } | null   (ENGTHUMB, round 2) where the job's words sit on the picture this file draws of that job's face: the job's fit (job.fit, or job.writtenFit for a
 *                                 written piece that let go of its geometry) as a fraction of the picture's box; angle counter-clockwise as job.fit.angle. null: no placement yet (the words are then centred)
 *   frameFor(o) -> frame | null    the charm's geometry as this face is cut (the work area's own frame: charm-nest-geom.js backView turned over for the back, the front as it is, long charms laid flat)
 *   fontStatus(fontId) 'idle'|'loading'|'ready'|'failed' · layoutOf(o) · CAP_PER_EM · linesOf
 *
 * o / partial
 *   row         the order line (for the drawing and the line's own font)
 *   drawing     function (host) that draws into the box. Default (ENGTHUMB, round 2: ListMedia is private to the bridge, so the old default drew nothing on the real page): THE CHARM ITSELF, from
 *               the very geometry the work area draws, ONE source with or without an Engrave job: the job's own view (job.view: the fitted back / front view) when it has one, else the pool's charm
 *               of the piece turned the way the fit turns it (Pool.charmOf + charm-nest-geom.js upAngleOf), else the order window's Vector design (PieceMedia.vectorThumb) as drawn. The FRONT
 *               shows everything: the cut line, the cut-outs and the design's engraved art (hatching, line art); the BACK shows the metal turned over (the cut line and cut-outs only, mirrored the way
 *               the back is cut). Drawn once per view / charm and face (cached), never again per keystroke
 *   job         the face's Engrave job (its view and fit give the picture and the words' place); slot the piece's own slot ('L' | 'R' | 'D1'..) for a face with no job yet; image a saved picture of the
 *               face (the approved back file's PNG) used only when no geometry can be had
 *   text        the words: a string (line breaks kept) or an array of lines. None: the quiet empty slot, nothing is written
 *   fontId      an installed font id (Orders.ENGRAVING_FONTS)  |  fontWord  the shop's word for it ("Ace" is Jost Slashed Zero; a word the shop does not offer is the app's own font)
 *   face        'front' | 'back'. The BACK flips only the drawing (scaleX(-1), the charm turned over); the words always read the right way
 *   position    { x, y, size, angle } the way CharmNestEngravePosition.of(job) gives it, each 0..1 of the drawing box: x, y the centre of the words, size the em as a fraction of
 *               the box's height, angle in degrees counter-clockwise (as job.fit.angle). null (or none): the middle, 0.2, 0 degrees. update() merges it key by key
 *   size        the size alone (the same as position.size)
 *
 * The words are laid out as charm-nest-geom.js layoutLines lays the job's: every row centred, a row as tall as the font's capitals plus 0.216 em, the block centred on the
 * position by its capitals; a font with no lowercase (Ace) in capitals (Orders.capsText). Words longer than the box allows are scaled down to it (the job fits to the metal
 * when it is saved). While the font is really being fetched the words are hidden (no flash of the wrong font) and a small "Loading font" with a spinner shows; both go the
 * moment it is ready. A font that cannot be had says "Font not available" and the words show in a serif. Nothing waits on it.
 * Loads in the page (window.CharmNestEngravePreview) and in node (fontIdOf, fontFamilyOf, layoutOf are pure there). */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === 'object' && module.exports) module.exports = api; else root.CharmNestEngravePreview = api;
})(typeof self !== 'undefined' ? self : this, function (root) {
  'use strict';
  const dep = (name, file) => { const g = root && root[name]; if (g) return g; try { return typeof require === 'function' ? require(file) : null; } catch (_) { return null; } };
  const Orders = () => dep('CharmNestOrders', './charm-nest-orders.js');
  const str = x => String(x == null ? '' : x);
  const num = (v, d) => (v !== null && v !== undefined && v !== '' && Number.isFinite(+v) ? +v : d);
  const warn = e => { try { if (typeof console !== 'undefined') console.warn('engraving preview', e); } catch (_) { /* nothing to tell */ } };

  /* ── the one formula of the engraving job ──────────────────────────────────────
   * charm-nest-geom.js layoutLines: cap = size * capPerEm(font) (OS/2 sCapHeight / unitsPerEm of the file the job fits with), the gap between rows is size * 0.216, a block of n
   * rows is n*cap + (n-1)*gap tall and is centred on the position. CAP_PER_EM is that capPerEm measured from the files of vendor/fonts (the test checks it against them). */
  const CAP_PER_EM = { 'source-sans-3': 0.66, 'playwrite-us-trad': 1, 'marck-script': 0.638, mynerve: 0.67, 'crimson-text': 0.6406, 'playwrite-us-modern': 0.938, jost: 0.7, 'alegreya-sans': 0.644, vibur: 0.6699, 'jost-ace': 0.7, 'libre-franklin': 0.742 };
  const GAP = 0.216, AUTO = { x: 0.5, y: 0.5, size: 0.2, angle: 0 };
  const capPerEm = id => CAP_PER_EM[id] || 0.7;

  /* ── fonts ─────────────────────────────────────────────────────────────────── */
  const FONTS = new Map();   // font id -> { id, status: loading | ready | failed, at, error, promise }
  const LIVE = new Set();    // the previews on screen: repainted when a font they wait for arrives
  const defaultId = () => { const O = Orders(); return (O && O.DEFAULT_FONT) || 'source-sans-3'; };
  /** The installed font id of an id (an unknown one is the app's own font, as the engraving does). */
  const known = id => { const O = Orders(), f = id && O && O.fontById ? O.fontById(id) : null; return f ? f.id : ''; };
  const idOf = id => known(id) || defaultId();
  const fontFamilyOf = id => `'CNEng ${idOf(id)}', serif`;
  const statusOf = id => { const s = FONTS.get(idOf(id)); return s ? s.status : 'idle'; };
  const ready = id => statusOf(id) === 'ready';
  /** The font a face is engraved in: the shop's word (the drop-down's word, Orders.fontOfPhrase), else the line's own font for that piece and face (Orders.pieceFont). */
  function fontIdOf(row, face, fontWord) {
    const O = Orders(); let id = '';
    try { if (fontWord && O && O.fontOfPhrase) id = O.fontOfPhrase(fontWord) || ''; } catch (_) { /* below */ }
    if (!id && O && O.pieceFont && row && row.spec) {
      const m = /^D(\d+)$/.exec(str(row.slot)), ix = m ? +m[1] - 1 : null;
      try { id = O.pieceFont(row.spec, ix, face === 'front' ? 'front' : 'back').id || ''; } catch (_) { /* the app's own font */ }
    }
    return idOf(id);
  }
  const withTimeout = (p, ms, msg) => { let t; return Promise.race([p, new Promise((_, rej) => { t = setTimeout(() => rej(new Error(msg)), ms); })]).finally(() => clearTimeout(t)); };
  function changed(id) {
    for (const el of [...LIVE]) {
      const st = el._engpv;
      if (!st) { LIVE.delete(el); continue; }
      if (!el.isConnected && st.seen) { LIVE.delete(el); continue; }
      if (st.fontId === id) paint(el);
    }
  }
  /** Loads the installed font as a FontFace of this page ('CNEng <id>': the Regular file of vendor/fonts/ the engraving fits with), added to document.fonts. Memoised. */
  function ensure(fontId) {
    const id = idOf(fontId), prev = FONTS.get(id);
    if (prev && (prev.status !== 'failed' || Date.now() - prev.at < 30000)) return prev.promise;
    const O = Orders(), meta = O && O.fontById ? O.fontById(id) : null, doc = root && root.document, FF = root && root.FontFace, file = meta && meta.files && meta.files.Regular;
    const s = { id, status: 'loading', at: Date.now(), error: '', promise: null };
    FONTS.set(id, s);
    if (!file || !doc || !doc.fonts || typeof FF !== 'function') {
      s.status = 'failed'; s.error = !file ? 'This font is not one the shop engraves in' : 'This browser cannot load fonts'; s.promise = Promise.resolve(false); Promise.resolve().then(() => changed(id)); return s.promise;
    }
    let face = null;
    try { face = new FF(`CNEng ${id}`, `url("${file}")`, { style: 'normal', weight: '400', display: 'swap' }); doc.fonts.add(face); } catch (e) { s.status = 'failed'; s.error = str(e && e.message); s.promise = Promise.resolve(false); Promise.resolve().then(() => changed(id)); return s.promise; }
    s.promise = withTimeout(face.load(), 12000, 'The font took too long to load')
      .then(() => { s.status = 'ready'; return true; })
      .catch(e => { s.status = 'failed'; s.error = (e && e.message) || 'The font could not be loaded'; try { doc.fonts.delete(face); } catch (_) { /* gone anyway */ } return false; })
      .then(ok => { s.at = Date.now(); changed(id); return ok; });
    return s.promise;
  }
  /** Starts the shop's fonts (or the ones named) when the browser is idle, so a pick is drawn at once. Nobody waits for it. */
  function warm(ids) {
    const O = Orders(), list = [...new Set((ids || [defaultId()].concat(((O && O.FONT_WORDS) || []).map(g => g.id))).map(idOf))];
    const go = () => { for (const id of list) ensure(id); };
    try { if (root && typeof root.requestIdleCallback === 'function') root.requestIdleCallback(go, { timeout: 2000 }); else setTimeout(go, 0); } catch (_) { go(); }
  }

  /* ── geometry ──────────────────────────────────────────────────────────────── */
  const linesOf = v => (Array.isArray(v) ? v : str(v).split(/\r?\n/)).map(x => str(x).replace(/\s+/g, ' ').trim()).filter(Boolean);
  /** PURE. The rows of a block of words as charm-nest-geom.js layoutLines lays them out, in em of the font: { cap, gap, pitch, totalH, rows: [{ text, y }] } (y of a row's baseline, up from the block's centre). */
  function layoutOf(o) {
    const lines = linesOf(o && o.lines), cap = num(o && o.capPerEm, 0.7), gap = num(o && o.lineGap, GAP), totalH = lines.length ? lines.length * cap + (lines.length - 1) * gap : 0;
    return { cap, gap, pitch: cap + gap, totalH, rows: lines.map((text, i) => ({ text, y: totalH / 2 - cap - i * (cap + gap) })) };
  }
  const METRICS = new Map();
  /** How far the browser's own line box puts the letters below where the job's capital-centred block would: (ascent - descent - cap) / 2 em, from the font as loaded (0 when the browser cannot say). */
  function dropOf(id) {
    if (METRICS.has(id)) return METRICS.get(id);
    let d = 0;
    try {
      const doc = root && root.document, c = doc && doc.createElement ? doc.createElement('canvas').getContext('2d') : null;
      if (c) { c.font = `100px 'CNEng ${id}'`; const m = c.measureText('H'); if (Number.isFinite(m.fontBoundingBoxAscent) && Number.isFinite(m.fontBoundingBoxDescent)) d = (m.fontBoundingBoxAscent - m.fontBoundingBoxDescent) / 200 - capPerEm(id) / 2; }
    } catch (_) { d = 0; }
    if (ready(id)) METRICS.set(id, d);   // (only measured on the font itself, never on the stand-in)
    return d;
  }

  /* ── the charm's own picture (ENGTHUMB, round 2) ─────────────────────────────────
   * Paul, 12 Oct 2026: "in each box I would see the current engraving and one would be front facing and one would be back facing so that I would see the charm front and back and the front is gonna
   * have to show all of the engraving/cutouts". Why the boxes showed text on a white square: the old default drawing was ListMedia.vectorInto, and ListMedia is a private const of the bridge's
   * closure (the page hands only PieceMedia to other scripts), so on the real page `typeof ListMedia` was always 'undefined' and nothing was ever drawn behind the words.
   * The picture is drawn here from the geometry the work area itself draws, in the work area's own frame (so a thumbnail is the work area in small, and the words' place is the job's fit with
   * no conversion): charm-nest-geom.js backView = the cut geometry turned over for the BACK, as it is for the FRONT, then turned (long charms flat, the hanging hole near the top).
   *   frame = { kind: 'view'|'charm', face, box: [x0,y0,x1,y1] (the cut line's bounds in that frame, pt), outline, cut: [segments], art: [segments] (the FRONT only: what the design engraves) }
   * A frame comes from the job's view when the job has one (exact), else from the pool's charm of the piece (the fit's own rule, upAngleOf, or the saved angle of a written piece). It is built once per
   * view / charm and face (WeakMap) and painted once (a canvas kept on the frame); a thumbnail takes a copy. */
  const GEO = () => dep('CharmNestGeom', './charm-nest-geom.js');
  const PDFLIB = () => (root && root.CharmNestPDF) || null;
  const fin = v => typeof v === 'number' && Number.isFinite(v);
  const unionBox = segs => { let b = null; for (const s of segs || []) { const q = s && s.bbox; if (!q || q.length !== 4 || !q.every(fin)) continue; b = b ? [Math.min(b[0], q[0]), Math.min(b[1], q[1]), Math.max(b[2], q[2]), Math.max(b[3], q[3])] : q.slice(); } return b && b[2] > b[0] && b[3] > b[1] ? b : null; };
  const isArt = s => !!s && s.kind === 'path' && !/keep-?out/i.test(str(s.layer));
  const FRAMES = typeof WeakMap === 'function' ? new WeakMap() : null;
  function memo(obj, key, make) {
    if (!FRAMES || !obj || typeof obj !== 'object') return make();
    let m = FRAMES.get(obj); if (!m) FRAMES.set(obj, m = new Map());
    if (m.has(key)) return m.get(key);
    const v = make(); if (v) m.set(key, v); return v;
  }
  /** The frame of a job's view (a back view: the charm turned over and turned; a front view: as it is, turned): the cut geometry as the work area draws it, and for the FRONT the design's art in the same frame. */
  function frameOfView(view) {
    if (!view || !Array.isArray(view.members) || !view.members.length) return null;
    const face = view.face === 'front' ? 'front' : 'back';
    return memo(view, 'v', () => {
      const G = GEO(), box = unionBox(view.members); if (!G || !box) return null;
      const outline = view.outline && view.members.includes(view.outline) ? view.outline : view.members[0];
      let art = [];
      if (face === 'front' && Array.isArray(view.dropped) && G.transformSeg) {
        art = view.dropped.filter(isArt).map(d => { try { const a = view.M ? G.transformSeg(d, view.M) : d; return view.R ? G.transformSeg(a, view.R) : a; } catch (_) { return null; } }).filter(Boolean);
      }
      return { kind: 'view', face, box, outline, cut: view.members, art };
    });
  }
  /** The frame of a pool charm for a face, turned as the fit turns it: the BACK mirrored about the charm's centre line, both turned by 90 - up about it (up: the saved angle of a written piece, else
   *  the library's typed angle, else charm-nest-geom.js upAngleOf, the very rule backView uses). */
  function frameOfCharm(charm, face, up) {
    if (!charm || !charm.outline || !Array.isArray(charm.members) || !charm.members.length) return null;
    const G = GEO(); if (!G || !G.isCutLine || !G.transformSeg || !G.rotateAbout || !G.mirrorX) return null;
    return memo(charm, face + '|' + (fin(up) ? up : 'auto'), () => {
      const outline = charm.outline, ob = outline.bbox; if (!ob || ob.length !== 4 || !ob.every(fin)) return null;
      const cut = charm.members.filter(m => m === outline || G.isCutLine(m)); if (!cut.includes(outline)) cut.unshift(outline);
      let upA = up;
      if (!fin(upA)) { try { upA = G.upAngleOf(Object.assign({}, charm, { outline, members: cut }), face === 'front' ? { face: 'front' } : {}).angle; } catch (_) { upA = 90; } }
      const cx = (ob[0] + ob[2]) / 2, cy = (ob[1] + ob[3]) / 2, M = face === 'front' ? null : G.mirrorX(cx), R = G.rotateAbout(cx, cy, 90 - upA);
      const T = s => G.transformSeg(M ? G.transformSeg(s, M) : s, R);
      const tcut = cut.map(T), box = unionBox(tcut); if (!box) return null;
      const art = face === 'front' ? charm.members.filter(m => !cut.includes(m) && isArt(m)).map(T) : [];
      return { kind: 'charm', face, box, outline: tcut[cut.indexOf(outline)], cut: tcut, art };
    });
  }
  /** The pool's charm of a piece: the job's own copy, else the line's pool id (a counted line's disc N is its N-th). null without a pool (an order not yet on a sheet). */
  function charmFor(o) {
    const Pl = root && root.Pool; if (!Pl || typeof Pl.charmOf !== 'function') return null;
    const job = o && o.job, row = (o && o.row) || (job && job.row) || null, slot = (o && o.slot) || (job && job.slot) || '';
    let id = job && Array.isArray(job.copies) && job.copies.length ? job.copies[0] : '';
    if (!id && row && Array.isArray(row.poolIds) && row.poolIds.length) { const m = /^D(\d+)$/.exec(str(slot)); id = row.poolIds[m ? Math.min(+m[1] - 1, row.poolIds.length - 1) : 0]; }
    try { return id ? Pl.charmOf(id) || null : null; } catch (_) { return null; }
  }
  /** A person's own typed angle for a design (the library's entry says "operator"): the back is turned that way, as backView is told by viewOptionsFor. */
  function operatorUp(row) {
    try { const M = root && root.Master, sku = row && row.spec && row.spec.designSku, e = M && M.entryFor && sku ? M.entryFor(sku) : null; return e && e.upSource === 'operator' && fin(+e.upAngle) ? +e.upAngle : null; } catch (_) { return null; }
  }
  const faceOfJob = job => (job && job.face === 'front' ? 'front' : 'back');
  /** The frame to draw for `o` ({ job, row, slot, face }): the job's view when it has one, else the piece's pool charm. null when neither exists. */
  function frameFor(o) {
    o = o || {}; const job = o.job, face = job ? faceOfJob(job) : o.face === 'front' ? 'front' : 'back';
    if (job && job.view) { const f = frameOfView(job.view); if (f) return f; }
    const charm = charmFor(o); if (!charm) return null;
    const w = job && job.writtenFit, up = w && fin(+w.upAngle) ? +w.upAngle : (face === 'front' ? null : operatorUp(o.row || (job && job.row)));
    return frameOfCharm(charm, face, up);
  }
  const placeOf = (fit, box) => { const w = box[2] - box[0], h = box[3] - box[1]; return { x: (fit.centre[0] - box[0]) / w, y: 1 - (fit.centre[1] - box[1]) / h, size: (fin(+fit.size) ? +fit.size : 0) / h, angle: fin(+fit.angle) ? +fit.angle : 0 }; };
  /** Where the job's words sit on its face's picture (a fraction of the box; y counts down; angle counter-clockwise as fit.angle). null while the job has no placement. */
  function positionOf(job, o) {
    try {
      if (!job) return null; o = o || {};
      const fit = job.fit && Array.isArray(job.fit.centre) && job.fit.centre.every(fin) ? job.fit : null;
      if (fit && job.view) { const F = frameOfView(job.view); return F ? placeOf(fit, F.box) : null; }
      const w = job.writtenFit;
      if (w && Array.isArray(w.centre) && w.centre.every(fin) && fin(+w.upAngle)) { const F = frameFor({ job, row: o.row || job.row, slot: o.slot }); return F ? placeOf(w, F.box) : null; }
    } catch (e) { warn(e); }
    return null;
  }

  /** The picture of a frame as a canvas (kept on the frame): a faint metal tint inside the cut line, the design's art (the FRONT), then the cut line and cut-outs, as the page draws a charm everywhere
   *  (CharmNestPDF.drawCharm). The canvas is the box plus a 4 % margin, so the words' coordinates (a fraction of the box) are the canvas's inner rectangle. */
  const PX = 224, MARGIN = 0.04;
  function artOf(F) {
    if (F.art_) return F.art_;
    const doc = root && root.document, PDF = PDFLIB(); if (!doc || !PDF || typeof PDF.drawCharm !== 'function') return null;
    const [x0, , , y1] = F.box, w = F.box[2] - F.box[0], h = F.box[3] - F.box[1], pad = Math.max(w, h) * MARGIN, W = w + 2 * pad, H = h + 2 * pad, k = PX / Math.max(W, H);
    const cv = doc.createElement('canvas'); cv.width = Math.max(2, Math.round(W * k)); cv.height = Math.max(2, Math.round(H * k));
    const ctx = cv.getContext('2d'); if (!ctx) return null;
    const tx = (x, y) => [(x - x0 + pad) * k, (y1 + pad - y) * k];
    try {
      ctx.beginPath(); PDF.pathToCanvas(ctx, F.outline, tx); for (const m of F.cut) if (m !== F.outline && m.closed) PDF.pathToCanvas(ctx, m, tx);
      ctx.fillStyle = '#f1ece1'; ctx.fill('evenodd');
      PDF.drawCharm(ctx, { outline: F.outline, members: F.art.concat(F.cut) }, tx, k);
    } catch (e) { warn(e); }
    return (F.art_ = { canvas: cv, w: cv.width, h: cv.height, ar: cv.width / cv.height, ix: pad / W, iy: pad / H });
  }

  /** The order window's Vector design of the line (PieceMedia.vectorThumb: the page's own picture, as drawn, from the page's own cache), the last resort when there is no geometry to draw from. */
  const VEC = new Map();
  function vectorOf(row) {
    const PM = root && root.PieceMedia; if (!PM || typeof PM.vectorThumb !== 'function' || !row) return null;
    let key = ''; try { key = PM.vectorKey ? str(PM.vectorKey(row)) : ''; } catch (_) { key = ''; }
    if (!key) return null;
    let e = VEC.get(key);
    if (!e) {
      e = { key, url: null, status: 'loading', subs: new Set() }; VEC.set(key, e);
      while (VEC.size > 60) VEC.delete(VEC.keys().next().value);
      Promise.resolve().then(() => PM.vectorThumb(row)).then(u => { e.url = typeof u === 'string' && u ? u : null; e.status = e.url ? 'ready' : 'none'; }, () => { e.status = 'none'; }).then(() => { const subs = [...e.subs]; e.subs.clear(); for (const f of subs) { try { f(); } catch (_) { /* its own */ } } });
    }
    return e;
  }

  /** What `o` draws from: a caller's own drawing function; else the charm's frame (job view, or pool charm); else a saved picture; else the line's Vector design; else nothing. */
  function sourceOf(o) {
    const face = o.face === 'front' ? 'front' : o.face === 'back' ? 'back' : '';
    if (typeof o.drawing === 'function') return { kind: 'fn', key: 'f' + ident(o.drawing) + '|' + ident(o.row) + '|' + face, flip: face === 'back' };
    const F = frameFor(o);
    if (F && artOf(F)) return { kind: 'frame', key: 'g' + ident(F), frame: F, flip: false };
    if (o.image) return { kind: 'png', key: 'p' + o.image, url: String(o.image), flip: false, baked: true };
    const V = vectorOf(o.row);
    if (V && V.status !== 'none') return { kind: 'vec', key: 'v' + V.key + '|' + V.status, entry: V, flip: face === 'back' };
    return { kind: 'none', key: 'n', flip: false };
  }

  /* ── what it looks like ────────────────────────────────────────────────────── */
  const CSS = `.engpv{position:relative;display:flex;align-items:center;justify-content:center;box-sizing:border-box;width:100%;height:100%;overflow:hidden;container-type:size}
.engpvFrame{position:relative;flex:none;width:100cqw;height:100cqh}
.engpvFrame[data-ar]{width:min(100cqw,calc(100cqh * var(--ar)));height:auto;aspect-ratio:var(--ar)}
.engpvDraw{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;pointer-events:none}
.engpvDraw canvas,.engpvDraw img,.engpvDraw svg{max-width:100%;max-height:100%;object-fit:contain}
.engpvFrame[data-ar] .engpvDraw canvas,.engpvFrame[data-ar] .engpvDraw img{width:100%;height:100%;display:block}
.engpv[data-flip="1"] .engpvDraw{transform:scaleX(-1)}
.engpvBox{position:absolute;inset:0;container-type:size;pointer-events:none}
.engpvText{position:absolute;margin:0;white-space:pre;text-align:center;color:var(--ink,#1c1a17);transform-origin:50% 50%;pointer-events:none}
.engpvText[hidden]{display:block;visibility:hidden}
.engpvSlot{position:absolute;transform-origin:50% 50%;box-sizing:border-box;border:1px dashed var(--ink45,#938c80);opacity:.7;pointer-events:none}
.engpvWait{position:absolute;right:4px;bottom:3px;display:inline-flex;align-items:center;gap:5px;max-width:calc(100% - 8px);padding:1px 6px;border-radius:999px;background:var(--card,#fffefb);font:10px var(--sans,system-ui);color:var(--ink70,#5b554c);white-space:nowrap;overflow:hidden}
.engpvWait[hidden]{display:none}
.engpvSpin{width:9px;height:9px;flex:0 0 9px;border:2px solid var(--line,#e4ddd0);border-top-color:var(--ink70,#5b554c);border-radius:50%;animation:engpvSpin .7s linear infinite}
.engpvWait[data-fail] .engpvSpin{display:none}
@keyframes engpvSpin{to{transform:rotate(360deg)}}
@media (prefers-reduced-motion:reduce){.engpvSpin{animation-duration:1.6s}}`;
  function ensureCss(doc) {
    try { if (!doc) return; const had = doc.getElementById('engPreviewCss'); if (had) { if (had.textContent !== CSS) had.textContent = CSS; return; } const s = doc.createElement('style'); s.id = 'engPreviewCss'; s.textContent = CSS; (doc.head || doc.documentElement).appendChild(s); } catch (_) { /* unstyled is still readable */ }
  }
  const ids = new WeakMap(); let idN = 0;
  const ident = o => { if (!o || (typeof o !== 'object' && typeof o !== 'function')) return String(o); let n = ids.get(o); if (!n) ids.set(o, n = ++idN); return n; };

  /** The preview's own nodes: the frame (the picture's shape inside the box), the drawing (kept while its source stays), the words' layer (the picture's box), the words, the empty slot, the wait line. */
  function shell(el, st, src) {
    const doc = el.ownerDocument, key = src.key;
    if (st.nodes && st.nodes.root.parentNode === el && st.drawKey === key) return st.nodes;
    el.replaceChildren();
    const box = doc.createElement('span'); box.className = 'engpv';
    const frame = doc.createElement('span'); frame.className = 'engpvFrame';
    const draw = doc.createElement('span'); draw.className = 'engpvDraw';
    const layer = doc.createElement('span'); layer.className = 'engpvBox';
    const text = doc.createElement('span'); text.className = 'engpvText'; text.hidden = true;
    const slot = doc.createElement('span'); slot.className = 'engpvSlot'; slot.hidden = true; slot.setAttribute('aria-hidden', 'true');
    const wait = doc.createElement('span'); wait.className = 'engpvWait'; wait.setAttribute('role', 'status'); wait.setAttribute('aria-live', 'polite'); wait.setAttribute('aria-label', 'Loading font'); wait.hidden = true;
    wait.innerHTML = '<i class="engpvSpin" aria-hidden="true"></i><span class="engpvWaitT">Loading font</span>';
    layer.append(slot, text); frame.append(draw, layer); box.append(frame, wait); el.appendChild(box);
    st.drawKey = key; st.nodes = { root: box, frame, draw, layer, text, slot, wait };
    const shape = (ar, ix, iy) => {
      if (ar > 0) { frame.style.setProperty('--ar', ar.toFixed(5)); frame.setAttribute('data-ar', '1'); } else { frame.style.removeProperty('--ar'); frame.removeAttribute('data-ar'); }
      layer.style.inset = ix || iy ? `${(iy * 100).toFixed(3)}% ${(ix * 100).toFixed(3)}%` : '0';
    };
    try {
      if (src.kind === 'fn') src.draw ? src.draw(draw) : st.opts.drawing(draw);
      else if (src.kind === 'frame') { const a = artOf(src.frame), c = doc.createElement('canvas'); c.width = a.w; c.height = a.h; c.getContext('2d').drawImage(a.canvas, 0, 0); c.setAttribute('aria-hidden', 'true'); draw.appendChild(c); shape(a.ar, a.ix, a.iy); }
      else if (src.kind === 'png' || (src.kind === 'vec' && src.entry.url)) {
        const im = doc.createElement('img'); im.alt = ''; im.setAttribute('aria-hidden', 'true'); if (src.kind === 'png') im.crossOrigin = 'anonymous';
        const fit = () => { if (im.naturalWidth > 0 && im.naturalHeight > 0 && st.nodes && st.nodes.frame === frame) shape(im.naturalWidth / im.naturalHeight, 0, 0); };
        im.addEventListener('load', fit); im.src = src.kind === 'png' ? src.url : src.entry.url; draw.appendChild(im); if (im.complete) fit();
      }
    } catch (e) { warn(e); }
    return st.nodes;
  }

  /** Repaints `el` from its state, synchronously. */
  function paint(el) {
    const st = el._engpv; if (!st) return;
    try {
      const o = st.opts, src = sourceOf(o), N = shell(el, st, src), id = idOf(o.fontId || fontIdOf(o.row, o.face, o.fontWord)), O = Orders();
      if (el.isConnected) st.seen = true;
      st.fontId = id;
      if (src.kind === 'vec' && src.entry.status === 'loading') { if (!st.sub) st.sub = () => { if (el._engpv === st) paint(el); }; src.entry.subs.add(st.sub); }   // (painted again when the picture arrives)
      if (statusOf(id) === 'idle' || (statusOf(id) === 'failed' && Date.now() - (FONTS.get(id).at || 0) >= 30000)) ensure(id);
      const status = statusOf(id), fs = FONTS.get(id), picWait = src.kind === 'vec' && src.entry.status === 'loading';
      N.root.dataset.face = o.face === 'front' ? 'front' : o.face === 'back' ? 'back' : ''; N.root.dataset.font = status; N.root.dataset.fontId = id; N.root.dataset.pic = src.kind + (picWait ? ':loading' : '');
      if (src.flip) N.root.setAttribute('data-flip', '1'); else N.root.removeAttribute('data-flip');
      N.wait.hidden = status === 'ready' && !picWait; if (status === 'failed') N.wait.setAttribute('data-fail', ''); else N.wait.removeAttribute('data-fail');
      const waitWord = status === 'failed' ? 'Font not available' : status !== 'ready' ? 'Loading font' : 'Loading charm';
      N.wait.setAttribute('aria-label', waitWord);
      N.wait.querySelector('.engpvWaitT').textContent = waitWord;
      if (status === 'failed' && fs && fs.error) N.wait.title = fs.error; else N.wait.removeAttribute('title');
      // where the words go: the position (merged key by key over the middle), the size from the picture box's height
      const p = Object.assign({}, AUTO, st.pos || {}), size = num(o.size, NaN), em = Number.isFinite(size) && size > 0 && !(st.pos && Number.isFinite(+st.pos.size)) ? size : num(p.size, AUTO.size), angle = num(p.angle, 0);
      const x = num(p.x, AUTO.x), y = num(p.y, AUTO.y);
      let lines = src.baked ? [] : linesOf(o.text); if (lines.length && O && O.fontCaps && O.fontCaps(id)) lines = lines.map(l => (O.capsText ? O.capsText(l, id) : l.toLocaleUpperCase('en-US')));
      N.root.dataset.state = src.baked ? 'baked' : lines.length ? 'ready' : 'empty';
      const pos = `left:${+(x * 100).toFixed(3)}%;top:${+(y * 100).toFixed(3)}%;`;
      if (src.baked) { N.slot.hidden = true; N.text.hidden = true; N.text.textContent = ''; N.root.dataset.size = ''; return; }   // (a saved picture has its words in it already)
      if (!lines.length) {   // the quiet empty slot: a dashed place where the words will go, turned as they would be
        const w = Math.min(0.46, Math.max(0.2, em * 3)), h = Math.min(0.14, Math.max(0.06, em * 0.7));
        N.slot.style.cssText = `${pos}width:${(w * 100).toFixed(2)}%;height:${(h * 100).toFixed(2)}%;border-radius:${(h * 50).toFixed(2)}cqh;transform:translate(-50%,-50%) rotate(${-angle}deg)`;
        N.slot.hidden = false; N.text.hidden = true; N.text.textContent = ''; N.root.dataset.size = ''; return;
      }
      N.slot.hidden = true;
      const cap = capPerEm(id), L = layoutOf({ lines, capPerEm: cap }), drop = status === 'ready' ? dropOf(id) : 0;
      const t = N.text;
      t.textContent = lines.join('\n');
      t.style.cssText = `${pos}font-family:${fontFamilyOf(id)};font-size:${(em * 100).toFixed(3)}cqh;line-height:${L.pitch.toFixed(4)};transform:translate(-50%,-50%) rotate(${-angle}deg) translateY(${(-drop).toFixed(4)}em)`;
      t.hidden = status === 'loading';   // (no flash of the wrong font: the words appear when the font is ready; a font that cannot be had shows them in a serif)
      // words longer than the picture allows are scaled down to its box (about their own centre; the job fits them to the metal when it is saved)
      const bw = N.layer.clientWidth, bh = N.layer.clientHeight;
      if (bw > 0 && bh > 0 && status !== 'loading') {
        const a = angle * Math.PI / 180, w = t.offsetWidth, h = t.offsetHeight, ew = Math.abs(w * Math.cos(a)) + Math.abs(h * Math.sin(a)), eh = Math.abs(w * Math.sin(a)) + Math.abs(h * Math.cos(a));
        const k = Math.min(1, 0.94 * bw / Math.max(ew, 1e-6), 0.94 * bh / Math.max(eh, 1e-6));
        if (k < 0.999) t.style.transform = `translate(-50%,-50%) rotate(${-angle}deg) scale(${k.toFixed(4)}) translateY(${(-drop).toFixed(4)}em)`;
        N.root.dataset.size = (em * Math.min(1, k)).toFixed(4);
      } else N.root.dataset.size = em.toFixed(4);
    } catch (e) { warn(e); }
  }

  /* ── the API ───────────────────────────────────────────────────────────────── */
  const KEYS = ['row', 'drawing', 'text', 'fontId', 'fontWord', 'face', 'size', 'job', 'slot', 'image'];
  function render(el, o) {
    try {
      if (!el || !el.ownerDocument || typeof el.replaceChildren !== 'function') return el || null;
      ensureCss(el.ownerDocument);
      const was = el._engpv, opts = Object.assign({}, o || {});
      el._engpv = { opts, pos: o && o.position ? Object.assign({}, o.position) : null, nodes: was ? was.nodes : null, drawKey: was ? was.drawKey : '', seen: !!el.isConnected, fontId: '' };
      LIVE.add(el); paint(el);
    } catch (e) { warn(e); }
    return el;
  }
  function update(el, partial) {
    try {
      if (!el || !el.ownerDocument) return el || null;
      if (!el._engpv) return render(el, partial);
      const st = el._engpv, p = partial || {};
      for (const k of KEYS) if (p[k] !== undefined) st.opts[k] = p[k];
      if (p.position === null) st.pos = null; else if (p.position) st.pos = Object.assign({}, st.pos || {}, p.position);
      if (p.size !== undefined && p.position === undefined && st.pos) delete st.pos.size;
      if (p.fontId !== undefined && p.fontWord === undefined && p.fontId) st.opts.fontWord = '';   // (an id given: the old word no longer says the font)
      if (p.fontWord !== undefined && p.fontId === undefined && p.fontWord) st.opts.fontId = '';
      paint(el);
    } catch (e) { warn(e); }
    return el;
  }
  function link(el, ctl, o) {
    o = o || {}; ctl = ctl || {};
    const offs = [];
    const patchOf = () => {
      const p = {};
      if (ctl.text) p.text = ctl.text.value;
      if (ctl.font) {
        const v = str(ctl.font.value), id = known(v);
        if (id) { p.fontId = id; p.fontWord = ''; } else if (v) { p.fontWord = v; p.fontId = ''; } else { p.fontWord = ''; p.fontId = o.fallbackFontId || ''; }
      }
      return p;
    };
    const go = () => { try { update(el, patchOf()); } catch (e) { warn(e); } };
    const on = (node, evs) => { if (!node || typeof node.addEventListener !== 'function') return; for (const ev of evs) { node.addEventListener(ev, go); offs.push(() => node.removeEventListener(ev, go)); } };
    on(ctl.text, ['input', 'keyup', 'change']); on(ctl.font, ['input', 'change', 'keyup']);
    return { unlink() { for (const f of offs.splice(0)) f(); }, sync: go };
  }

  return { render, update, ensure, ready, fontFamilyOf, fontIdOf, warm, link, positionOf, frameFor, fontStatus: statusOf, layoutOf, CAP_PER_EM, linesOf };
});
