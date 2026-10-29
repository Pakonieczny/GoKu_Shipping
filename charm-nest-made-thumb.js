/* charm-nest-made-thumb.js — THE PIECE AS IT WILL BE MADE, in every list (THUMBSYNC, Paul, 28 Oct 2026: "the same changes should be saved as thumbnails to represent those particular orders so
   that a user scrolling through a list of orders can see updated thumbnails that already include the given charm's engraving for both back and front when necessary, including any images or text ...
   These changes must be real time on the host computer and updated on the cloud fairly quickly for others to see on other computers.")

   What there was, and why it was not in step. charm-nest-art-edit.js marks a charm the Charm Editor changed `artThumb` ("its picture is drawn again") and its ready() redrew `charm.thumb` on the edited
   charm, in this page's memory only. Nothing in a list ever read charm.thumb: every list box (the Orders rows and cards, Design & Engrave, Review, the order window's Vector design) is ListMedia.vectorInto
   in charm-nest-bridge.js, which draws the pool charm's FRONT as the master draws it (P.frontPreview) under a key made of the design and the piece's hash, so an edit showed only when a list happened to be
   drawn again, the fitted words never showed at all (they live on the Engrave jobs, drawn only by the Engrave card's Front / Back boxes, charm-nest-engrave-preview.js), and another computer, which holds
   neither the jobs nor the edited charm, could never show either. Nothing was ever stored in the cloud.

   What this does (one mechanism; artThumb stays what it is, the edited charm's own small picture):
     · PIECE  every piece (pool id) that has something of its own (fitted words on its front or back, or a Charm Editor change on either face) has a picture "as made": its FRONT, and beside it its
              BACK when the back is engraved (or has parts a person added), each drawn from the very frame the Engrave card's boxes draw (CharmNestEngravePreview.frameFor: the job's view, or the
              piece's pool charm turned the way the fit turns it), with the metal tint, the master's art and the editor's parts in the laser's colours (CharmNestPDF.drawCharm), the cut line, and the
              fitted words as the laser cuts them (job.fit.glyphs: the outlines of the font, not a stand-in). A piece with nothing of its own has no such picture: its line keeps the Vector design.
     · LINE   a list box shows its line: one piece's picture, or the pieces' pictures side by side (two columns past two, at most four, "+N" after). Local first (live), else what the cloud holds.
     · LIVE   the page tells changed() whenever anything may have moved (charm-nest-bridge.js: Engrave's render, Session.schedule, the artwork host's swap and changed). A sweep 120 ms later compares
              each piece's signature (the words' fit, the font, the view, the charm and its edit keys) with the last one; a piece that moved has the boxes that show it drawn again at once (ListMedia.refresh),
              the old picture kept in the box until the new one is there.
     · CLOUD  a piece that moved HERE is sent once the moves stop (DEBOUNCE after the last, MAX_WAIT at most, never while its face is being fitted again): op madeThumbPut keeps the PNG in Storage
              (charmnest/made/<poolId>/<hash>.png, charmnest/sandbox/... in the sandbox) and its address on the piece's pool row (`made`: url, at, sig, faces, base, by, dev), and names the order in the
              changes feed (p:<order>). One upload at a time, best effort, nothing waits on it.
     · OTHERS another computer reads the pool rows of the orders it shows anyway (charm-nest-placement-feed.js reads an order the feed names: one getOrderPieces); the row now carries `made`, OrderPieces
              tells its listeners, and the boxes on screen look again (refreshSoon): the picture is read once (charmNestAsset, kept by CharmNestThumbs on that computer) and shown. No new timer, no read
              per list drawn: a list box only looks at rows the page already holds.
   COST (Firestore): per change of a piece, on the computer that made it: one read and one write of its pool row (a transaction), plus the feed's own bump that every write makes; on each other computer
   that has the order on screen: the one getOrderPieces the feed already asks for that order. Nothing for a list drawn, a scroll or a reload (the row read for the list carries it).
   Which picture wins: this computer's own when the cloud has none, holds the same signature, or the piece was changed here (or sent from here before: `dev`); else the cloud's (a change made elsewhere).
   A change this page only heard of (an artwork edit synced from another computer: adopt) is not sent from here.

   window.CharmNestMadeThumb: setHost(h) · changed(why, poolIds?) · adopt(poolIds) · forLine(row) → { key, load } | null · forPiece(poolId) → { key, load } | null · sigOf(poolId) · flush() · state() */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === 'object' && module.exports) module.exports = api;
  if (root) root.CharmNestMadeThumb = api;
})(typeof self !== 'undefined' ? self : typeof globalThis !== 'undefined' ? globalThis : this, function (root) {
  'use strict';
  const V = 4;                                   // the drawing's own version (sent with the picture, kept on the pool row's made.v: MADEV): 2 FACES (named, a hairline), 3 BIGTHUMB, 4 BOXFIX (drawn to the list's fixed box); a picture of an older drawing is drawn again and sent, paced
  const TILE = 288, MARGIN = 0.02;               // a list box is drawn at least TILE device px high (BOXFIX: the cloud's picture: a 4:3 box, 384 x 288), each face with a 2 % margin
  const DEBOUNCE = 1500, MAX_WAIT = 6000, PACE = 1500, SWEEP = 120, REFRESH = 200;
  const MAX_PIECES = 4, INK = '#1c1a17', METAL = '#f1ece1', RULE = '#d6cfc2';
  // (the earrings' chip, charm-nest-pair-thumb.js INK / INK_TEXT / FONT, its words 0.07 of the picture's long side: a face's chip says "Front" / "Back" in words a third larger)
  const CHIP_INK = '#2a2724', CHIP_TEXT = '#fffaf0', CHIP_FONT = '"Source Sans 3","Segoe UI",system-ui,-apple-system,Helvetica,Arial,sans-serif', LABEL_FRAC = 0.1, BOX_PX = 112, BOX_AR = 4 / 3, MIN_CHIP_CSS = 11, CUT_CSS = 1.25;   // (BOXFIX: a list box is 4:3, about 112 css px high; the cloud's picture is drawn for that box)
  const GONE = new Set(['abandoned', 'superseded']);
  const doc = root && root.document || null;
  let host = {};
  const safe = (f, d) => { try { const v = f(); return v === undefined ? d : v; } catch (_) { return d; } };
  const fnv = s => { let h = 0x811c9dc5; for (let i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 0x01000193) >>> 0; } return h.toString(36); };
  const r2 = v => (Number.isFinite(+v) ? Math.round(+v * 100) / 100 : 0);
  const EP = () => (root && root.CharmNestEngravePreview) || null;
  const PDF = () => (root && root.CharmNestPDF) || null;
  const faceOf = j => (j && j.face === 'front' ? 'front' : 'back');

  /* ── who sent it: this computer (production: kept; the sandbox leaves nothing behind in the browser, so one per page there) ── */
  let DEV = '';
  const dev = () => {
    if (DEV) return DEV;
    const sb = !!safe(() => host.sandbox && host.sandbox(), false), mk = () => 'd' + Math.random().toString(36).slice(2, 10);
    if (sb) return (DEV = mk());
    let v = safe(() => root.localStorage.getItem('cn.madeThumb.dev'), '');
    if (!/^d[0-9a-z]{4,12}$/.test(v || '')) { v = mk(); safe(() => root.localStorage.setItem('cn.madeThumb.dev', v)); }
    return (DEV = v);
  };

  /* ── the pieces this page holds (jobs and edited charms), indexed once per sweep ── */
  let IDX = null;
  const index = () => { if (!IDX) IDX = safe(() => host.pieceIndex && host.pieceIndex(), null) || new Map(); return IDX; };
  const jobsOf = id => index().get(id) || {};   // (the index holds every job's pieces: a piece not in it has none, and no scan of the jobs is made for it)
  const REFIT = () => (root && root.CNEngraveRefit) || null;
  /** The fitted words of a face: { view, fit } with glyphs (the job's own, else the drawing held while it is fitted again after a charm edit), null when the face has none. */
  function wordsOf(j) {
    if (!j || j.archived || j.editingBack || ['none', 'skipped'].includes(j.state)) return null;
    let view = j.view, fit = j.fit;
    if (!(fit && view)) { const h = safe(() => { const R = REFIT(); return R && R.held ? R.held(j) : null; }, null); if (h && h.view && h.fit) { view = h.view; fit = h.fit; } }
    if (fit && view && Array.isArray(fit.glyphs) && fit.glyphs.length) return { view, fit };
    /* MADEV (Paul, 28 Oct 2026: "The thumbnails are missing all of the labels"): an approved face the upkeep put away (charm-nest-bridge.js shelveWritten: its placement kept as numbers, writtenFit, its
       view and letters let go) still has its words: counted here by its kept placement, and drawn from a copy laid out again exactly as approved (shelvedOf, Engrave.madeJobOf: the engraving card's own way) */
    const w = !fit && j.writtenFit; if (w && w.approvedAt && w.approvedAt === j.approvedAt && ['approved', 'written'].includes(j.state) && Array.isArray(w.lines) && w.lines.length) return { shelved: true, fit: w };
    return null;
  }
  /** A shelved face laid out again for its picture (Engrave.madeJobOf on a copy: the job is never touched): { view, fit, job } or null (its font still loading, its charm changed since). Kept per job and approval. */
  const SHELVED = typeof WeakMap === 'function' ? new WeakMap() : null;
  function shelvedOf(job, poolId) {
    if (!job) return null;
    const charm = safe(() => host.charmOf && host.charmOf(poolId), null), had = SHELVED && SHELVED.get(job);
    if (had && had.at === job.approvedAt && had.charm === charm && (had.j || Date.now() - had.t < 20000)) return had.j;
    const j = safe(() => (host.madeJob ? host.madeJob(job) : null), null), ok = j && j.view && j.fit && Array.isArray(j.fit.glyphs) && j.fit.glyphs.length ? j : null;
    if (SHELVED) SHELVED.set(job, { at: job.approvedAt, charm, j: ok, t: Date.now() });
    return ok;
  }
  const busy = j => !!j && (j.state === 'fitting' || safe(() => { const R = REFIT(); return !!(R && R.busy && !j.fit && R.busy(j)); }, false));
  /** What a face's art edit says (charm-nest-art-edit.js apply: the charm's artEdit[face] note), '' when it has none. */
  const editKey = (charm, face) => { const n = charm && charm.artEdit && charm.artEdit[face]; return n && (n.added || n.changed || n.removed) ? String(n.key || n.at || 1) : ''; };
  const backAdds = charm => !!(charm && Array.isArray(charm.artBack) && charm.artBack.length);
  /** One piece as this page holds it: { tiles: [{ face, job, words }], sig, faces, busy } or null (nothing of its own: no words fitted, no edit). */
  function stateOf(poolId) {
    if (!poolId) return null;
    const charm = safe(() => host.charmOf && host.charmOf(poolId), null), pj = jobsOf(poolId);
    const wf = wordsOf(pj.front), wb = wordsOf(pj.back), ef = editKey(charm, 'front'), eb = editKey(charm, 'back') || (backAdds(charm) ? 'adds' : '');
    if (!wf && !wb && !ef && !eb) return null;
    if (!charm && !(wf || wb)) return null;
    const tiles = [{ face: 'front', job: wf ? pj.front : null, words: wf }];
    if (wb || eb) tiles.push({ face: 'back', job: pj.back || null, words: wb });
    // (MADEV: the same words in the same place are the same signature whether the face holds its placement or the upkeep put it away: its turn is the view's upAngle, kept with it as writtenFit.upAngle)
    const fitSig = w => (w ? [(w.fit.lines || []).join('\n'), r2(w.fit.size), (w.fit.centre || []).map(r2), r2(w.fit.angle), String(w.fit.weight || '')] : 0);
    const viewSig = w => (!w ? 0 : [r2(w.shelved ? w.fit.upAngle : w.view.upAngle)]);
    const sig = fnv(JSON.stringify([V, charm ? String(charm.hash || charm.charmHash || '') + (charm.mirror ? 'm' : '') : '', ef, eb,
      tiles.map(t => [t.face, viewSig(t.words), fitSig(t.words), t.job ? String(t.job.fontKey || '') : ''])]));
    return { tiles, sig, faces: tiles.map(t => t.face), busy: busy(pj.front) || busy(pj.back) };
  }
  const sigOf = poolId => { const s = stateOf(poolId); return s ? s.sig : ''; };

  /* ── drawing ── */
  const canvas = (w, h) => { const c = doc.createElement('canvas'); c.width = Math.max(2, Math.round(w)); c.height = Math.max(2, Math.round(h)); return c; };
  /** The frame of one face of a piece (the Engrave card's own: CharmNestEngravePreview.frameFor). */
  const frameOf = (poolId, t) => safe(() => EP().frameFor(t.job ? { job: t.job, face: t.face } : { row: { poolIds: [poolId] }, face: t.face }), null);
  function glyphs(ctx, list, tx) {
    ctx.fillStyle = INK;
    for (const g of list || []) {
      ctx.beginPath();
      for (const c of g.cmds || []) {
        if (c.type === 'M') { const p = tx(c.x, c.y); ctx.moveTo(p[0], p[1]); }
        else if (c.type === 'L') { const p = tx(c.x, c.y); ctx.lineTo(p[0], p[1]); }
        else if (c.type === 'C') { const a = tx(c.x1, c.y1), b = tx(c.x2, c.y2), d = tx(c.x, c.y); ctx.bezierCurveTo(a[0], a[1], b[0], b[1], d[0], d[1]); }
        else if (c.type === 'Q') { const a = tx(c.x1, c.y1), d = tx(c.x, c.y); ctx.quadraticCurveTo(a[0], a[1], d[0], d[1]); }
        else if (c.type === 'Z') ctx.closePath();
      }
      ctx.fill('nonzero');
    }
  }
  /** The picture of one piece, drawn to fill a list box: o = { tile (the box's height in device px), ar (its width over its height), px (its height in css px) }. → { url (PNG data URL), w, h, faces,
   *  layout, labels, fontPx, hair, boxes } or null. Kept by signature and box. */
  const LOCAL = new Map();
  function pictureOf(poolId, st, o) {
    st = st || stateOf(poolId); if (!st || !doc) return null;
    o = typeof o === 'number' ? { tile: o } : o && o.nodeType ? (b => (b.px ? { tile: tileFor(b.px), ar: b.ar, px: b.px } : {}))(boxOf(o)) : o || {};
    const T = Math.max(48, Math.min(1200, Math.round(+o.tile || TILE))), AR = Math.max(0.5, Math.min(3, +o.ar || BOX_AR)), CSSPX = Math.max(40, +o.px || BOX_PX);
    const ck = st.sig + '@' + T + 'x' + AR.toFixed(3) + '/' + Math.round(CSSPX);
    const hit = LOCAL.get(ck); if (hit) return hit;
    const P = PDF(); if (!P || !P.drawCharm || !P.pathToCanvas || !EP()) return null;
    const parts = [];
    for (const t0 of st.tiles) {
      // (MADEV: a shelved face is drawn from its copy laid out again; one that cannot be yet (its font is loading) leaves the picture to the cloud's for now, and the boxes look again shortly)
      let t = t0;
      if (t0.words && t0.words.shelved) { const j = shelvedOf(t0.job, poolId); if (!j) { retrySoon(); return null; } t = { face: t0.face, job: j, words: { view: j.view, fit: j.fit } }; }
      const F = frameOf(poolId, t); if (!F || !F.box) continue;
      const w = F.box[2] - F.box[0], h = F.box[3] - F.box[1], pad = Math.max(w, h) * MARGIN;
      parts.push({ t, F, pad, fw: w + 2 * pad, fh: h + 2 * pad });   // (the face with its margin, in points)
    }
    if (!parts.length) return null;
    /* FACES (Paul, 28 Oct 2026: "label the back engraving with the same style of text with the black background ... larger ... side-by-side and top bottom alignment, depending on the charm shape ...
       split the image with very thin lines"): under each face (side by side) or beside it (one over the other) its name, "Front" / "Back", in the earrings' own chip (charm-nest-pair-thumb.js: the dark
       pill, light Semibold words), larger than theirs; the faces parted by one hairline, never a frame round each.
       BOXFIX (Paul, 28 Oct 2026: "thumbnail preview boxes must be locked into the same size always ... If you're using the split screen, either top bottom or side-by-side, it must always fit into the same
       sized thumbnail preview box and everything must be always perfectly aligned horizontally"): the picture IS the list's box (BW x BH device px, the box's own shape), white to its edges; the faces are
       laid in it side by side or one over the other, whichever draws them larger in that box; the chips' words are LABEL_FRAC of the box's height and never under MIN_CHIP_CSS css px. */
    const BH = T, BW = Math.round(T * AR), dpx = T / CSSPX, n = parts.length, labels = n > 1, gap = Math.max(2, Math.round(BH * 0.04));
    const fontPx = labels ? Math.max(Math.round(BH * LABEL_FRAC), Math.round(MIN_CHIP_CSS * dpx)) : 0, chipH = Math.round(fontPx * 1.55), hair = Math.max(1, Math.round(dpx));
    let words = parts.map(p => (p.t.face === 'front' ? 'Front' : 'Back'));
    const meter = canvas(4, 4).getContext('2d');
    const chipW = f => { if (!meter) return f * 3.8; meter.font = `600 ${f}px ${CHIP_FONT}`; return Math.max(...words.map(w => meter.measureText(w).width)) + f * 1.2; };
    const band = labels ? chipH + Math.round(fontPx * 0.3) : 0, side = labels ? Math.round(chipW(fontPx) + fontPx * 1.1) : 0;
    const inset = Math.round(BH * 0.03);   // (a little white round the drawing)
    const fitRow = () => { const aw = BW - 2 * inset - gap * (n - 1), ah = BH - 2 * inset - band; return Math.min(aw / parts.reduce((a, p) => a + p.fw, 0), ah / Math.max(...parts.map(p => p.fh))); };
    const fitCol = () => { const aw = BW - 2 * inset - side, ah = BH - 2 * inset - gap * (n - 1); return Math.min(aw / Math.max(...parts.map(p => p.fw)), ah / parts.reduce((a, p) => a + p.fh, 0)); };
    const one = () => Math.min((BW - 2 * inset) / parts[0].fw, (BH - 2 * inset) / parts[0].fh);
    const sRow = n > 1 ? fitRow() : one(), sCol = n > 1 ? fitCol() : 0, col = n > 1 && sCol > sRow, k = col ? sCol : sRow;
    // (a small box (a cell of a line of several pieces, a card): a chip wider than its face says F / B, as the earrings' say L / R when small)
    if (labels && !col && parts.some(p => chipW(fontPx) > p.fw * k + gap)) words = words.map(w => w[0]);
    const cv = canvas(BW, BH), ctx = cv.getContext('2d'); if (!ctx) return null;
    ctx.fillStyle = '#fff'; ctx.fillRect(0, 0, cv.width, cv.height);
    const chip = (word, cx, top) => {
      ctx.save(); ctx.font = `600 ${fontPx}px ${CHIP_FONT}`; ctx.textAlign = 'center'; ctx.textBaseline = 'alphabetic';
      const w = ctx.measureText(word).width + fontPx * 1.2, x = cx - w / 2, r = chipH / 2;
      ctx.fillStyle = CHIP_INK; ctx.beginPath(); ctx.moveTo(x + r, top); ctx.lineTo(x + w - r, top); ctx.arc(x + w - r, top + r, r, -Math.PI / 2, Math.PI / 2); ctx.lineTo(x + r, top + chipH); ctx.arc(x + r, top + r, r, Math.PI / 2, Math.PI * 1.5); ctx.closePath(); ctx.fill();
      ctx.fillStyle = CHIP_TEXT; ctx.fillText(word, cx, top + chipH / 2 + fontPx * 0.35); ctx.restore();
    };
    // (the block of faces, chips and gaps, centred in the box)
    const fws = parts.map(p => p.fw * k), fhs = parts.map(p => p.fh * k), maxW = Math.max(...fws), maxH = Math.max(...fhs);
    const blockW = col ? side + maxW : fws.reduce((a, b) => a + b, 0) + gap * (n - 1), blockH = col ? fhs.reduce((a, b) => a + b, 0) + gap * (n - 1) : maxH + band;
    const bx = (BW - blockW) / 2, by = (BH - blockH) / 2, boxes = [];
    let ox = bx, oy0 = by;
    for (const [i, p] of parts.entries()) {
      const { F, pad } = p, X = col ? bx + side + (maxW - fws[i]) / 2 : ox, Y = col ? oy0 : by + (maxH - fhs[i]) / 2, x0 = F.box[0], y1 = F.box[3];
      const tx = (x, y) => [X + (x - x0 + pad) * k, Y + (y1 + pad - y) * k];
      boxes.push({ face: p.t.face, x: Math.round(X), y: Math.round(Y), w: Math.round(fws[i]), h: Math.round(fhs[i]) });
      try {
        ctx.beginPath(); P.pathToCanvas(ctx, F.outline, tx); for (const m of F.cut || []) if (m !== F.outline && m.closed) P.pathToCanvas(ctx, m, tx);
        ctx.fillStyle = METAL; ctx.fill('evenodd');
        // (the cut line a fine line of about CUT_CSS px on screen, whatever the drawing's own pen: Paul, 28 Oct 2026, "heavy black rounded outlines around each half" — the master's 1 pt pen drawn at this scale)
        const thin = Math.max(0.05, (CUT_CSS * dpx) / k), cut = (F.cut || []).map(m => Object.assign({}, m, { lwPt: thin })), outline = cut[(F.cut || []).indexOf(F.outline)] || Object.assign({}, F.outline, { lwPt: thin });
        P.drawCharm(ctx, { outline, members: (F.art || []).concat(cut) }, tx, k);
        // (the words only on the frame they were fitted in: the job's own view, the one frameFor drew)
        if (p.t.words && F.kind === 'view') glyphs(ctx, p.t.words.fit.glyphs, tx);
      } catch (e) { safe(() => console.warn('made thumbnail', e)); }
      if (labels) { if (col) chip(words[i], bx + side / 2, oy0 + (fhs[i] - chipH) / 2); else chip(words[i], ox + fws[i] / 2, by + maxH + Math.round(fontPx * 0.15)); }
      if (i) { ctx.fillStyle = RULE; if (col) ctx.fillRect(Math.round(BW * 0.04), Math.round(oy0 - gap / 2 - hair / 2), Math.round(BW * 0.92), hair); else ctx.fillRect(Math.round(ox - gap / 2 - hair / 2), Math.round(BH * 0.04), hair, Math.round(BH * 0.92)); }
      ox += fws[i] + gap; oy0 += fhs[i] + gap;
    }
    const out = { url: cv.toDataURL('image/png'), w: cv.width, h: cv.height, faces: parts.map(p => p.t.face), layout: n < 2 ? 'one' : col ? 'column' : 'row', labels, fontPx, fontCss: fontPx / dpx, hair, boxes };
    LOCAL.set(ck, out); while (LOCAL.size > 400) LOCAL.delete(LOCAL.keys().next().value);
    return out;
  }

  /* ── the cloud's picture of a piece (its pool row's `made`, as this page holds the row) ── */
  function cloudOf(poolId) {
    const rows = safe(() => (host.poolRows ? host.poolRows(poolId) : []), []) || [];
    let best = null;
    for (const r of rows) {
      const m = r && r.made; if (!m || typeof m !== 'object' || !m.url || GONE.has(r.state)) continue;
      if (m.base != null && r.charmHash != null && String(m.base) !== String(r.charmHash)) continue;   // (a piece made again from another design keeps its pool id: its old picture is not its own)
      if (!best || (+m.at || 0) > (+best.at || 0)) best = m;
    }
    return best;
  }
  const known = poolId => (safe(() => (host.poolRows ? host.poolRows(poolId) : []), []) || []).some(Boolean);
  const mine = new Set();                       // pieces changed on this page (their picture is this page's until the cloud has it)
  /** MADEV (Paul, 28 Oct 2026: "The thumbnails are missing all of the labels"): the drawing a cloud picture was made by (made.v; one sent before the version was kept is 1). */
  const verOf = m => (m && Number.isFinite(+m.v) && +m.v >= 1 ? Math.floor(+m.v) : 1);
  /** NONEPLAIN (Paul, 29 Oct 2026: "When the user chooses "None" for engraving ... all Thumbnails must update in real-time to stop showing the need to add engraving"): this page holds the piece's engraving
   *  and every face of it is taken away (cut plain, or no engraving), and its charm has no edit of its own: it is a plain piece, so the cloud's picture of it as engraved (still on its pool row until the
   *  clear sent from here lands) is never shown: its line shows the Vector design, in the same tick. */
  function plainNow(poolId) {
    const pj = jobsOf(poolId), js = [pj.front, pj.back].filter(Boolean), charm = safe(() => host.charmOf && host.charmOf(poolId), null);
    return !!charm && js.length > 0 && js.every(j => j.state === 'none' || j.state === 'skipped') && !editKey(charm, 'front') && !editKey(charm, 'back') && !backAdds(charm);
  }
  /** Which picture a piece shows: { kind: 'local', sig, st, made } | { kind: 'cloud', made } | null. A cloud picture drawn by an older drawing than this page's never wins over this page's own. */
  function pick(poolId) {
    const st = stateOf(poolId), made = cloudOf(poolId);
    if (st && (!made || verOf(made) < V || made.sig === st.sig || mine.has(poolId) || made.dev === dev())) return { kind: 'local', sig: st.sig, st, made };
    if (made && !st && plainNow(poolId)) return null;
    if (made) return { kind: 'cloud', made };
    return null;
  }

  /* ── reading a picture the cloud keeps (once per computer: CharmNestThumbs keeps it on disk) ── */
  const CLOUD = new Map();
  const assetUrl = u => safe(() => (root.CharmNestAssets && root.CharmNestAssets.url ? root.CharmNestAssets.url(u) : u), u);
  function cloudPicture(url) {
    if (CLOUD.has(url)) return CLOUD.get(url);
    const T = root && root.CharmNestThumbs, read = () => (T && T.fetchDataUrl ? T.fetchDataUrl(assetUrl(url)) : fetch(assetUrl(url)).then(r => { if (!r.ok) throw new Error('picture ' + r.status); return r.blob(); }).then(b => new Promise((res, rej) => { const f = new FileReader(); f.onload = () => res(f.result); f.onerror = () => rej(f.error); f.readAsDataURL(b); })));
    const p = (T && T.picture ? T.picture('made|' + url, read) : read()).catch(e => { CLOUD.delete(url); throw e; });
    CLOUD.set(url, p); while (CLOUD.size > 400) CLOUD.delete(CLOUD.keys().next().value);
    return p;
  }
  const imageOf = url => new Promise((res, rej) => { const im = new Image(); im.onload = () => res(im); im.onerror = () => rej(new Error('picture')); im.src = url; });
  /** A piece's picture as a data URL (local, or read from the cloud). */
  async function urlOf(poolId, p, box) {
    if (!p) return null;
    if (p.kind === 'local') { const pic = pictureOf(poolId, p.st, box); if (pic) return pic.url; if (!p.made) return null; }
    return labelled(p.made);
  }
  /* MADEV (Paul, 28 Oct 2026: "The thumbnails are missing all of the labels (Back/Front Engraving)"): a picture the cloud keeps from the first drawing (v 1: no labels, the master's pen drawn heavy) that no
     page here can draw again yet is shown with its faces named over it: the same chips, under each face of a picture side by side, beside each of one over the other. Which layout it was is read
     from its size: the first drawing drew a face 192 px high, so a picture up to 200 px high is side by side and one 380 to 400 px high one over the other. A picture that says it is of a later
     drawing (made.v), or of a size the first drawing never made (a later drawing sent before the version was kept), has its chips already: shown as it is. */
  const OLD_TILE = 192;
  const LABELLED = new Map();
  function labelled(made) {
    const faces = Array.isArray(made.faces) ? made.faces.filter(f => f === 'front' || f === 'back') : [];
    if (verOf(made) >= 2 || faces.length < 2 || !doc) return cloudPicture(made.url);
    if (LABELLED.has(made.url)) return LABELLED.get(made.url);
    const job = cloudPicture(made.url).then(u => imageOf(u).then(im => {
      const W = im.naturalWidth, H = im.naturalHeight, row = H <= OLD_TILE + 8, col = !row && H >= 2 * OLD_TILE - 4 && H <= 2 * OLD_TILE + 16;
      if (!row && !col) return u;   // (a later drawing's picture: its chips are in it)
      const cv = canvas(W, H), ctx = cv.getContext('2d'); ctx.drawImage(im, 0, 0);
      const f = Math.max(10, Math.round((row ? H : H / 2) * LABEL_FRAC * 1.05)), ch = Math.round(f * 1.55);
      const chip = (word, cx, top) => {
        ctx.save(); ctx.font = `600 ${f}px ${CHIP_FONT}`; ctx.textAlign = 'center'; ctx.textBaseline = 'alphabetic';
        const w = ctx.measureText(word).width + f * 1.2, x = cx - w / 2, r = ch / 2;
        ctx.fillStyle = CHIP_INK; ctx.beginPath(); ctx.moveTo(x + r, top); ctx.lineTo(x + w - r, top); ctx.arc(x + w - r, top + r, r, -Math.PI / 2, Math.PI / 2); ctx.lineTo(x + r, top + ch); ctx.arc(x + r, top + r, r, Math.PI / 2, Math.PI * 1.5); ctx.closePath(); ctx.fill();
        ctx.fillStyle = CHIP_TEXT; ctx.fillText(word, cx, top + ch / 2 + f * 0.35); ctx.restore();
      };
      faces.slice(0, 2).forEach((face, i) => { const word = face === 'front' ? 'Front' : 'Back'; if (row) chip(word, W * (i ? 0.75 : 0.25), H - ch - Math.round(f * 0.2)); else chip(word, Math.round(f * 2.4), H * (i ? 0.75 : 0.25) - ch / 2); });
      return cv.toDataURL('image/png');
    })).catch(e => { LABELLED.delete(made.url); throw e; });
    LABELLED.set(made.url, job); while (LABELLED.size > 200) LABELLED.delete(LABELLED.keys().next().value);
    return job;
  }
  /** BOXFIX: the grid of a line's pieces in one box of shape `ar` (width over height): 1, 2 side by side (or one over the other in a tall box), 3 and 4 in two by two. */
  const gridOf = (n, ar) => { const cols = n <= 1 ? 1 : n === 2 ? (ar >= 1 ? 2 : 1) : 2; return { cols, rows: Math.ceil(n / cols) }; };
  /** Several pieces' pictures in one box (BW x BH px): each in its own cell of the grid, the cells parted by hairlines; the last cell says +N when the line has more pieces than are shown. */
  async function compose(urls, more, box) {
    const ims = await Promise.all(urls.map(u => (u ? imageOf(u).catch(() => null) : null)));
    if (!ims.some(Boolean)) return null;
    if (urls.length === 1 && !more) return urls[0];
    const BH = box.tile, BW = Math.round(box.tile * box.ar), n = urls.length + (more ? 1 : 0), { cols, rows } = gridOf(n, box.ar), cw = BW / cols, ch = BH / rows, hair = Math.max(1, Math.round(box.tile / box.px));
    const cv = canvas(BW, BH), ctx = cv.getContext('2d');
    ctx.fillStyle = '#fff'; ctx.fillRect(0, 0, cv.width, cv.height);
    ims.forEach((im, i) => { if (!im) return; const c = i % cols, r = Math.floor(i / cols), k = Math.min(cw / im.naturalWidth, ch / im.naturalHeight), w = im.naturalWidth * k, h = im.naturalHeight * k; ctx.drawImage(im, c * cw + (cw - w) / 2, r * ch + (ch - h) / 2, w, h); });
    // (FACES: the pieces parted by the same hairlines as a piece's faces)
    ctx.fillStyle = RULE;
    for (let c = 1; c < cols; c++) ctx.fillRect(Math.round(c * cw - hair / 2), Math.round(BH * 0.03), hair, Math.round(BH * 0.94));
    for (let r = 1; r < rows; r++) ctx.fillRect(Math.round(BW * 0.03), Math.round(r * ch - hair / 2), Math.round(BW * 0.94), hair);
    if (more) { const i = urls.length, c = i % cols, r = Math.floor(i / cols); ctx.fillStyle = '#8a8378'; ctx.font = `600 ${Math.round(ch * 0.28)}px system-ui, sans-serif`; ctx.textAlign = 'center'; ctx.textBaseline = 'middle'; ctx.fillText('+' + more, c * cw + cw / 2, r * ch + ch / 2); }
    return cv.toDataURL('image/png');
  }
  const COMPOSED = new Map();
  /** BOXFIX: the box a list's picture element draws into: { px (its inner height, css px), ar (its inner width over that) }, {} before it is laid out. */
  const boxOf = el => { const h = el ? el.clientHeight : 0, w = el ? el.clientWidth : 0; return h > 0 && w > 0 ? { px: h, ar: Math.round(w / h * 1000) / 1000 } : {}; };
  /** The device px this page draws a box `px` css px high at: the screen's density (steps of 32 px, at least TILE, at most 640). */
  const tileFor = px => { const d = safe(() => +root.devicePixelRatio || 1, 1); return Math.max(TILE, Math.min(640, Math.ceil((+px > 0 ? +px : BOX_PX) * Math.max(1, d) / 32) * 32)); };
  /** The picture of a list box for a line: { key, load } (load → data URL), or null when no piece of the line has anything of its own (the Vector design stands). opts = { px (the box's height in css px),
   *  ar (its width over its height) }: BOXFIX, the picture is drawn to the box's own shape, a line of several pieces in a grid of cells of that box. */
  function forLine(row, opts) {
    if (opts && opts.nodeType) opts = boxOf(opts);
    const px = +(opts && opts.px) > 0 ? +opts.px : BOX_PX, ar = Math.max(0.5, Math.min(3, +(opts && opts.ar) || BOX_AR)), tile = tileFor(px);
    const ids = (safe(() => host.poolIdsOf && host.poolIdsOf(row), []) || []).filter(Boolean); if (!ids.length) return null;
    const shown = ids.length > MAX_PIECES ? ids.slice(0, MAX_PIECES - 1) : ids, more = ids.length - shown.length;
    const picks = shown.map(id => pick(id)); if (!picks.some(Boolean)) return null;
    // (a piece of the line with nothing of its own, beside one that has: its plain front, when this page holds its charm)
    const plain = id => { const c = safe(() => host.charmOf && host.charmOf(id), null); return c ? { kind: 'plain', sig: 'p' + fnv(String(c.hash || '') + (c.mirror ? 'm' : '')) } : null; };
    const parts = picks.map((p, i) => p || (shown.length > 1 ? plain(shown[i]) : null));
    const { cols, rows } = gridOf(shown.length + (more ? 1 : 0), ar), box = { tile, ar, px }, cell = { tile: Math.round(tile / rows), ar: ar * rows / cols, px: px / rows };
    const pbox = shown.length > 1 || more ? cell : box, bk = '@' + pbox.tile + 'x' + pbox.ar.toFixed(3);
    const key = 'made:' + parts.map(p => (!p ? '-' : p.kind === 'cloud' ? 'c' + p.made.url : p.kind === 'local' ? 'l' + p.sig + bk : p.sig + bk)).join('|') + (more ? '+' + more : '') + '#' + tile + 'x' + ar.toFixed(3);
    const load = () => {
      if (COMPOSED.has(key)) return COMPOSED.get(key);
      const job = Promise.all(parts.map((p, i) => (!p ? null : p.kind === 'plain' ? plainUrl(shown[i], pbox) : urlOf(shown[i], p, pbox).catch(() => null))))
        .then(urls => (urls.length === 1 && !more ? urls[0] : compose(urls, more, box))).catch(e => { COMPOSED.delete(key); throw e; });
      COMPOSED.set(key, job); while (COMPOSED.size > 200) COMPOSED.delete(COMPOSED.keys().next().value);
      return job;
    };
    return { key, load, pieces: ids.length, busy: busyAny(ids), faces: picks.some(p => p && (p.kind === 'local' ? p.st.tiles.length > 1 : (p.made.faces || []).length > 1)) ? 'both' : 'one' };
  }
  function plainUrl(poolId, box) {
    const st = { tiles: [{ face: 'front', job: null, words: null }], sig: 'plain|' + poolId + '|' + sigCharm(poolId) };
    const pic = pictureOf(poolId, st, box); return pic ? pic.url : null;
  }
  const sigCharm = id => { const c = safe(() => host.charmOf(id), null); return c ? String(c.hash || '') : ''; };
  /** One piece's own picture (the order window's ear rows, its sheet panel, the sheet window's piece): { key, load(box?) } or null. box = { tile, ar, px }: this page's own drawing is made for that
   *  box; the cloud's is what it is. */
  function forPiece(poolId) {
    const p = pick(poolId); if (!p) return null;
    return { key: 'made:' + (p.kind === 'cloud' ? 'c' + p.made.url : 'l' + p.sig), local: p.kind === 'local', load: box => urlOf(poolId, p, box) };
  }

  /** A piece's picture as it will be made, painted over a canvas that shows the piece (the order window's sheet panel, the sheet window's piece: they draw the charm alone, at once): the canvas is
   *  cleared to white and the picture laid in it (contained, as the charm was), when the piece has one. → Promise<boolean>. The canvas remembers what it was asked for: an older answer paints nothing. */
  function paintInto(cv, poolId) {
    const m = poolId && doc && cv && cv.getContext ? forPiece(poolId) : null; if (!m) return Promise.resolve(false);
    if (cv._madeKey === m.key && cv.dataset.made === '1') return Promise.resolve(true);   // (painted with this very picture already: a look again changes nothing, and says nothing)
    const ask = cv._madeAsk = {}; cv.dataset.madePid = String(poolId);   // (PICBUSY: found again when the piece moves: painted again, its box says Updating meanwhile: ListMedia.refresh / busy)
    const t = Math.max(TILE, Math.min(1200, cv.height)), ar = cv.width / Math.max(1, cv.height), box = cv.parentElement || cv, B_ = root && root.CNPicBusy;
    if (B_) B_.on(box, 'paint');   // (PICBUSY: Updating over the charm drawn alone until the piece as made is laid over it)
    return Promise.resolve(m.load({ tile: t, ar, px: t / 2 })).then(u => (u ? imageOf(u) : null)).finally(() => { if (B_) B_.off(box, 'paint'); }).then(im => {
      if (!im || cv._madeAsk !== ask) return false;
      const ctx = cv.getContext('2d'), k = Math.min(cv.width / im.naturalWidth, cv.height / im.naturalHeight) * 0.92, w = im.naturalWidth * k, h = im.naturalHeight * k;
      ctx.save(); ctx.setTransform(1, 0, 0, 1, 0, 0); ctx.fillStyle = '#fff'; ctx.fillRect(0, 0, cv.width, cv.height); ctx.drawImage(im, (cv.width - w) / 2, (cv.height - h) / 2, w, h); ctx.restore();
      cv.dataset.made = '1'; cv._madeKey = m.key; return true;
    }).catch(() => false);
  }

  let retryT = 0, retries = 0;
  const retrySoon = () => { if (retryT || retries > 20) return; retries++; retryT = setTimeout(() => { retryT = 0; safe(() => host.refresh && host.refresh(null)); }, 1500); };

  /* ── what moved: the sweep ── */
  const SIGS = new Map(), quiet = new Map(), touched = new Set(), redrawn = new Set();
  let sweepT = 0, refreshT = 0, first = true;
  /** Something may have moved (poolIds: the pieces, when known): looked at SWEEP ms later, once for a burst. */
  function changed(why, ids) {
    for (const id of ids || []) touched.add(String(id));
    IDX = null;
    if (!sweepT) sweepT = setTimeout(sweep, SWEEP);
  }
  /** These pieces changed on another computer (this page only heard of it): their next move is not sent from here. */
  function adopt(ids) { const until = Date.now() + 10000; for (const id of ids || []) quiet.set(String(id), until); changed('adopt', ids); }
  const isQuiet = id => { const t = quiet.get(id); if (!t) return false; if (t < Date.now()) { quiet.delete(id); return false; } return true; };
  function sweep() {
    sweepT = 0; IDX = null;
    const ids = new Set([...index().keys(), ...touched]); touched.clear();
    const moved = [], states = new Map();
    for (const id of ids) {
      const st = stateOf(id), sig = st ? st.sig : '', prev = SIGS.get(id); states.set(id, st);
      if (prev === undefined) {
        SIGS.set(id, sig);
        if (!sig || isQuiet(id)) continue;
        // (first seen on this page: sent only when the cloud has no picture of it while its row is held here, or the cloud's was sent from here and is older: never over another computer's)
        const made = cloudOf(id);
        if ((!made && known(id)) || (made && verOf(made) < V) || (made && made.dev === dev() && made.sig !== sig)) want(id, true);   // (MADEV: a picture of an older drawing is drawn again by any page that can, paced)
        if (!first) moved.push(id);   // (a piece restored or pooled since the page began: its boxes look again)
      } else if (prev !== sig) {
        SIGS.set(id, sig); moved.push(id);
        if (isQuiet(id)) { quiet.delete(id); continue; }
        mine.add(id); want(id, false);
      } else if (sig && !pend.has(id) && !redrawn.has(id)) {
        // (MADEV: a piece this page knows whose cloud picture is of an older drawing (one the feed brought since, or one this page saw before it could draw the piece): drawn again and sent, once, paced)
        const made = cloudOf(id); if (made && verOf(made) < V) { redrawn.add(id); want(id, true); }
      }
    }
    first = false;
    if (moved.length) { safe(() => host.refresh && host.refresh(moved)); }
    busySweep(states, new Set(moved));
    while (SIGS.size > 6000) SIGS.delete(SIGS.keys().next().value);
    for (const f of [...LISTEN]) safe(() => f(moved));
  }
  /* ── PICBUSY (Paul, 28 Oct 2026: "There must be feedback spinners everywhere. The user has to wait for the system to update"): the boxes of a piece being saved (expect: from the press of the
     Charm Editor's Save, or the moment a save lands) or fitted again (its face busy in the fit worker) say Updating (host.busy: charm-nest-pic-busy.js, the reason 'piece'), and stop saying it the moment
     the piece's drawing moved (the box's own new picture then says it until it is painted: ListMedia watch, 'load'), the save failed (release) or nothing came of it (PENDING_MS). ── */
  const PENDING = new Map(), HELD = new Set(), LISTEN = new Set(), PENDING_MS = 30000;
  /** These pieces are being saved (opts.landed: their record has just landed: the next look lets go even when the drawing did not move, so an edit that changes nothing leaves no ring). */
  function expect(ids, opts) {
    const until = Date.now() + PENDING_MS, list = (ids || []).map(String).filter(Boolean); if (!list.length) return;
    for (const id of list) { PENDING.set(id, { until, released: false, landed: !!(opts && opts.landed) }); HELD.add(id); }
    safe(() => host.busy && host.busy(list, true));
    setTimeout(() => changed('expect', list), PENDING_MS + 50);
  }
  function release(ids) { for (const id of (ids || []).map(String)) { const e = PENDING.get(id); if (e) e.released = true; } changed('release', ids); }
  function busySweep(states, moved) {
    const now = Date.now(), on = [], off = [];
    for (const id of new Set([...states.keys(), ...PENDING.keys(), ...HELD])) {
      const pe = PENDING.get(id);
      if (pe && (moved.has(id) || pe.released || pe.landed || pe.until <= now)) PENDING.delete(id);
      const st = states.has(id) ? states.get(id) : stateOf(id), hold = !!(st && st.busy) || PENDING.has(id);
      if (hold) { on.push(id); HELD.add(id); } else if (HELD.has(id)) { off.push(id); HELD.delete(id); }
    }
    if (on.length) safe(() => host.busy && host.busy(on, true));
    if (off.length) safe(() => host.busy && host.busy(off, false));
  }
  /** Whether any of these pieces is being saved or fitted again (a box drawn now says Updating at once). */
  const busyAny = ids => (ids || []).some(id => HELD.has(String(id)));
  /** Told after every look at what moved (the engraving cards' pictures that wait for a face to be fitted look again): → stop(). */
  function onSweep(fn) { LISTEN.add(fn); return () => LISTEN.delete(fn); }
  /** The boxes on screen look again (the cloud's rows moved: OrderPieces told so). */
  function refreshSoon() { if (refreshT) return; refreshT = setTimeout(() => { refreshT = 0; safe(() => host.refresh && host.refresh(null)); changed('cloud'); }, REFRESH); }   // (MADEV: and a look at what the cloud's rows say of the pieces this page can draw)

  /* ── to the cloud: one piece at a time, once its moves have stopped ── */
  const pend = new Map();                       // poolId → { first, due, backfill }
  const stats = { puts: 0, clears: 0, fails: 0, skipped: 0, last: null };
  let sending = false, pumpT = 0, lastSent = 0;
  function want(id, backfill) {
    const now = Date.now(), p = pend.get(id) || { first: now, backfill: !!backfill };
    if (!backfill) p.backfill = false;
    p.due = p.backfill ? now + PACE : Math.min(p.first + MAX_WAIT, now + DEBOUNCE);
    pend.set(id, p); arm();
  }
  function arm() {
    clearTimeout(pumpT); pumpT = 0; if (sending || !pend.size) return;
    const next = Math.min(...[...pend.values()].map(p => p.due));
    pumpT = setTimeout(pump, Math.max(0, next - Date.now()));
  }
  async function pump() {
    pumpT = 0; if (sending) return;
    const now = Date.now(); let id = null, p = null;
    for (const [k, v] of pend) if (v.due <= now && (!p || v.due < p.due)) { id = k; p = v; }
    if (!id) return arm();
    if (p.backfill && now - lastSent < PACE) { p.due = lastSent + PACE; return arm(); }   // (pieces sent because the cloud had none: one every PACE, never a burst)
    const st = stateOf(id);
    if (st && st.busy) { p.due = now + DEBOUNCE; return arm(); }   // (its face is being fitted again: the picture waits for the words' new place)
    pend.delete(id);
    sending = true;
    try { await send(id, st); } catch (e) { stats.fails++; stats.last = String((e && e.message) || e).slice(0, 160); } finally { sending = false; lastSent = Date.now(); arm(); }
  }
  async function send(id, st) {
    if (!host.api) return;
    const made = cloudOf(id);
    if (st && made && made.sig === st.sig && verOf(made) >= V) { stats.skipped++; return; }
    if (!st && !made) { stats.skipped++; return; }
    const pic = st ? pictureOf(id, st) : null;
    if (st && !pic) { const p = pend.get(id) || { first: Date.now(), backfill: true }; if ((p.tries = (p.tries || 0) + 1) <= 8) { p.due = Date.now() + 5000; pend.set(id, p); } stats.skipped++; return; }   // (a shelved face whose font is loading: tried again shortly)
    const by = String(safe(() => host.who && host.who(), '') || '').trim() || null;
    const body = st ? { op: 'madeThumbPut', poolId: id, png: pic.url, sig: st.sig, faces: pic.faces, by, dev: dev(), v: V } : { op: 'madeThumbPut', poolId: id, clear: true, by, dev: dev() };
    const res = await host.api('charmNestLibrary', body, { quiet: true, timeoutMs: 30000 });
    if (!res || !res.ok) throw new Error((res && res.error) || 'the picture was not kept');
    if (st) stats.puts++; else stats.clears++;
    safe(() => host.keepMade && host.keepMade(id, res.made || null));
  }
  /** Send what waits now (a test; the page going away). */
  function flush() { for (const p of pend.values()) p.due = 0; arm(); }
  function state() { return Object.assign({ pending: pend.size, sending, sigs: SIGS.size, mine: mine.size, dev: dev() }, stats); }
  function setHost(h) { host = Object.assign({}, h || {}); IDX = null; return host; }

  return { V, verOf, setHost, changed, adopt, refreshSoon, forLine, forPiece, paintInto, tileFor, boxOf, expect, release, onSweep, isBusy: busyAny, sigOf, stateOf, pictureOf, flush, state, host: () => host, _sweep: sweep };
});
