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
  const V = 1;                                   // the drawing's own version: a change of how it is drawn is a new signature (every piece is sent again once, when it moves)
  const TILE = 192, GAP = 12, MARGIN = 0.06;     // a face is drawn TILE px high (a 88 px box at twice the screen's density), the faces GAP apart
  const DEBOUNCE = 1500, MAX_WAIT = 6000, PACE = 1500, SWEEP = 120, REFRESH = 200;
  const MAX_PIECES = 4, INK = '#1c1a17', METAL = '#f1ece1', RULE = '#e4ddd0';
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
  const jobsOf = id => index().get(id) || safe(() => host.jobsOfPiece && host.jobsOfPiece(id), null) || {};
  const REFIT = () => (root && root.CNEngraveRefit) || null;
  /** The fitted words of a face: { view, fit } with glyphs (the job's own, else the drawing held while it is fitted again after a charm edit), null when the face has none. */
  function wordsOf(j) {
    if (!j || j.archived || j.editingBack || ['none', 'skipped'].includes(j.state)) return null;
    let view = j.view, fit = j.fit;
    if (!(fit && view)) { const h = safe(() => { const R = REFIT(); return R && R.held ? R.held(j) : null; }, null); if (h && h.view && h.fit) { view = h.view; fit = h.fit; } }
    return fit && view && Array.isArray(fit.glyphs) && fit.glyphs.length ? { view, fit } : null;
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
    const fitSig = w => (w ? [(w.fit.lines || []).join('\n'), r2(w.fit.size), (w.fit.centre || []).map(r2), r2(w.fit.angle), w.fit.glyphs.length, String(w.fit.weight || '')] : 0);
    const viewSig = w => (w ? [w.view.face || '', (w.view.R || []).map(r2).join(','), w.view.M ? 1 : 0, (w.view.members || []).length] : 0);
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
  /** The picture of one piece: its faces side by side, each TILE px high. → { url (PNG data URL), w, h, faces } or null. Kept by signature. */
  const LOCAL = new Map();
  function pictureOf(poolId, st, tile) {
    st = st || stateOf(poolId); if (!st || !doc) return null;
    const T = Math.max(48, Math.min(1200, Math.round(+tile || TILE))), ck = st.sig + (T === TILE ? '' : '@' + T);
    const hit = LOCAL.get(ck); if (hit) return hit;
    const P = PDF(); if (!P || !P.drawCharm || !P.pathToCanvas || !EP()) return null;
    const parts = [];
    for (const t of st.tiles) {
      const F = frameOf(poolId, t); if (!F || !F.box) continue;
      const w = F.box[2] - F.box[0], h = F.box[3] - F.box[1], pad = Math.max(w, h) * MARGIN;
      let k = T / (h + 2 * pad); if ((w + 2 * pad) * k > T * 2.2) k = T * 2.2 / (w + 2 * pad);
      parts.push({ t, F, pad, k, w: (w + 2 * pad) * k, h: (h + 2 * pad) * k });
    }
    if (!parts.length) return null;
    // (the faces side by side, or one over the other for a long piece laid flat (a bar, a tag): whichever is nearer a square, as the list's boxes are)
    const gap = GAP * T / TILE, sum = k => parts.reduce((a, p) => a + p[k], 0) + gap * (parts.length - 1), mx = k => Math.max(...parts.map(p => p[k]));
    const ratio = (w, h) => Math.max(w, h) / Math.max(1, Math.min(w, h)), col = parts.length > 1 && ratio(mx('w'), sum('h')) < ratio(sum('w'), mx('h'));
    const W = col ? mx('w') : sum('w'), H = col ? sum('h') : mx('h');
    const cv = canvas(W, H), ctx = cv.getContext('2d'); if (!ctx) return null;
    ctx.fillStyle = '#fff'; ctx.fillRect(0, 0, cv.width, cv.height);
    let ox = 0, oy0 = 0;
    for (const [i, p] of parts.entries()) {
      const { F, pad, k } = p, oy = col ? oy0 : (H - p.h) / 2, x0 = F.box[0], y1 = F.box[3], X = col ? (W - p.w) / 2 : ox;
      const tx = (x, y) => [X + (x - x0 + pad) * k, oy + (y1 + pad - y) * k];
      try {
        ctx.beginPath(); P.pathToCanvas(ctx, F.outline, tx); for (const m of F.cut || []) if (m !== F.outline && m.closed) P.pathToCanvas(ctx, m, tx);
        ctx.fillStyle = METAL; ctx.fill('evenodd');
        P.drawCharm(ctx, { outline: F.outline, members: (F.art || []).concat(F.cut || []) }, tx, k);
        // (the words only on the frame they were fitted in: the job's own view, the one frameFor drew)
        if (p.t.words && F.kind === 'view') glyphs(ctx, p.t.words.fit.glyphs, tx);
      } catch (e) { safe(() => console.warn('made thumbnail', e)); }
      if (i) { ctx.fillStyle = RULE; const t = Math.max(1, Math.round(T / TILE)); if (col) ctx.fillRect(Math.round(W * 0.12), Math.round(oy0 - gap / 2), Math.round(W * 0.76), t); else ctx.fillRect(Math.round(ox - gap / 2), Math.round(H * 0.12), t, Math.round(H * 0.76)); }
      ox += p.w + gap; oy0 += p.h + gap;
    }
    const out = { url: cv.toDataURL('image/png'), w: cv.width, h: cv.height, faces: parts.map(p => p.t.face), layout: col ? 'column' : 'row' };
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
  /** Which picture a piece shows: { kind: 'local', sig, st } | { kind: 'cloud', made } | null. */
  function pick(poolId) {
    const st = stateOf(poolId), made = cloudOf(poolId);
    if (st && (!made || made.sig === st.sig || mine.has(poolId) || made.dev === dev())) return { kind: 'local', sig: st.sig, st };
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
  async function urlOf(poolId, p) {
    if (!p) return null;
    if (p.kind === 'local') { const pic = pictureOf(poolId, p.st); return pic ? pic.url : null; }
    return cloudPicture(p.made.url);
  }
  /** Several pieces' pictures in one: a row when every one is a single face and they are few, else two columns; at most MAX_PIECES (the last cell says +N). */
  async function compose(urls, more) {
    const ims = (await Promise.all(urls.map(u => (u ? imageOf(u).catch(() => null) : null)))).filter(Boolean);
    if (!ims.length) return null;
    if (ims.length === 1 && !more) return urls.find(Boolean);
    const n = ims.length + (more ? 1 : 0), single = ims.every(im => im.naturalWidth <= im.naturalHeight * 1.4), cols = single && n <= 4 ? n : Math.min(2, n), rows = Math.ceil(n / cols);
    const cw = Math.max(...ims.map(im => im.naturalWidth)), ch = Math.max(...ims.map(im => im.naturalHeight)), g = GAP * 1.5;
    const cv = canvas(cols * cw + (cols - 1) * g, rows * ch + (rows - 1) * g), ctx = cv.getContext('2d');
    ctx.fillStyle = '#fff'; ctx.fillRect(0, 0, cv.width, cv.height);
    ims.forEach((im, i) => { const c = i % cols, r = Math.floor(i / cols), k = Math.min(cw / im.naturalWidth, ch / im.naturalHeight), w = im.naturalWidth * k, h = im.naturalHeight * k; ctx.drawImage(im, c * (cw + g) + (cw - w) / 2, r * (ch + g) + (ch - h) / 2, w, h); });
    if (more) { const i = ims.length, c = i % cols, r = Math.floor(i / cols); ctx.fillStyle = '#8a8378'; ctx.font = `600 ${Math.round(ch * 0.28)}px system-ui, sans-serif`; ctx.textAlign = 'center'; ctx.textBaseline = 'middle'; ctx.fillText('+' + more, c * (cw + g) + cw / 2, r * (ch + g) + ch / 2); }
    return cv.toDataURL('image/png');
  }
  const COMPOSED = new Map();
  /** The picture of a list box for a line: { key, load } (load → data URL), or null when no piece of the line has anything of its own (the Vector design stands). */
  function forLine(row) {
    const ids = (safe(() => host.poolIdsOf && host.poolIdsOf(row), []) || []).filter(Boolean); if (!ids.length) return null;
    const shown = ids.length > MAX_PIECES ? ids.slice(0, MAX_PIECES - 1) : ids, more = ids.length - shown.length;
    const picks = shown.map(id => pick(id)); if (!picks.some(Boolean)) return null;
    // (a piece of the line with nothing of its own, beside one that has: its plain front, when this page holds its charm)
    const plain = id => { const c = safe(() => host.charmOf && host.charmOf(id), null); return c ? { kind: 'plain', sig: 'p' + fnv(String(c.hash || '') + (c.mirror ? 'm' : '')) } : null; };
    const parts = picks.map((p, i) => p || (shown.length > 1 ? plain(shown[i]) : null));
    const key = 'made:' + parts.map(p => (!p ? '-' : p.kind === 'cloud' ? 'c' + p.made.url : p.kind === 'local' ? 'l' + p.sig : p.sig)).join('|') + (more ? '+' + more : '');
    const load = () => {
      if (COMPOSED.has(key)) return COMPOSED.get(key);
      const job = Promise.all(parts.map((p, i) => (!p ? null : p.kind === 'plain' ? plainUrl(shown[i]) : urlOf(shown[i], p).catch(() => null))))
        .then(urls => (urls.length === 1 && !more ? urls[0] : compose(urls, more))).catch(e => { COMPOSED.delete(key); throw e; });
      COMPOSED.set(key, job); while (COMPOSED.size > 200) COMPOSED.delete(COMPOSED.keys().next().value);
      return job;
    };
    return { key, load, pieces: ids.length };
  }
  function plainUrl(poolId) {
    const st = { tiles: [{ face: 'front', job: null, words: null }], sig: 'plain|' + poolId + '|' + sigCharm(poolId) };
    const pic = pictureOf(poolId, st); return pic ? pic.url : null;
  }
  const sigCharm = id => { const c = safe(() => host.charmOf(id), null); return c ? String(c.hash || '') : ''; };
  /** One piece's own picture (the order window's ear rows, its sheet panel, the sheet window's piece): { key, load(px?) } or null. px: the picture's height on screen, for a drawing made larger
   *  (this page's own drawing is made again at that size, up to 1200 px a face; the cloud's is what it is). */
  function forPiece(poolId) {
    const p = pick(poolId); if (!p) return null;
    return { key: 'made:' + (p.kind === 'cloud' ? 'c' + p.made.url : 'l' + p.sig), local: p.kind === 'local', load: px => (p.kind === 'local' && px ? Promise.resolve((pictureOf(poolId, p.st, px) || {}).url || null) : urlOf(poolId, p)) };
  }

  /** A piece's picture as it will be made, painted over a canvas that shows the piece (the order window's sheet panel, the sheet window's piece: they draw the charm alone, at once): the canvas is
   *  cleared to white and the picture laid in it (contained, as the charm was), when the piece has one. → Promise<boolean>. The canvas remembers what it was asked for: an older answer paints nothing. */
  function paintInto(cv, poolId) {
    const m = poolId && doc && cv && cv.getContext ? forPiece(poolId) : null; if (!m) return Promise.resolve(false);
    const ask = cv._madeAsk = {};
    return Promise.resolve(m.load(Math.round(Math.min(cv.width, cv.height) * 0.6))).then(u => (u ? imageOf(u) : null)).then(im => {
      if (!im || cv._madeAsk !== ask) return false;
      const ctx = cv.getContext('2d'), k = Math.min(cv.width / im.naturalWidth, cv.height / im.naturalHeight) * 0.92, w = im.naturalWidth * k, h = im.naturalHeight * k;
      ctx.save(); ctx.setTransform(1, 0, 0, 1, 0, 0); ctx.fillStyle = '#fff'; ctx.fillRect(0, 0, cv.width, cv.height); ctx.drawImage(im, (cv.width - w) / 2, (cv.height - h) / 2, w, h); ctx.restore();
      cv.dataset.made = '1'; return true;
    }).catch(() => false);
  }

  /* ── what moved: the sweep ── */
  const SIGS = new Map(), quiet = new Map(), touched = new Set();
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
    const moved = [];
    for (const id of ids) {
      const st = stateOf(id), sig = st ? st.sig : '', prev = SIGS.get(id);
      if (prev === undefined) {
        SIGS.set(id, sig);
        if (!sig || isQuiet(id)) continue;
        // (first seen on this page: sent only when the cloud has no picture of it while its row is held here, or the cloud's was sent from here and is older: never over another computer's)
        const made = cloudOf(id);
        if ((!made && known(id)) || (made && made.dev === dev() && made.sig !== sig)) want(id, true);
        if (!first) moved.push(id);   // (a piece restored or pooled since the page began: its boxes look again)
      } else if (prev !== sig) {
        SIGS.set(id, sig); moved.push(id);
        if (isQuiet(id)) { quiet.delete(id); continue; }
        mine.add(id); want(id, false);
      }
    }
    first = false;
    if (moved.length) { safe(() => host.refresh && host.refresh(moved)); }
    while (SIGS.size > 6000) SIGS.delete(SIGS.keys().next().value);
  }
  /** The boxes on screen look again (the cloud's rows moved: OrderPieces told so). */
  function refreshSoon() { if (refreshT) return; refreshT = setTimeout(() => { refreshT = 0; safe(() => host.refresh && host.refresh(null)); }, REFRESH); }

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
    if (st && made && made.sig === st.sig) { stats.skipped++; return; }
    if (!st && !made) { stats.skipped++; return; }
    const pic = st ? pictureOf(id, st) : null;
    if (st && !pic) { stats.skipped++; return; }
    const by = String(safe(() => host.who && host.who(), '') || '').trim() || null;
    const body = st ? { op: 'madeThumbPut', poolId: id, png: pic.url, sig: st.sig, faces: pic.faces, by, dev: dev() } : { op: 'madeThumbPut', poolId: id, clear: true, by, dev: dev() };
    const res = await host.api('charmNestLibrary', body, { quiet: true, timeoutMs: 30000 });
    if (!res || !res.ok) throw new Error((res && res.error) || 'the picture was not kept');
    if (st) stats.puts++; else stats.clears++;
    safe(() => host.keepMade && host.keepMade(id, res.made || null));
  }
  /** Send what waits now (a test; the page going away). */
  function flush() { for (const p of pend.values()) p.due = 0; arm(); }
  function state() { return Object.assign({ pending: pend.size, sending, sigs: SIGS.size, mine: mine.size, dev: dev() }, stats); }
  function setHost(h) { host = Object.assign({}, h || {}); IDX = null; return host; }

  return { V, setHost, changed, adopt, refreshSoon, forLine, forPiece, paintInto, sigOf, stateOf, pictureOf, flush, state, host: () => host, _sweep: sweep };
});
