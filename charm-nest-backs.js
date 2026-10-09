/* Exact-copy ownership shared by the live bench, Library and persistence. */
(function(root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory();
  else root.CharmNestBacks = factory();
})(typeof self !== 'undefined' ? self : this, function() {
  'use strict';
  function placedIds(sheet) {
    if (Array.isArray(sheet.poolIds)) return new Set(sheet.poolIds);
    const byId = new Map((sheet.charms || []).map(c => [c.id, c.poolId]));
    return new Set((sheet.placements || []).map(p => byId.get(p.id) || p.poolId).filter(Boolean));
  }
  function forSheet(sheet, records) {
    const ids = placedIds(sheet), id = sheet.sheetId || sheet.id;
    const found = new Map();
    for (const b of records || []) {
      if (!b.poolId || !ids.has(b.poolId) || b.invalidated) continue;
      const prev = found.get(b.poolId);
      if (!prev || (+b.approvedAt || 0) >= (+prev.approvedAt || 0))
        found.set(b.poolId, Object.assign({}, b, {sheetId:id, setId:sheet.setId || null}));
    }
    return [...found.values()].sort((a,b) => String(a.poolId).localeCompare(String(b.poolId), undefined, {numeric:true}));
  }
  const esc = v => String(v == null ? '' : v).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const safeUrl = v => /^(https?:\/\/|data:image\/png;base64,)/i.test(v || '') ? v : '';
  const previewCache = new Map(), previewRequests = new Map();
  const previewKey = b => [b.sheetId, b.poolId, +b.approvedAt || 0].join('|');
  async function recoverPreview(identity, resolve) {
    const key = previewKey(identity);
    if (previewCache.has(key)) return previewCache.get(key);
    if (!previewRequests.has(key)) previewRequests.set(key, Promise.resolve().then(() => resolve(identity)).then(result => {
      if (!/^data:image\/png;base64,/i.test(result?.dataUrl || '')) throw new Error('Saved preview unavailable');
      if (identity.approvedAt && +result.approvedAt !== +identity.approvedAt) throw new Error('Engraving changed');
      previewCache.set(key, result.dataUrl);
      while (previewCache.size > 200) previewCache.delete(previewCache.keys().next().value);
      return result.dataUrl;
    }).finally(() => previewRequests.delete(key)));
    return previewRequests.get(key);
  }
  // The station uses COEP require-corp. Storage previews must load in CORS mode,
  // with the same separate cache key as front previews (plain cached responses
  // may lack Access-Control-Allow-Origin). Inline approval previews stay local.
  function previewUrl(value) {
    if(typeof CharmNestAssets!=='undefined')return CharmNestAssets.url(safeUrl(value));
    const url = safeUrl(value);
    if (!/^https?:\/\//i.test(url)) return url;
    try {
      const parsed = new URL(url);
      if (parsed.hostname !== 'firebasestorage.googleapis.com') return url;
      parsed.searchParams.set('c', '1');
      return parsed.href;
    } catch (_) { return ''; }
  }
  function dimensions(b) {
    const pt = 72 / 25.4, pad = 3 * pt;
    // Older saved backs contain the export page size (5 mm padding + cut stroke).
    // New records store the exact preview dimensions, including its 3 mm border.
    const w = +b.previewWPt || (+b.pageWPt > 10 * pt + .5 ? +b.pageWPt - 4 * pt - .5 : 0);
    const h = +b.previewHPt || (+b.pageHPt > 10 * pt + .5 ? +b.pageHPt - 4 * pt - .5 : 0);
    return w > 2 * pad && h > 2 * pad ? {w, h, pad} : null;
  }
  /* ── the words where the laser burns them ───────────────────────────────
     Every sheet view that turns to its Back draws each charm's own engraving with this: the same words, the same place,
     the same size and the same mirroring as the written back file. A back file (charm-nest-pdf buildBackFile §7b) is the
     charm's cut geometry mirrored about its outline's centre (x = cx) and then turned hoop-up by 90° − upAngle about
     that centre (charm-nest-geom backView, steps 3 and 6); the words are laid out in that back frame, at `centre` and
     `angle`, by CharmNestGeom.layoutLines. Undoing those two steps puts each glyph back into the charm's own front
     (source pt) frame, so any drawing of the charm — a plate mirrored as the laser sees it — lays the words exactly
     where they are cut, readable from behind. `glyphs` is null when the engraving font is not loaded yet; `centre`,
     `size` and the words are still true, so the caller can say what is there.                                        */
  const linesOf = b => {
    const l = Array.isArray(b.lines) ? b.lines.map(v => String(v == null ? '' : v)) : String(b.text == null ? '' : b.text).split(/\r?\n/);
    return l.filter(v => v !== '').length ? l : [];
  };
  function engraveOn(back, charm, opts = {}) {
    if (!back || back.invalidated) return null;
    const lines = linesOf(back); if (!lines.length) return null;
    const bb = (charm && charm.outline && charm.outline.bbox) || (charm && charm.bbox) || null;
    const cp = charm && charm.centerPt;
    const cx = bb ? (bb[0] + bb[2]) / 2 : cp ? +cp[0] : 0, cy = bb ? (bb[1] + bb[3]) / 2 : cp ? +cp[1] : 0;
    const angleDeg = back.upAngle == null ? 0 : 90 - +back.upAngle;                 // backView step 6
    const a = -angleDeg * Math.PI / 180, ca = Math.cos(a), sa = Math.sin(a);
    // back frame → the charm's own frame: turn back, then mirror back about x = cx
    const toFront = (x, y) => { const dx = x - cx, dy = y - cy; return [2 * cx - (cx + dx * ca - dy * sa), cy + dx * sa + dy * ca]; };
    const c = Array.isArray(back.centre) && back.centre.length === 2 ? [+back.centre[0], +back.centre[1]] : [cx, cy];
    const size = +back.sizePt || 0, PT = 72 / 25.4;
    const out = { poolId: back.poolId || null, text: lines.join('\n'), lines, size, capPt: +back.capMm > 0 ? +back.capMm * PT : size * 0.66,
      centre: toFront(c[0], c[1]), angleDeg, approvedAt: +back.approvedAt || 0, glyphs: null };
    const G = opts.geom || (typeof CharmNestGeom !== 'undefined' ? CharmNestGeom : typeof window !== 'undefined' ? window.CharmNestGeom : null);
    const font = opts.font || null;
    if (font && G && G.layoutLines && size > 0 && Number.isFinite(c[0]) && Number.isFinite(c[1])) try {
      const L = G.layoutLines(lines, font, size, back.lineGap == null ? null : +back.lineGap, +back.angle || 0, c);
      const pt = (o, kx, ky, x, y) => { const p = toFront(x, y); o[kx] = p[0]; o[ky] = p[1]; };
      out.glyphs = L.glyphs.map(g => ({ cmds: g.cmds.map(k => {
        const o = { type: k.type };
        if (k.type !== 'Z') pt(o, 'x', 'y', k.x, k.y);
        if (k.type === 'C' || k.type === 'Q') pt(o, 'x1', 'y1', k.x1, k.y1);
        if (k.type === 'C') pt(o, 'x2', 'y2', k.x2, k.y2);
        return o;
      }) }));
      if (L.capPt) out.capPt = L.capPt;
    } catch (_) { out.glyphs = null; }
    return out;
  }
  /** Fills one piece's words on a canvas already set up to draw that charm: `tx` is the charm's own source-pt mapping. */
  function drawEngrave(ctx, geo, tx, opts = {}) {
    if (!geo || !geo.glyphs || !geo.glyphs.length) return false;
    ctx.save(); ctx.fillStyle = opts.fill || '#3f3320'; if (opts.alpha != null) ctx.globalAlpha = opts.alpha;
    ctx.beginPath();
    for (const g of geo.glyphs) { let cur = null;
      for (const k of g.cmds) {
        if (k.type === 'M') { const p = tx(k.x, k.y); ctx.moveTo(p[0], p[1]); cur = [k.x, k.y]; }
        else if (k.type === 'L') { const p = tx(k.x, k.y); ctx.lineTo(p[0], p[1]); cur = [k.x, k.y]; }
        else if (k.type === 'C') { const p1 = tx(k.x1, k.y1), p2 = tx(k.x2, k.y2), p = tx(k.x, k.y); ctx.bezierCurveTo(p1[0], p1[1], p2[0], p2[1], p[0], p[1]); cur = [k.x, k.y]; }
        else if (k.type === 'Q' && cur) { const p1 = tx(k.x1, k.y1), p = tx(k.x, k.y); ctx.quadraticCurveTo(p1[0], p1[1], p[0], p[1]); cur = [k.x, k.y]; }
        else if (k.type === 'Z') ctx.closePath();
      }
    }
    ctx.fill('nonzero'); ctx.restore(); return true;
  }
  /** Which piece of its order a back is, in words: "Left ear", "Right ear", "Disc 2"; nothing for a back of a line that is not cut into slots (an old back has no slot). */
  const pieceWords = b => { const k = (b && (b.slot || b.side)) || ''; return k === 'L' ? 'Left ear' : k === 'R' ? 'Right ear' : /^D\d+$/.test(k) ? 'Disc ' + k.slice(1) : ''; };
  function markup(backs, stock = {}) {
    if (!backs?.length) return '';
    return `<section class="sheetBacks" aria-label="Back engravings" data-stock-w="${+stock.wPt || 0}" data-stock-h="${+stock.hPt || 0}"><div class="backPieces">${backs.map(b => {
      const png = previewCache.get(previewKey(b)) || previewUrl(b.preview || b.outputs?.png?.url || b.png);
      const identity = `${b.order || ''} · ${b.sku || ''} · copy ${b.copy || String(b.poolId).split('_').pop()}${pieceWords(b) ? ' · ' + pieceWords(b) : ''}`;
      const dims = dimensions(b), label = 'Back: ' + (b.text || '') + ' — ' + identity;
      return `<figure data-pool-id="${esc(b.poolId)}" data-sheet-id="${esc(b.sheetId)}" data-approved-at="${+b.approvedAt || 0}" data-rid="${esc(b.order)}" ${dims ? `data-preview-w="${dims.w}" data-preview-h="${dims.h}" data-preview-pad="${dims.pad}"` : ''}>${png ? `<button type="button" class="backThumb" aria-label="${esc(label)}"><img crossorigin="anonymous" referrerpolicy="no-referrer" src="${esc(png)}" alt=""></button>` : '<span class="backPending">Preview pending</span>'}${b.pending ? '<span class="backPending">Saving…</span>' : ''}</figure>`;

    }).join('')}</div></section>`;
  }
  // One delegated inspector serves live cards, history, Sets and dialogs. Its
  // top-layer popup cannot be clipped by a horizontal sheet scroller or modal.
  function mount() {
    const observed = new Set(); let frame = 0, active = null, closeTimer = 0;
    const zoom = document.createElement('div'); zoom.className = 'backZoom';
    zoom.setAttribute('popover', 'manual'); zoom.setAttribute('aria-hidden', 'true');
    document.body.appendChild(zoom);
    const resize = section => {
      const host = section.parentElement, next = host?.nextElementSibling;
      const front = next?.matches('img,canvas') ? next : next?.querySelector('canvas,img');
      const sw = +section.dataset.stockW, sh = +section.dataset.stockH;
      let scale = sw > 0 ? section.clientWidth / sw : 0;
      if (front && sw > 0 && sh > 0) {
        const box = front.getBoundingClientRect();
        // a saved picture at true scale sits in its frame's content box (the padding is the rest of the 100 × 50 mm frame)
        const cs = getComputedStyle(front), padX = (parseFloat(cs.paddingLeft) || 0) + (parseFloat(cs.paddingRight) || 0), padY = (parseFloat(cs.paddingTop) || 0) + (parseFloat(cs.paddingBottom) || 0);
        scale = Math.min((front.clientWidth - padX) / sw, (front.clientHeight - padY) / sh);
        // Live canvas includes rulers; saved previews do not.
        if (front.tagName === 'CANVAS' && front._backSheetScale) scale = front._backSheetScale;
        if (!box.width) return;
      }
      for (const fig of section.querySelectorAll('.backPieces figure')) {
        const w = +fig.dataset.previewW, h = +fig.dataset.previewH, pad = +fig.dataset.previewPad;
        const thumb = fig.querySelector('.backThumb'), img = thumb?.querySelector('img');
        if (!img || !scale || !w || !h) continue;
        const k = Math.min(scale * .9, section.clientWidth / (w - 2 * pad));
        const width = (w - 2 * pad) * k, height = (h - 2 * pad) * k;
        fig.style.setProperty('--back-width', width + 'px');
        fig.style.setProperty('--back-height', height + 'px');
        const slot = Math.max(0, (section.clientWidth - 15) / 6 - width / .9);
        fig.style.marginRight = slot * .34 + 'px';
        img.style.cssText = `width:${w*k}px;height:${h*k}px;max-width:none;left:${-pad*k}px;top:${-pad*k}px`;
      }
    };
    const ro = new ResizeObserver(() => schedule());
    const sync = () => {
      frame = 0;
      for (const s of observed) if (!s.isConnected) { ro.unobserve(s); observed.delete(s); }
      document.querySelectorAll('.sheetBacks').forEach(s => { if (!observed.has(s)) { observed.add(s); ro.observe(s); } resize(s); s.querySelectorAll('.backThumb img').forEach(img => { if (img.complete && !img.naturalWidth && !img.dataset.recovered) recover(img); }); });
      // Align populated shelves only within their actual responsive grid row.
      // Empty cards never inherit the height of another card's engraving shelf.
      const rows = new Map();
      document.querySelectorAll('.sheets .sheetCard').forEach(card => {
        const shelf = card.querySelector('[data-r="backs"]>.sheetBacks');
        if (!shelf || shelf.parentElement.getAttribute('data-eng') === 'closed') return;   // (a collapsed shelf takes no part in its row: EngravingToggle)
        const key = card.offsetTop;
        if (!rows.has(key)) rows.set(key, []);
        rows.get(key).push(shelf);
      });
      for (const shelves of rows.values()) {
        const height = Math.ceil(Math.max(...shelves.map(s => s.firstElementChild.getBoundingClientRect().height)) + 6) + 'px';
        for (const shelf of shelves) if (shelf.style.getPropertyValue('--back-row-height') !== height) shelf.style.setProperty('--back-row-height', height);
      }
      if (active && !active.isConnected) hide();
    };
    const schedule = () => { if (!frame) frame = requestAnimationFrame(sync); };
    async function recover(img, manual = false) {
      const fig = img.closest('.backPieces figure'), thumb = img.closest('.backThumb');
      if (!fig || !thumb || img.dataset.recovering || (!manual && img.dataset.recovered)) return;
      img.dataset.recovering = '1'; img.dataset.recovered = '1'; img.hidden = true;
      const note = thumb.querySelector('.backPending') || document.createElement('span');
      note.className = 'backPending'; note.textContent = 'Loading preview…'; thumb.appendChild(note);
      try {
        const src = await recoverPreview({sheetId:fig.dataset.sheetId, poolId:fig.dataset.poolId, approvedAt:+fig.dataset.approvedAt || 0}, id => window.Engrave.loadBackPreview(id));
        if (!img.isConnected) return;
        img.src = src; img.hidden = false; delete thumb.dataset.previewFailed; note.remove(); schedule();
      } catch (_) { thumb.dataset.previewFailed = '1'; note.textContent = 'Retry preview'; }
      finally { delete img.dataset.recovering; }
    }
    // Image errors do not bubble. Capture them on every live/history/dialog shelf.
    document.addEventListener('error', e => { if (e.target.matches?.('.backThumb img')) recover(e.target); }, true);
    document.addEventListener('load', e => { if (e.target.matches?.('.backThumb img')) schedule(); }, true);
    function hide() {
      active = null; zoom.classList.remove('visible'); clearTimeout(closeTimer);
      closeTimer = setTimeout(() => { if (!active) { if (zoom.hidePopover) zoom.hidePopover(); else zoom.hidden = true; } }, 180);
    }
    function show(thumb) {
      if (active === thumb) return;
      const source = thumb.querySelector('img'); if (!source?.src || source.hidden || !source.naturalWidth) return;
      clearTimeout(closeTimer); active = thumb;
      const img = new Image(); img.crossOrigin = 'anonymous'; img.referrerPolicy = 'no-referrer'; img.src = source.src; img.alt = source.alt;
      zoom.replaceChildren(img);
      const rect = thumb.getBoundingClientRect(), size = Math.min(250, innerWidth - 24, innerHeight - 24);
      const left = Math.max(12, Math.min(innerWidth - size - 12, rect.left + rect.width / 2 - size / 2));
      const top = rect.top >= size + 14 ? rect.top - size - 10 : Math.min(innerHeight - size - 12, rect.bottom + 10);
      zoom.style.cssText = `width:${size}px;height:${size}px;left:${left}px;top:${Math.max(12,top)}px;transform-origin:${rect.left+rect.width/2-left}px ${rect.top+rect.height/2-top}px`;
      if (zoom.showPopover) { if (!zoom.matches(':popover-open')) zoom.showPopover(); } else zoom.hidden = false;
      requestAnimationFrame(() => { if (active === thumb) zoom.classList.add('visible'); });
    }
    document.addEventListener('click', e => {const t=e.target.closest?.('.backThumb'); if(!t) return; e.preventDefault();e.stopPropagation();hide();if(t.dataset.previewFailed){recover(t.querySelector('img'),true);return;}const f=t.closest('figure');window.Engrave?.openBack(f.dataset.poolId,f.dataset.sheetId);},true);
    document.addEventListener('pointerover', e => { const t=e.target.closest?.('.backThumb'); if(t && e.pointerType !== 'touch') show(t); });
    document.addEventListener('pointerout', e => { if(active && active.contains(e.target) && !active.contains(e.relatedTarget)) hide(); });
    document.addEventListener('focusin', e => { const t=e.target.closest?.('.backThumb'); if(t) show(t); });
    document.addEventListener('focusout', e => { if(active?.contains(e.target)) hide(); });
    document.addEventListener('keydown', e => { if(e.key === 'Escape') hide(); });
    document.addEventListener('scroll', hide, true); window.addEventListener('resize', () => {hide(); schedule();});
    // Text that changes several times a second (the progress line, a sheet's stage line and its live pills, toasts) moves
    // no shelf: re-measuring every card for it forced a layout about four times a second while anything was busy, even in
    // a hidden tab (audit, 25 Sep).
    const busyText = '.cnp, .toasts, [data-r="stage"], [data-r="overlay"]';
    const moves = m => { const n = m.target.nodeType === 1 ? m.target : m.target.parentElement; return !n || !n.closest(busyText); };
    new MutationObserver(list => { if (list.some(moves)) schedule(); }).observe(document.body, {childList:true, subtree:true}); sync();
  }
  if (typeof document !== 'undefined') {
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', mount, {once:true}); else mount();
  }
  return {placedIds, forSheet, markup, pieceWords, dimensions, previewUrl, recoverPreview, engraveOn, drawEngrave};
});
