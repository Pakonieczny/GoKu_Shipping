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
  function markup(backs, stock = {}) {
    if (!backs?.length) return '';
    return `<section class="sheetBacks" aria-label="Back engravings" data-stock-w="${+stock.wPt || 0}" data-stock-h="${+stock.hPt || 0}"><div class="backPieces">${backs.map(b => {
      const png = previewCache.get(previewKey(b)) || previewUrl(b.preview || b.outputs?.png?.url || b.png);
      const identity = `${b.order || ''} · ${b.sku || ''} · copy ${b.copy || String(b.poolId).split('_').pop()}`;
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
        scale = Math.min(front.clientWidth / sw, front.clientHeight / sh);
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
        if (!shelf) return;
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
    new MutationObserver(schedule).observe(document.body, {childList:true, subtree:true}); sync();
  }
  if (typeof document !== 'undefined') {
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', mount, {once:true}); else mount();
  }
  return {placedIds, forSheet, markup, dimensions, previewUrl, recoverPreview};
});
