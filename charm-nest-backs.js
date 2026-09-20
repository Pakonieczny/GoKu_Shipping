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
  function markup(backs) {
    if (!backs?.length) return '';
    return `<section class="sheetBacks" aria-label="Back engravings"><div class="backLabel">Back engraving <span>${backs.length}</span></div><div class="backPieces">${backs.map(b => {
      const png = previewUrl(b.preview || b.outputs?.png?.url || b.png), ai = safeUrl(b.outputs?.ai?.url || b.ai);
      const identity = `${b.order || ''} · ${b.sku || ''} · copy ${b.copy || String(b.poolId).split('_').pop()}`;
      return `<figure data-pool-id="${esc(b.poolId)}" title="${esc(identity + '\n' + (b.text || '') + (b.pending ? '\nSaving…' : ''))}">${png ? `<img crossorigin="anonymous" referrerpolicy="no-referrer" src="${esc(png)}" alt="${esc('Back: ' + (b.text || '') + ' — ' + identity)}">` : '<span>Preview pending</span>'}<figcaption>${ai ? `<a href="${esc(ai)}" target="_blank" rel="noopener" onclick="event.stopPropagation()">` : ''}${esc(b.order)} · ${esc(b.copy || String(b.poolId).split('_').pop())}${ai ? '</a>' : ''}${b.pending ? ' · saving' : ''}</figcaption></figure>`;
    }).join('')}</div></section>`;
  }
  return {placedIds, forSheet, markup};
});
