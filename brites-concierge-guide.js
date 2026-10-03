(function (root, factory) {
  'use strict';
  const api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.BritesConciergeGuide = api;
})(typeof window !== 'undefined' ? window : globalThis, function () {
  'use strict';
  const clamp = (value, min, max) => Math.max(min, Math.min(max, value));
  function placement(target, viewport) {
    const width = Number(viewport.width), height = Number(viewport.height), gap = 14, edge = 12;
    if (!Number.isFinite(width) || !Number.isFinite(height) || width < 240 || height < 280) return null;
    const keys = ['left', 'top', 'right', 'bottom', 'width', 'height'];
    if (!target || keys.some(key => !Number.isFinite(target[key])) || target.width <= 0 || target.height <= 0 || target.bottom <= edge || target.right <= edge || target.top >= height - edge || target.left >= width - edge) return null;
    const w = clamp(width * .17, 144, 210), h = Math.min(244, Math.max(176, w * 1.16));
    const cx = (target.left + target.right) / 2, cy = (target.top + target.bottom) / 2;
    const candidates = [
      {left: target.left - gap - w, top: clamp(cy - h / 2, edge, height - h - edge), side: 'left'},
      {left: target.right + gap, top: clamp(cy - h / 2, edge, height - h - edge), side: 'right'},
      {left: clamp(cx - w / 2, edge, width - w - edge), top: target.top - gap - h, side: 'above'},
      {left: clamp(cx - w / 2, edge, width - w - edge), top: target.bottom + gap, side: 'below'}
    ];
    const free = candidates.filter(c => c.left >= edge && c.top >= edge && c.left + w <= width - edge && c.top + h <= height - edge && (c.left + w <= target.left - 8 || c.left >= target.right + 8 || c.top + h <= target.top - 8 || c.top >= target.bottom + 8));
    return free.length ? {...free[0], width: w, height: h} : null;
  }
  function create(options = {}) {
    const stage = options.stage, panel = options.panel, doc = stage?.ownerDocument, win = options.window || doc?.defaultView;
    if (!stage || !panel || !doc || !win || typeof stage.getBoundingClientRect !== 'function') throw Error('Guide needs the existing avatar stage and panel.');
    let enabled = false, paused = false, destroyed = false, target = null, placeholder = null, originalStyle = null, originalFloating = null, current = null, frame = null;
    const media = win.matchMedia?.('(prefers-reduced-motion: reduce)');
    let reducedMotion = media?.matches === true;
    const notifyFloating = value => {try {options.onFloating?.(value);} catch {}};
    const canGuide = () => enabled && !paused && !reducedMotion && !destroyed && !doc.hidden && !panel.hidden && stage.isConnected;
    function clear() {
      if (frame !== null) {win.cancelAnimationFrame?.(frame); frame = null;}
      if (placeholder) {
        if (originalStyle === null) stage.removeAttribute('style'); else stage.setAttribute('style', originalStyle);
        if (originalFloating === null) stage.removeAttribute('data-floating'); else stage.setAttribute('data-floating', originalFloating);
        placeholder.remove(); placeholder = null; notifyFloating(false);
      }
      target = null; current = null;
    }
    function guide(next) {
      if (!canGuide() || !next || !next.isConnected || typeof next.getBoundingClientRect !== 'function' || next === stage || stage.contains(next)) {clear(); return false;}
      const position = placement(next.getBoundingClientRect(), {width: win.innerWidth, height: win.innerHeight});
      if (!position) {clear(); return false;}
      if (!placeholder) {
        const rect = stage.getBoundingClientRect(); originalStyle = stage.getAttribute('style'); originalFloating = stage.getAttribute('data-floating');
        placeholder = doc.createElement('div'); placeholder.className = 'brites-guide-placeholder'; placeholder.setAttribute('aria-hidden', 'true');
        placeholder.style.cssText = 'height:' + Math.max(0, rect.height) + 'px;min-height:' + Math.max(0, rect.height) + 'px;flex:1 1 ' + Math.max(0, rect.height) + 'px;pointer-events:none;';
        stage.parentNode.insertBefore(placeholder, stage); notifyFloating(true);
      }
      target = next; current = position; stage.dataset.floating = 'true';
      // Keep the very same stage, canvas, WebGL context and media state. The
      // fixed child escapes the panel's normal flex layout without reparenting.
      const style = stage.style;
      style.position = 'fixed'; style.left = position.left + 'px'; style.top = position.top + 'px'; style.width = position.width + 'px'; style.height = position.height + 'px'; style.minHeight = '0'; style.minWidth = '0'; style.maxWidth = 'none'; style.maxHeight = 'none'; style.right = 'auto'; style.bottom = 'auto'; style.flex = 'none'; style.zIndex = '2147483005'; style.pointerEvents = 'none'; style.overflow = 'visible'; style.background = 'transparent';
      try {options.onPlacement?.({...position}, next);} catch {}
      return true;
    }
    function schedule() {
      if (!target || frame !== null) return;
      if (!canGuide()) {clear(); return;}
      if (!win.requestAnimationFrame) {guide(target); return;}
      frame = win.requestAnimationFrame(() => {frame = null; if (target) guide(target);});
    }
    const visibility = () => {if (doc.hidden) clear();};
    const motion = event => {reducedMotion = event.matches === true; if (reducedMotion) clear();};
    const key = event => {if (event.key === 'Escape') clear();};
    doc.addEventListener('visibilitychange', visibility); doc.addEventListener('keydown', key);
    win.addEventListener('resize', schedule); win.addEventListener('scroll', schedule, {capture: true, passive: true}); media?.addEventListener?.('change', motion);
    function setEnabled(value) {enabled = value === true; if (!enabled) clear(); return enabled;}
    function setPaused(value) {paused = value === true; if (paused) clear();}
    function snapshot() {return {enabled, paused, reducedMotion, floating: !!placeholder, placement: current ? {...current} : null, destroyed};}
    function destroy() {if (destroyed) return; clear(); destroyed = true; enabled = false; doc.removeEventListener('visibilitychange', visibility); doc.removeEventListener('keydown', key); win.removeEventListener('resize', schedule); win.removeEventListener('scroll', schedule, true); media?.removeEventListener?.('change', motion);}
    return {setEnabled, setPaused, guide, clear, destroy, snapshot};
  }
  return {create, placement};
});
