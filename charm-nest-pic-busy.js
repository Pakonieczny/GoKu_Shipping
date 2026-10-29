/* charm-nest-pic-busy.js — ONE quiet "updating" mark for every picture that is being drawn again (PICBUSY, Paul, 28 Oct 2026: "when these thumbnails are being updated with the charm editor
   updates, there is no loading spinners to tell the user and give the user feedback that something is about to change ... There must be feedback spinners everywhere. The user has to wait for
   the system to update.")

   A picture box that is waiting for its new drawing keeps the picture it has (dimmed a little, never blank) and shows a small gold ring in its top right corner, role="status", "Updating…" as its
   name and tooltip, aria-busy on the box. It is put on and taken off by the work itself, never by a clock: each reason (a picture being read or drawn, a piece being saved or fitted again) is
   added with on(el, reason) and removed with off(el, reason); the mark is there while any reason is. A box whose work never answers lets go after SAFETY_MS (a picture that fails shows its own
   fallback, and no ring turns for ever). prefers-reduced-motion: the ring stands still.

   window.CNPicBusy: on(el, reason?) · off(el, reason?) (no reason: every reason) · sync(el) (after the box's children were replaced: the ring is put back when still owed) · isOn(el) · count() · why(el)
   · inline() (the same ring inside a button's words: the Charm Editor's Save while it is checked) */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === 'object' && module.exports) module.exports = api;
  if (root) root.CNPicBusy = api;
})(typeof self !== 'undefined' ? self : typeof globalThis !== 'undefined' ? globalThis : this, function (root) {
  'use strict';
  const SAFETY_MS = 20000, WORD = 'Updating…';
  const doc = root && root.document || null;
  const reasons = typeof WeakMap === 'function' ? new WeakMap() : new Map();
  const live = new Set();
  const CSS = `.cnPicBusy{position:relative}
.cnPicBusy>img,.cnPicBusy>canvas,.cnPicBusy>svg,.cnPicBusy>.engpv{opacity:.62;transition:opacity .15s ease}
.cnPicSpin{position:absolute;top:5px;right:5px;z-index:4;width:18px;height:18px;display:grid;place-items:center;border-radius:50%;background:rgba(255,255,255,.92);box-shadow:0 0 0 1px rgba(169,130,63,.25);pointer-events:auto}
.cnPicBusy:has(>.egRefitCue)>.cnPicSpin{display:none}
.cnPicSpin>i{display:block;width:10px;height:10px;box-sizing:border-box;border:2px solid rgba(169,130,63,.28);border-top-color:var(--gold2,#a9823f);border-radius:50%;animation:cnPicSpin .8s linear infinite}
@keyframes cnPicSpin{to{transform:rotate(360deg)}}
.cnPicSpinInline{display:inline-block;width:11px;height:11px;box-sizing:border-box;margin-right:6px;vertical-align:-1px;border:2px solid currentColor;border-right-color:transparent;border-radius:50%;animation:cnPicSpin .8s linear infinite}
@media (prefers-reduced-motion:reduce){.cnPicSpinInline{animation:none}.cnPicSpin>i{animation:none;border-color:var(--gold2,#a9823f);border-right-color:rgba(169,130,63,.28)}.cnPicBusy>img,.cnPicBusy>canvas{transition:none}}`;
  function css() {
    if (!doc || doc.getElementById('cnPicBusyCss')) return;
    const s = doc.createElement('style'); s.id = 'cnPicBusyCss'; s.textContent = CSS; (doc.head || doc.documentElement).appendChild(s);
  }
  const setOf = el => { let s = reasons.get(el); if (!s) reasons.set(el, s = new Set()); return s; };
  function sync(el) {
    if (!el || !doc) return false;
    const s = reasons.get(el), on = !!(s && s.size);
    let spin = null; for (const c of el.children || []) if (c.classList && c.classList.contains('cnPicSpin')) { spin = c; break; }
    if (on) {
      css(); el.classList.add('cnPicBusy'); el.setAttribute('aria-busy', 'true');
      if (!spin) { spin = doc.createElement('span'); spin.className = 'cnPicSpin'; spin.setAttribute('role', 'status'); spin.setAttribute('aria-label', WORD); spin.title = WORD; spin.innerHTML = '<i aria-hidden="true"></i>'; el.appendChild(spin); }
      live.add(el);
      // (a box whose pictures are put in again while it waits (its children replaced): the ring is put back at once, as long as it is owed)
      if (!el._cnPicMO && typeof root.MutationObserver === 'function') { el._cnPicMO = new root.MutationObserver(() => { const r = reasons.get(el); if (r && r.size && !el.querySelector(':scope>.cnPicSpin')) sync(el); }); el._cnPicMO.observe(el, { childList: true }); }
    } else {
      if (el._cnPicMO) { el._cnPicMO.disconnect(); el._cnPicMO = null; }
      el.classList.remove('cnPicBusy'); if (el.getAttribute('aria-busy') === 'true' && !el.querySelector('.thumbLoading')) el.removeAttribute('aria-busy');
      if (spin) spin.remove();
      live.delete(el); clearTimeout(el._cnPicBusyT); el._cnPicBusyT = 0;
    }
    return on;
  }
  function on(el, reason) {
    if (!el || !doc) return;
    setOf(el).add(String(reason || 'busy'));
    clearTimeout(el._cnPicBusyT); el._cnPicBusyT = setTimeout(() => { el._cnPicBusyT = 0; const s = reasons.get(el); if (s) s.clear(); sync(el); }, SAFETY_MS);   // (no ring for ever)
    sync(el);
  }
  function off(el, reason) {
    if (!el) return;
    const s = reasons.get(el); if (!s) return;
    if (reason == null) s.clear(); else s.delete(String(reason));
    sync(el);
  }
  /** The same ring in a line of words (a button that waits: the Charm Editor's Save): its markup. */
  const inline = () => { css(); return '<i class="cnPicSpinInline" aria-hidden="true"></i>'; };
  const isOn = el => !!(el && reasons.get(el) && reasons.get(el).size);
  const count = () => { for (const el of [...live]) if (!el.isConnected) { live.delete(el); } return live.size; };
  const why = el => [...((el && reasons.get(el)) || [])];   // (what a box still waits for: for the tests and the console)
  return { on, off, sync, isOn, count, inline, why, SAFETY_MS, WORD };
});
