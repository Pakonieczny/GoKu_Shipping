/* The small show/hide control for a sheet's back-engraving shelf (Paul, 5 Oct: "Add a colapsable button functionality to hide/show
   all of the back engraving on top of a given sheet … both the Nest tab sheets and the Library Tab Sheets … small and elegant so
   not to crowd the existing UI. By default keep it collapsed").

   What it hides is the SHELF of back-engraving thumbnails above a sheet's picture (CharmNestBacks.markup: .sheetBacks >
   .backPieces, one figure per piece), never the sheet's picture, its files, its saved previews, approvals or the Back ·
   engraving view. It is visual only: the shelf's own host element (a Nest card's [data-r="backs"], a Library card's
   [data-back-sheet]) takes an attribute, data-eng="open" | "closed", and one rule below collapses a closed host. The host
   outlives the shelf's redraws (they replace its children), so a repaint or a live refresh never resets it, and the images
   inside are the ones already loading: no extra request, no redraw, no per-frame work for the sheets that are not moving.

   The choice is per sheet and lives in this page's memory only: nothing is stored, so every page load starts collapsed.

     const t = EngravingToggle.mount(host, { sheetKey, count, getState?, onChange? })   // the control, into an element
         t.update(count, sheetKey?)  — a new count (0 hides the control: no engraving, no control) or another sheet
         t.destroy()
     EngravingToggle.follow(shelfHost, sheetKey | () => sheetKey)   // keeps the shelf host's data-eng in step (idempotent; call it
         after every redraw of the host's own markup; the host's children may be replaced freely)
         → { setKey(k), stop() }
     EngravingToggle.isOpen(sheetKey) · .set(sheetKey, open) · .toggle(sheetKey) · .rekey(from, to) · .countIn(root)
     document event "engravingtoggle" (bubbles to window): detail { sheetKey, open, source } — any picture may listen.
   getState(key) / onChange(open, key) let a host keep the state itself; without them the module's own memory is used. */
(function (root) {
  'use strict';
  const EVENT = 'engravingtoggle';
  const opened = new Set();                       // the sheets whose shelf is open; absent = collapsed (the default, and what a load starts as)
  const mounts = new Set(), followers = new Set();
  const doc = () => root.document || null;
  const keyOf = k => String(k == null ? '' : k);
  const reduced = () => { try { return !!(root.matchMedia && root.matchMedia('(prefers-reduced-motion: reduce)').matches); } catch (_) { return false; } };
  const isOpen = k => opened.has(keyOf(k));

  const CSS = `
.engTog{display:inline-flex;align-items:center;flex:0 0 auto;position:relative;white-space:nowrap}
.engTog[hidden]{display:none}
.engTog.owSeg button{display:inline-flex;align-items:center;gap:4px;padding:2px 7px;line-height:1.2}
.engTog .engIc{width:11px;height:11px;flex:0 0 auto;display:block}
.engTog .engN{font:600 10.5px var(--mono,monospace);font-variant-numeric:tabular-nums;letter-spacing:0}
.engTog .engChev{width:7px;height:7px;flex:0 0 auto;display:block;transition:transform .2s ease}
.engTog button[aria-expanded="true"] .engChev{transform:rotate(180deg)}
.engTog button:focus-visible{outline:2px solid var(--gold2,#caa861);outline-offset:1px}
.engTog .engSr{position:absolute;width:1px;height:1px;margin:-1px;padding:0;overflow:hidden;clip:rect(0 0 0 0);white-space:nowrap;border:0}
@media (max-width:480px){.engTog .engChev{display:none}}
@media (prefers-reduced-motion:reduce){.engTog .engChev{transition:none}}
[data-eng="closed"]{height:0!important;min-height:0!important;margin-top:0!important;margin-bottom:0!important;padding-top:0!important;padding-bottom:0!important;border-width:0!important;overflow:hidden!important;visibility:hidden!important}`;
  let cssIn = false;
  function injectCss() {
    const d = doc(); if (cssIn || !d || !d.head) return; cssIn = true;
    if (d.getElementById('engTogCss')) return;
    const s = d.createElement('style'); s.id = 'engTogCss'; s.textContent = CSS; d.head.appendChild(s);
  }

  const ICON = '<svg class="engIc" viewBox="0 0 12 12" aria-hidden="true" focusable="false"><path d="M2 10.2l.7-2.6L8 2.3a1 1 0 0 1 1.4 0l.3.3a1 1 0 0 1 0 1.4L4.4 9.3z" fill="none" stroke="currentColor" stroke-width="1.1" stroke-linejoin="round"/><path d="M2 11.3h8" fill="none" stroke="currentColor" stroke-width="1.1" stroke-linecap="round"/></svg>';
  const CHEV = '<svg class="engChev" viewBox="0 0 8 8" aria-hidden="true" focusable="false"><path d="M1.2 2.8L4 5.6l2.8-2.8" fill="none" stroke="currentColor" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round"/></svg>';

  function fire(sheetKey, open, source) {
    const d = doc(); if (!d || typeof root.CustomEvent !== 'function') return;
    d.dispatchEvent(new root.CustomEvent(EVENT, { bubbles: true, detail: { sheetKey, open, source: source || 'api' } }));
  }
  /** Set one sheet's shelf open or closed (a no-op when it already is), and tell every control and shelf of that sheet. */
  function set(k, open, source) {
    k = keyOf(k); if (!k) return false; open = !!open;
    if (opened.has(k) === open) return open;
    if (open) opened.add(k); else opened.delete(k);
    fire(k, open, source);
    return open;
  }
  const toggle = (k, source) => set(k, !isOpen(k), source);
  /** A sheet that was known under one key is now known under another (a page saved for the first time): its choice goes with it. */
  function rekey(from, to) {
    from = keyOf(from); to = keyOf(to); if (!from || !to || from === to) return;
    if (opened.has(from)) { opened.delete(from); opened.add(to); }
    for (const m of mounts) if (m.key === from) m.key = to;
    for (const f of followers) if (f.key === from) f.key = to;
  }
  const countIn = r => r && r.querySelectorAll ? r.querySelectorAll('.backPieces figure').length : 0;

  /* ── the shelf host: data-eng follows the sheet's state; a press eases it open or shut, a redraw never animates ── */
  function stopAnim(f) {
    if (f.anim) { const a = f.anim; f.anim = null; try { a.cancel(); } catch (_) {} }
    if (f.el && f.el.style) f.el.style.removeProperty('overflow');
  }
  function paintShelf(f, open, animate) {
    const el = f.el, want = open ? 'open' : 'closed', was = el.getAttribute('data-eng');
    if (!animate || was === want || !el.isConnected || typeof el.animate !== 'function' || reduced() || (doc() && doc().hidden)) { stopAnim(f); el.setAttribute('data-eng', want); return; }
    const from = el.getBoundingClientRect().height;               // (where a turn in flight has got to)
    stopAnim(f);
    const ease = 'cubic-bezier(.2,.8,.2,1)';
    if (open) {
      el.setAttribute('data-eng', 'open');
      const to = el.getBoundingClientRect().height;
      if (!(to > 0)) return;
      el.style.overflow = 'hidden';
      const a = f.anim = el.animate([{ height: from + 'px', opacity: Math.min(1, from / to) }, { height: to + 'px', opacity: 1 }], { duration: 200, easing: ease });
      a.onfinish = a.oncancel = () => { if (f.anim === a) { f.anim = null; el.style.removeProperty('overflow'); } };
    } else {
      if (!(from > 0)) { el.setAttribute('data-eng', 'closed'); return; }
      el.style.overflow = 'hidden';
      const a = f.anim = el.animate([{ height: from + 'px', opacity: 1 }, { height: '0px', opacity: 0 }], { duration: 180, easing: ease, fill: 'forwards' });
      a.onfinish = () => { if (f.anim !== a) return; f.anim = null; el.setAttribute('data-eng', 'closed'); el.style.removeProperty('overflow'); try { a.cancel(); } catch (_) {} };
      a.oncancel = () => { if (f.anim === a) f.anim = null; };
    }
  }
  function follow(el, sheetKey) {
    if (!el || !el.setAttribute) return { setKey() {}, stop() {} };
    injectCss();
    let f = el._engFollow;
    const key = () => keyOf(typeof sheetKey === 'function' ? sheetKey() : sheetKey);
    if (f && !f.dead) {
      const k = key(); if (f.key === k && el.hasAttribute('data-eng')) return f.api;
      f.key = k; paintShelf(f, isOpen(k), false); return f.api;
    }
    if (followers.size > 64) for (const x of followers) if (!x.el.isConnected) { stopAnim(x); followers.delete(x); }
    f = { el, key: key(), anim: null, dead: false };
    f.api = { setKey(k) { f.key = keyOf(k); paintShelf(f, isOpen(f.key), false); }, stop() { f.dead = true; stopAnim(f); followers.delete(f); if (el._engFollow === f) el._engFollow = null; el.removeAttribute('data-eng'); } };
    el._engFollow = f; followers.add(f);
    paintShelf(f, isOpen(f.key), false);
    return f.api;
  }

  /* ── the control ── */
  function mount(host, o) {
    o = o || {}; injectCss();
    const d = doc(); if (!host || !d) return { update() {}, destroy() {}, button: null };
    const wrap = d.createElement('span'); wrap.className = 'owSeg engTog';
    const btn = d.createElement('button'); btn.type = 'button';
    btn.innerHTML = ICON + '<span class="engN"></span>' + CHEV;
    const sr = d.createElement('span'); sr.className = 'engSr'; sr.setAttribute('role', 'status'); sr.setAttribute('aria-live', 'polite');
    wrap.appendChild(btn); wrap.appendChild(sr); host.appendChild(wrap);
    const n = btn.querySelector('.engN');
    const m = { key: keyOf(typeof o.sheetKey === 'function' ? o.sheetKey() : o.sheetKey), count: 0, shown: null, said: false };
    const on = () => typeof o.getState === 'function' ? !!o.getState(m.key) : isOpen(m.key);
    const pieces = c => `${c} piece${c === 1 ? '' : 's'}`;
    function paint() {
      const open = on(), sig = `${m.key}|${m.count}|${open}`;
      host.hidden = !(m.count > 0);
      if (m.shown === sig) return; m.shown = sig;
      n.textContent = String(m.count);
      btn.classList.toggle('on', open);
      btn.setAttribute('aria-expanded', open ? 'true' : 'false'); btn.setAttribute('aria-pressed', open ? 'true' : 'false');
      btn.setAttribute('aria-label', `Back engraving, ${pieces(m.count)}`);
      btn.title = `${open ? 'Hide' : 'Show'} the back engraving of this sheet · ${pieces(m.count)}`;
      if (m.said) sr.textContent = `Back engraving ${open ? 'shown' : 'hidden'}, ${pieces(m.count)}`;
    }
    btn.addEventListener('click', () => {
      const next = !on(); m.said = true;
      set(m.key, next, 'button');                  // (tells every control and shelf of this sheet, this one among them)
      if (typeof o.onChange === 'function') { try { o.onChange(next, m.key); } catch (_) {} }
      m.shown = null; paint();
    });
    m.count = Math.max(0, +o.count || 0); m.paint = paint; mounts.add(m); paint();
    return {
      button: btn,
      key: () => m.key,
      update(count, sheetKey) {
        if (sheetKey != null && keyOf(sheetKey) !== m.key) m.key = keyOf(sheetKey);
        m.count = Math.max(0, +count || 0); paint();
      },
      destroy() { mounts.delete(m); try { wrap.remove(); } catch (_) {} host.hidden = true; }
    };
  }

  // every control of the sheet that changed repaints, whoever pressed (a press elsewhere, the API, another view of the same sheet)
  if (doc()) doc().addEventListener(EVENT, e => {
    const k = e && e.detail && e.detail.sheetKey, open = !!(e && e.detail && e.detail.open); if (k == null) return;
    for (const m of mounts) if (m.key === k) { m.shown = null; if (m.paint) m.paint(); }
    for (const f of followers) { if (!f.el.isConnected) continue; if (f.key === k) paintShelf(f, open, true); }
  });

  root.EngravingToggle = { mount, follow, isOpen, set, toggle, rekey, countIn, EVENT };
  if (typeof module === 'object' && module.exports) module.exports = root.EngravingToggle;
})(typeof self !== 'undefined' ? self : this);
