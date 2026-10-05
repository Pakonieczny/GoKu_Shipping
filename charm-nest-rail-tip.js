/* Charm Nest · the small card over a step circle of the Library's rail.
   Paul, 5 Oct 2026: "Add a slight expanding zoom for each of the timeline milestones similar to the Seals and a small extra info popup over
   each milestone when hovering."
   The zoom is the seals' own (Seal.zoom, charm-nest-motion.js): a rail circle carries data-zoom-dot, the engine waits ZOOM_DELAY under a resting
   pointer (a click, a tap or Tab does it at once), grows it a little in place and puts it back on leaving, Esc, a scroll or a press elsewhere.
   This file only DRAWS what the engine says. It listens for the engine's "dotzoom" event, so the card and the zoom are one hover: they come
   together and go together, and nothing here has a timer of its own for the pointer.
   What the card says is on the circle itself (data-tip: step name, state in plain words, one line, and who and when for a step the laser finished),
   stamped by the Library's rail (charm-nest-bridge.js) from CharmNestReadiness.explain and the sheet's own record: no read, no network call. The
   same words are the circle's aria-label.
   The card: one rounded card with a tiny arrow, above the circle (below it when there is no room above), kept inside the screen and under the top
   bar. It is never clickable and takes no press (pointer-events none), so a '!' under it still opens its panel; it is hidden while an issues
   panel or a window is open (never a pop-up on a pop-up) and a reduced-motion setting turns its movement into a plain fade. The one card is
   reused for every circle; nothing is added per circle and nothing runs while no card is showing. A redraw of the rail patches it in place, so a
   hovered circle stays the same element and the card only follows its words; when a circle is replaced, the card is handed to its successor
   if the pointer or the keyboard is still on it. The arrow keys move the keyboard along a rail (one Tab stop per rail). */
(function (root) {
  'use strict';
  const doc = root.document;
  if (!doc) return;
  const reduced = () => { try { return root.matchMedia('(prefers-reduced-motion: reduce)').matches; } catch (_) { return false; } };
  const clamp = (n, a, b) => Math.max(a, Math.min(b, n));
  const CIRCLE = '.flowRail .flowDot[data-zoom-dot]';
  const st = { el: null, tip: null, box: null, flow: '', step: '', text: '', anim: null, mo: null, watch: 0, wait: 0 };

  function css() {
    if (doc.getElementById('railTipCss')) return;
    const s = doc.createElement('style'); s.id = 'railTipCss';
    s.textContent = `.railTip{position:fixed;z-index:2147482000;left:0;top:0;box-sizing:border-box;width:max-content;max-width:min(236px,calc(100vw - 16px));padding:8px 11px 9px;border-radius:11px;background:var(--card,#fffefb);color:var(--ink70,#5b554c);border:1px solid var(--line,#e4ddd0);box-shadow:0 10px 26px rgba(30,26,20,.13),0 1px 3px rgba(30,26,20,.07);font:11px/1.4 var(--sans,system-ui,sans-serif);text-align:left;pointer-events:none;visibility:hidden;opacity:0;--ax:50%}
.railTip[data-on]{visibility:visible}
.railTip *{pointer-events:none}
.rtHead{display:flex;align-items:baseline;gap:6px;min-width:0}
.rtHead b{font:600 12px/1.3 var(--sans,system-ui,sans-serif);color:var(--ink,#1c1a17)}
.rtState{font-size:10px;color:var(--ink45,#938c80);white-space:nowrap}
.rtState::before{content:"·";margin-right:6px}
.railTip[data-state="Done"] .rtState{color:var(--sage,#5f7a5b)}
.railTip[data-state="Blocked"] .rtState{color:var(--clay,#b0563f)}
.rtLine,.rtBy{margin:3px 0 0;overflow-wrap:anywhere}
.rtBy{font-size:10px;color:var(--ink45,#938c80)}
.railTip::after{content:"";position:absolute;left:var(--ax);bottom:-3.5px;width:7px;height:7px;margin-left:-3.5px;background:var(--card,#fffefb);border:1px solid var(--line,#e4ddd0);border-top:0;border-left:0;border-radius:0 0 2px 0;transform:rotate(45deg)}
.railTip.below::after{bottom:auto;top:-3.5px;transform:rotate(225deg)}`;
    (doc.head || doc.documentElement).appendChild(s);
  }
  function make() {
    if (st.tip && st.tip.isConnected) return st.tip;
    css();
    const t = st.tip = doc.createElement('div'); t.className = 'railTip'; t.setAttribute('aria-hidden', 'true');   // (the circle's own label says the same: read once)
    (doc.body || doc.documentElement).appendChild(t);
    return t;
  }
  const parts = d => String(d.getAttribute('data-tip') || '').split('\n');
  function fill(t, d) {
    const [name = '', state = '', line = '', by = ''] = parts(d), node = (tag, cls, text) => { const n = doc.createElement(tag); if (cls) n.className = cls; n.textContent = text; return n; };
    const head = doc.createElement('div'); head.className = 'rtHead'; head.append(node('b', '', name), node('span', 'rtState', state));
    t.dataset.state = state; st.text = d.getAttribute('data-tip') || '';
    t.replaceChildren(...[head, line && node('p', 'rtLine', line), by && node('p', 'rtBy', by)].filter(Boolean));
  }
  /* above the circle (where the engine will have grown it to), below it when there is no room above; inside the screen sideways, under the top bar */
  function place(d) {
    const t = st.tip, zr = root.Seal && root.Seal.zoom && typeof root.Seal.zoom.rectOf === 'function' ? root.Seal.zoom.rectOf(d) : null, b = d.getBoundingClientRect();
    const r = zr ? { left: zr.left, right: zr.right, top: zr.top, bottom: zr.bottom } : { left: b.left, right: b.right, top: b.top, bottom: b.bottom };
    const vw = root.innerWidth || 1200, vh = root.innerHeight || 800, bar = doc.querySelector('.topbar'), q = bar && bar.getClientRects().length ? bar.getBoundingClientRect() : null;
    const lo = Math.max(8, q && q.bottom < vh / 2 ? q.bottom + 8 : 0), hi = vh - 8, GAP = 9;
    t.style.left = '0px'; t.style.top = '0px';
    const w = t.offsetWidth, h = t.offsetHeight, cx = (r.left + r.right) / 2;
    const above = r.top - GAP - lo, below = hi - (r.bottom + GAP), up = above >= h || (below < h && above >= below);
    const y = clamp(up ? r.top - GAP - h : r.bottom + GAP, lo, Math.max(lo, hi - h)), x = clamp(cx - w / 2, 8, Math.max(8, vw - w - 8));
    t.style.left = Math.round(x) + 'px'; t.style.top = Math.round(y) + 'px';
    t.style.setProperty('--ax', clamp(cx - x, 14, Math.max(14, w - 14)) + 'px'); t.classList.toggle('below', !up);
    return up;
  }
  // an issues panel or a window that does not hold the circle is open: no card (never a pop-up on a pop-up)
  function suppressed(d) {
    if (doc.getElementById('libIssuesPanel') || doc.querySelector('.lisPanel')) return true;
    for (const w of doc.querySelectorAll('dialog[open]')) if (!w.contains(d)) return true;
    return false;
  }
  function show(d) {
    if (!d || !d.isConnected || !parts(d)[0] || suppressed(d)) return;
    const t = make(), was = st.el === d && t.hasAttribute('data-on');
    st.el = d; st.box = d.closest('.flowBox'); st.flow = st.box ? st.box.dataset.flowFor || '' : ''; st.step = d.getAttribute('data-step') || '';
    fill(t, d);
    const up = place(d);
    if (st.anim) { st.anim.cancel(); st.anim = null; }
    t.setAttribute('data-on', ''); t.style.opacity = '1';
    if (!was && t.animate) {
      const off = reduced() ? 0 : up ? 4 : -4;
      st.anim = t.animate([{ opacity: 0, transform: `translateY(${off}px)` }, { opacity: 1, transform: 'none' }], { duration: reduced() ? 90 : 150, easing: 'ease-out' });
    }
    watch();
  }
  function hide(now) {
    unwatch();
    const t = st.tip; st.el = null; st.box = null;
    if (!t || !t.hasAttribute('data-on')) return;
    if (st.anim) { st.anim.cancel(); st.anim = null; }
    const off = () => { t.removeAttribute('data-on'); t.style.opacity = '0'; };
    if (now || !t.animate) { off(); return; }
    const a = st.anim = t.animate([{ opacity: 1 }, { opacity: 0 }], { duration: reduced() ? 70 : 110, easing: 'ease-in', fill: 'forwards' });
    a.finished.then(() => { if (st.anim === a) { st.anim = null; off(); a.cancel(); } }, () => {});
  }
  const away = d => { try { if (root.Seal && root.Seal.zoom) root.Seal.zoom.hide(false, d); } catch (_) {} };
  /* while a card shows: it follows its circle's words, goes with an issues panel or a window that opens, and is handed to the circle that replaced it */
  function watch() {
    unwatch();
    const d = st.el; if (!d) return;
    if (root.MutationObserver) {
      st.mo = new root.MutationObserver(() => {
        const e = st.el; if (!e) return;
        if (!e.isConnected) { relink(); return; }
        if ((e.getAttribute('data-tip') || '') !== st.text) { fill(st.tip, e); place(e); }
      });
      st.mo.observe(d, { attributes: true, attributeFilter: ['data-tip'] });
      if (st.box && st.box.isConnected) st.mo.observe(st.box, { childList: true, subtree: true });
    }
    st.watch = setInterval(() => {
      const e = st.el; if (!e) { unwatch(); return; }
      if (!e.isConnected) { relink(); return; }
      if (suppressed(e)) { const x = e; hide(false); away(x); }
    }, 250);
  }
  function unwatch() {
    if (st.mo) { try { st.mo.disconnect(); } catch (_) {} st.mo = null; }
    if (st.watch) { clearInterval(st.watch); st.watch = 0; }
  }
  function successor() {
    const sel = `.flowBox[data-flow-for="${String(st.flow).replace(/["\\]/g, '')}"] .flowDot[data-step="${String(st.step).replace(/["\\]/g, '')}"]`;
    const box = st.box && st.box.isConnected ? st.box : null, n = (box ? box.querySelector(`.flowDot[data-step="${String(st.step).replace(/["\\]/g, '')}"]`) : null) || (st.flow ? doc.querySelector(sel) : null);
    return n && n.isConnected ? n : null;
  }
  function relink() {
    const gone = st.el; if (!gone) return;
    const n = successor();
    hide(true); away(gone);
    if (!n) return;
    clearTimeout(st.wait);
    st.wait = setTimeout(() => {   // (the pointer's place is worked out after the redraw has been laid out)
      st.wait = 0;
      if (!n.isConnected || !root.Seal || !root.Seal.zoom) return;
      if (doc.activeElement === n) root.Seal.zoom.show(n, { keyboard: true });
      else if (n.matches(':hover')) root.Seal.zoom.show(n);
    }, 120);
  }
  doc.addEventListener('dotzoom', ev => {
    const d = ev.detail && ev.detail.el; if (!d) return;
    if (ev.detail.ask) { if (d.matches && d.matches(CIRCLE) && suppressed(d)) ev.preventDefault(); return; }   // (an issues panel or a window is open: the circles stay still too, and Esc is the panel's)
    if (ev.detail.on) { if (d.matches && d.matches(CIRCLE)) show(d); }
    else if (st.el === d) hide(false);
  });
  // the arrow keys move the keyboard along a rail: one Tab stop for the whole rail, every circle reachable
  doc.addEventListener('keydown', ev => {
    const k = ev.key; if (k !== 'ArrowLeft' && k !== 'ArrowRight' && k !== 'Home' && k !== 'End') return;
    if (ev.altKey || ev.ctrlKey || ev.metaKey || ev.shiftKey) return;
    const d = ev.target && ev.target.closest ? ev.target.closest(CIRCLE) : null; if (!d) return;
    const all = [...d.closest('.flowRail').querySelectorAll('.flowDot[data-zoom-dot]')], i = all.indexOf(d);
    const to = all[k === 'Home' ? 0 : k === 'End' ? all.length - 1 : clamp(i + (k === 'ArrowRight' ? 1 : -1), 0, all.length - 1)];
    if (to && to !== d) { ev.preventDefault(); to.focus({ preventScroll: true }); }
  });
  root.RailTip = { show, hide, shown: () => !!(st.tip && st.tip.hasAttribute('data-on')), current: () => st.el };
})(typeof self !== 'undefined' ? self : this);
