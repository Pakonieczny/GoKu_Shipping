/* Charm Nest · the small card over a step circle of the Library's rail.
   Paul, 5 Oct 2026: "Add a slight expanding zoom for each of the timeline milestones similar to the Seals and a small extra info popup over
   each milestone when hovering."
   The zoom is the seals' own (Seal.zoom, charm-nest-motion.js): a rail circle carries data-zoom-dot, the engine waits ZOOM_DELAY under a resting
   pointer (a click, a tap or Tab does it at once), grows it a little in place and puts it back on leaving, Esc, a scroll or a press elsewhere.
   This file only DRAWS what the engine says. It listens for the engine's "dotzoom" event, so the card and the zoom are one hover: they come
   together and go together, and nothing here has a timer of its own for the pointer.
   What the card says is on the circle itself (data-tip: step name, state in plain words, one line, and when a done step was completed and by whom),
   stamped by the Library's rail (charm-nest-bridge.js) from CharmNestReadiness.explain and the sheet's own record: no read, no network call. The
   same words are the circle's aria-label.
   The card: one rounded card with a tiny arrow, above the circle (below it when there is no room above), kept inside the screen and under the top
   bar. It is never clickable and takes no press (pointer-events none), so a '!' under it still opens its panel; it is hidden while an issues
   panel or a window is open (never a pop-up on a pop-up) and a reduced-motion setting turns its movement into a plain fade. The one card is
   reused for every circle; nothing is added per circle and nothing runs while no card is showing. A redraw of the rail patches it in place, so a
   hovered circle stays the same element and the card only follows its words; when a circle is replaced, the card is handed to its successor
   if the pointer or the keyboard is still on it. The arrow keys move the keyboard along a rail (one Tab stop per rail).

   The dots of a piece's row in the order window (Paul, 5 Oct 2026: "Enable each of these solid and hollow green dots into an active hover state like the
   other milestone timeline dots ... a 350 ms delay for the hover state") are the same kind of circle and use this same card, zoom and engine. A dot
   carries [data-pdot="<step key>"] with data-pdot-rid (the order) and data-pdot-piece (the piece's key), data-zoom-dot (its growth), data-zoom-delay="350"
   (the engine's rest for this dot) and data-zoom-group (the row: after one card has been open, the next dot of the row opens after a short beat). The
   card's words are not on the dot: they are asked, when the card opens, of the page's resolver, RailTip.pieceDot(dot) -> { name, state, line, by, seal }
   (charm-nest-bridge.js sets it: no read, no network call; seal: HTML or a node, what window.PieceSeals.render drew, or nothing). Everything else is
   the rail's: a click, a tap, Enter or Space opens it at once and holds it until the pointer leaves, Esc or a scroll; Tab opens it; the arrow keys move
   along the row (one Tab stop per row); the card is never clickable. A press on a dot belongs to the dot: it never reaches the row under it.
   A card over a dot inside a window (a <dialog>) lives in that window's layer, so it shows over it, and is placed by where it really is on screen.
   A REAL seal in the card (a done step's, from PieceSeals) is the one thing in it that takes the pointer, so it zooms in place like every seal (rest 500 ms,
   click at once): the rest of the card stays click-through, so the row's buttons stay pressable. The card keeps itself for a moment after the pointer
   leaves the dot (HOLD_MS) so the pointer can travel to the seal, and then for as long as it is on the seal; a seal put away (second click, Esc, a scroll,
   the pointer leaving) takes the card with it. The unfinished seal of a step still to come is only looked at. */
(function (root) {
  'use strict';
  const doc = root.document;
  if (!doc) return;
  const reduced = () => { try { return root.matchMedia('(prefers-reduced-motion: reduce)').matches; } catch (_) { return false; } };
  const clamp = (n, a, b) => Math.max(a, Math.min(b, n));
  const PDOT = '[data-pdot]';
  const CIRCLE = '.flowRail .flowDot[data-zoom-dot], [data-pdot][data-zoom-dot]';
  const isP = d => !!(d && d.hasAttribute && d.hasAttribute('data-pdot'));
  const API = root.RailTip = root.RailTip || {};   // (the page may have put its resolver on it before this file ran)
  const st = { el: null, tip: null, box: null, flow: '', step: '', kind: '', rid: '', piece: '', sig: '', text: '', anim: null, mo: null, watch: 0, wait: 0, held: 0, heldAt: 0, over: false, sealOn: false };

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
.railTip{display:flex;flex-direction:column}
.rtSeal{display:flex;justify-content:center;margin:7px 0 1px}
.railTip.below .rtSeal{order:-1;margin:1px 0 7px}
.railTip .rtSeal.live .seal{pointer-events:auto}
.rtBy{font-size:10px;color:var(--ink45,#938c80)}
.railTip::after{content:"";position:absolute;left:var(--ax);bottom:-3.5px;width:7px;height:7px;margin-left:-3.5px;background:var(--card,#fffefb);border:1px solid var(--line,#e4ddd0);border-top:0;border-left:0;border-radius:0 0 2px 0;transform:rotate(45deg)}
.railTip.below::after{bottom:auto;top:-3.5px;transform:rotate(225deg)}`;
    (doc.head || doc.documentElement).appendChild(s);
  }
  // the card, made once; over a dot inside a window it is moved into that window's layer (a <dialog> opened as a modal covers the page's own)
  function make(d) {
    let t = st.tip;
    if (!t || !t.isConnected) {
      css();
      t = st.tip = doc.createElement('div'); t.className = 'railTip'; t.setAttribute('aria-hidden', 'true');   // (the circle's own label says the same: read once)
    }
    const host = (d && d.closest && d.closest('dialog[open]')) || doc.body || doc.documentElement;
    if (t.parentNode !== host) host.appendChild(t);
    return t;
  }
  const parts = d => String(d.getAttribute('data-tip') || '').split('\n');
  /** What the card says of a circle: { name, state, line, by, seal } (null: nothing to say). A rail circle carries its words (data-tip); a dot of a piece's
   *  row is asked of the page (RailTip.pieceDot). */
  function infoOf(d) {
    if (isP(d)) {
      let i = null; try { i = typeof API.pieceDot === 'function' ? API.pieceDot(d) : null; } catch (e) { try { console.warn('Piece dot card', e); } catch (_) {} }
      return i && i.name ? { name: String(i.name), state: String(i.state || ''), line: String(i.line || ''), by: String(i.by || ''), seal: i.seal || null } : null;
    }
    const [name = '', state = '', line = '', by = ''] = parts(d);
    return name ? { name, state, line, by, seal: null } : null;
  }
  const keyOf = i => JSON.stringify([i.name, i.state, i.line, i.by, i.seal ? String(i.seal.outerHTML || i.seal) : '']);
  const sigOf = d => isP(d) ? (d.getAttribute('aria-label') || '') + '|' + (d.getAttribute('data-pdot-sig') || '') : d.getAttribute('data-tip') || '';
  function fill(t, d, info) {
    info = info || infoOf(d) || { name: '', state: '', line: '', by: '', seal: null };
    const node = (tag, cls, text) => { const n = doc.createElement(tag); if (cls) n.className = cls; n.textContent = text; return n; };
    const head = doc.createElement('div'); head.className = 'rtHead'; head.append(node('b', '', info.name), node('span', 'rtState', info.state));
    t.dataset.state = info.state; st.text = keyOf(info); st.sig = sigOf(d);
    let seal = null;
    if (info.seal) {   // (the seal slot: what PieceSeals drew for this step, the real seal of a done step or its unfinished version; in the card it is only looked at)
      seal = doc.createElement('div'); seal.className = 'rtSeal';
      if (typeof info.seal === 'string') seal.innerHTML = info.seal; else if (info.seal.nodeType === 1) seal.append(info.seal);
      if (!seal.firstChild) seal = null;
      else if (seal.querySelector('.seal')) {   // (a real seal: it takes the pointer; the card is aria-hidden and goes with focus, so it is no Tab stop here)
        seal.classList.add('live');
        for (const n of seal.querySelectorAll('[tabindex]')) n.setAttribute('tabindex', '-1');
      }
    }
    t.replaceChildren(...[head, info.line && node('p', 'rtLine', info.line), info.by && node('p', 'rtBy', info.by), seal].filter(Boolean));
  }
  /* above the circle (where the engine will have grown it to), below it when there is no room above; inside the screen sideways, under the top bar */
  function place(d) {
    const t = st.tip, zr = root.Seal && root.Seal.zoom && typeof root.Seal.zoom.rectOf === 'function' ? root.Seal.zoom.rectOf(d) : null, b = d.getBoundingClientRect();
    const r = zr ? { left: zr.left, right: zr.right, top: zr.top, bottom: zr.bottom } : { left: b.left, right: b.right, top: b.top, bottom: b.bottom };
    const vw = root.innerWidth || 1200, vh = root.innerHeight || 800, bar = doc.querySelector('.topbar'), q = bar && bar.getClientRects().length ? bar.getBoundingClientRect() : null;
    const lo = Math.max(8, q && q.bottom < vh / 2 ? q.bottom + 8 : 0), hi = vh - 8, GAP = 9;
    t.style.left = '0px'; t.style.top = '0px';
    // (inside a window that is moving or has a transform, a fixed card is placed from the window's corner, not the screen's: where it really is is read)
    const o = t.getBoundingClientRect(), ox = o.left || 0, oy = o.top || 0;
    const w = t.offsetWidth, h = t.offsetHeight, cx = (r.left + r.right) / 2;
    const above = r.top - GAP - lo, below = hi - (r.bottom + GAP), up = above >= h || (below < h && above >= below);
    const y = clamp(up ? r.top - GAP - h : r.bottom + GAP, lo, Math.max(lo, hi - h)), x = clamp(cx - w / 2, 8, Math.max(8, vw - w - 8));
    t.style.left = Math.round(x - ox) + 'px'; t.style.top = Math.round(y - oy) + 'px';
    t.style.setProperty('--ax', clamp(cx - x, 14, Math.max(14, w - 14)) + 'px'); t.classList.toggle('below', !up);
    return up;
  }
  // an issues panel or a window that does not hold the circle is open: no card (never a pop-up on a pop-up); a dot inside a window answers to the windows only
  function suppressed(d) {
    const own = d.closest && d.closest('dialog');
    if (!(isP(d) && own) && (doc.getElementById('libIssuesPanel') || doc.querySelector('.lisPanel'))) return true;
    for (const w of doc.querySelectorAll('dialog[open]')) if (!w.contains(d)) return true;
    return false;
  }
  function show(d) {
    const info = d && d.isConnected ? infoOf(d) : null;
    if (!info || suppressed(d)) return;
    const t = make(d), was = st.el === d && t.hasAttribute('data-on');
    unhold();
    st.el = d; st.kind = isP(d) ? 'pdot' : 'rail';
    st.box = st.kind === 'pdot' ? d.closest('.owPcSum') || d.parentElement : d.closest('.flowBox');
    st.flow = st.kind === 'rail' && st.box ? st.box.dataset.flowFor || '' : ''; st.step = st.kind === 'rail' ? d.getAttribute('data-step') || '' : d.getAttribute('data-pdot') || '';
    st.rid = d.getAttribute('data-pdot-rid') || ''; st.piece = d.getAttribute('data-pdot-piece') || '';
    if (st.anim) { st.anim.cancel(); st.anim = null; }   // (placed from where it rests: a running slide would be measured as an offset)
    fill(t, d, info);
    const up = place(d);
    t.setAttribute('data-on', ''); t.style.opacity = '1';
    if (!was && t.animate) {
      const off = reduced() ? 0 : up ? 4 : -4;
      st.anim = t.animate([{ opacity: 0, transform: `translateY(${off}px)` }, { opacity: 1, transform: 'none' }], { duration: reduced() ? 90 : 150, easing: 'ease-out' });
    }
    watch();
  }
  function hide(now) {
    unwatch(); unhold();
    const t = st.tip; st.el = null; st.box = null;
    if (!t || !t.hasAttribute('data-on')) return;
    if (st.anim) { st.anim.cancel(); st.anim = null; }
    const off = () => { t.removeAttribute('data-on'); t.style.opacity = '0'; };
    if (now || !t.animate) { off(); return; }
    const a = st.anim = t.animate([{ opacity: 1 }, { opacity: 0 }], { duration: reduced() ? 70 : 110, easing: 'ease-in', fill: 'forwards' });
    a.finished.then(() => { if (st.anim === a) { st.anim = null; off(); a.cancel(); } }, () => {});
  }
  const away = d => { try { if (root.Seal && root.Seal.zoom) root.Seal.zoom.hide(false, d); } catch (_) {} };
  /* The card kept for its real seal: the pointer left the dot (the engine says why: "leave") while the card holds a live seal. For HOLD_MS the pointer may
     travel there; once it is on the seal the card stays while it is; a seal that was grown and is put back ends it, and so does leaving the seal. */
  const HOLD_MS = 280;
  const zoomedIn = () => { const z = root.Seal && root.Seal.zoom, c = z && z.current; return !!(c && st.tip && st.tip.contains(c)); };
  function unhold() { if (st.held) { clearInterval(st.held); st.held = 0; } st.over = false; st.sealOn = false; }
  function holdFor() {
    if (!st.tip || !st.tip.querySelector('.rtSeal.live')) return false;
    unwatch(); unhold(); st.heldAt = Date.now();
    st.held = setInterval(() => {
      if (!st.tip || !st.tip.hasAttribute('data-on')) { unhold(); return; }
      const on = zoomedIn(); if (on) st.sealOn = true;
      if (st.sealOn && !on) { hide(false); return; }   // (the seal was grown and has been put back: done with it)
      if (!st.over && !on && Date.now() - st.heldAt > HOLD_MS) hide(false);
    }, 90);
    return true;
  }
  doc.addEventListener('pointerover', ev => { if (st.held && ev.target.closest && ev.target.closest('.railTip .rtSeal.live .seal')) st.over = true; }, true);
  doc.addEventListener('pointerout', ev => {
    if (!st.held || !st.over) return;
    const to = ev.relatedTarget; if (to && to.closest && to.closest('.railTip .rtSeal.live .seal')) return;
    st.over = false; st.heldAt = Date.now() - HOLD_MS + 120;   // (a short moment to come back)
  }, true);
  root.addEventListener('scroll', () => { if (st.held) hide(true); }, true);
  root.addEventListener('keydown', ev => { if (st.held && ev.key === 'Escape') hide(false); }, true);
  /* while a card shows: it follows its circle's words, goes with an issues panel or a window that opens, and is handed to the circle that replaced it */
  function watch() {
    unwatch();
    const d = st.el; if (!d) return;
    if (root.MutationObserver) {
      st.mo = new root.MutationObserver(() => {
        const e = st.el; if (!e) return;
        if (!e.isConnected) { relink(); return; }
        if (isP(e)) { if (sigOf(e) !== st.sig) { const i = infoOf(e); if (i) { fill(st.tip, e, i); place(e); } } }
        else if ((e.getAttribute('data-tip') || '') !== st.text) { fill(st.tip, e); place(e); }
      });
      st.mo.observe(d, { attributes: true, attributeFilter: ['data-tip', 'aria-label', 'data-pdot-sig'] });
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
    const q = x => String(x).replace(/["\\]/g, '');
    if (st.kind === 'pdot') {   // (a row drawn again: the same dot of the same piece of the same order)
      const n = doc.querySelector(`${PDOT}[data-pdot="${q(st.step)}"][data-pdot-rid="${q(st.rid)}"][data-pdot-piece="${q(st.piece)}"]`);
      return n && n.isConnected ? n : null;
    }
    const sel = `.flowBox[data-flow-for="${q(st.flow)}"] .flowDot[data-step="${q(st.step)}"]`;
    const box = st.box && st.box.isConnected ? st.box : null, n = (box ? box.querySelector(`.flowDot[data-step="${q(st.step)}"]`) : null) || (st.flow ? doc.querySelector(sel) : null);
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
    else if (st.el === d) { if (ev.detail.why === 'leave' && !st.held && holdFor()) return; hide(false); }
  });
  // the arrow keys move the keyboard along a rail (or a piece's row of dots): one Tab stop for the whole rail, every circle reachable
  doc.addEventListener('keydown', ev => {
    const k = ev.key; if (k !== 'ArrowLeft' && k !== 'ArrowRight' && k !== 'Home' && k !== 'End') return;
    if (ev.altKey || ev.ctrlKey || ev.metaKey || ev.shiftKey) return;
    const d = ev.target && ev.target.closest ? ev.target.closest(CIRCLE) : null; if (!d) return;
    const grp = isP(d) ? d.closest('[data-pdot-group], .steps') : d.closest('.flowRail'); if (!grp) return;
    const all = [...grp.querySelectorAll(isP(d) ? PDOT + '[data-zoom-dot]' : '.flowDot[data-zoom-dot]')], i = all.indexOf(d);
    const to = all[k === 'Home' ? 0 : k === 'End' ? all.length - 1 : clamp(i + (k === 'ArrowRight' ? 1 : -1), 0, all.length - 1)];
    if (to && to !== d) { ev.preventDefault(); to.focus({ preventScroll: true }); }
  });
  // a row of dots has one Tab stop, wherever the keyboard last was in it
  doc.addEventListener('focusin', ev => {
    const d = ev.target && ev.target.closest ? ev.target.closest(PDOT) : null; if (!d || d !== ev.target) return;
    const grp = d.closest('[data-pdot-group], .steps'); if (!grp) return;
    for (const n of grp.querySelectorAll(PDOT)) n.tabIndex = n === d ? 0 : -1;
  });
  // a press on a dot is the dot's: it never reaches the row under it (which would open the piece, or press its button)
  doc.addEventListener('click', ev => { if (ev.target && ev.target.closest && ev.target.closest(PDOT)) ev.stopPropagation(); }, true);
  // Enter or Space on a dot inside a button (the engine leaves those to the button): opens the card, or puts it away
  doc.addEventListener('keydown', ev => {
    if (ev.key !== 'Enter' && ev.key !== ' ') return;
    const d = ev.target && ev.target.closest ? ev.target.closest(PDOT) : null; if (!d || d !== ev.target || !d.closest('button, a')) return;
    const z = root.Seal && root.Seal.zoom; if (!z) return;
    ev.preventDefault(); ev.stopPropagation();
    if (z.current === d) z.hide(false, d); else z.show(d, { keyboard: true });
  }, true);
  Object.assign(API, { show, hide, shown: () => !!(st.tip && st.tip.hasAttribute('data-on')), current: () => st.el });
})(typeof self !== 'undefined' ? self : this);
