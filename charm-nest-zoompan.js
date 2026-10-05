/* Zoom and pan where a picture lies (Paul, 5 Oct 2026, 13:25 UTC: "You implemented the zoom features here as a fullscreen Zoom
 * view. This is not what I wanted. You need to implement the same type of sophisticated zoom and pan as we already have in other
 * parts of this app. When clicking to zoom it should zoom in within its own space and should be draggable to pan the photo and
 * when clicking to zoom it should centre the area of the image that was clicked.").
 *
 * This is ONE module for every order window picture: the Etsy listing photo, the Vector design, the back engraving preview, the
 * charm's drawing on the Sheet tab and in the Sheet window. It is the click-to-zoom / drag-to-pan the app already had (the
 * Listing Generator's attachPreviewBoxListeners, as Review's and the lists' thumbnails (ListZoom, charm-nest-bridge.js) and the
 * Shopify studio's attachZoomPan carry it), taken over with its numbers and its rules unchanged:
 *   · the picture is moved by `transform: scale(s) translate(x px, y px)` about its centre, and nothing else (no layout, no dialog);
 *   · a click zooms IN, centred on the point that was clicked: the new scale is the largest of 1.33, 1.5 × the one now and the
 *     scale at which that point can be brought to the middle without leaving the frame empty (never above 8); a click that
 *     would hardly change an already zoomed picture (above 1.2) puts it back whole, as a click at the 8 × limit does;
 *   · a drag pans a zoomed picture, damped by half and stopped at the picture's edges (the frame is never left empty); a
 *     click that ends a drag is not a click;
 *   · the wheel zooms by 1.1 a step and snaps back to the whole picture at 1 and below.
 * What it adds, because these pictures sit in a narrow window that must keep scrolling and are used on phones:
 *   · the plain wheel is NOT taken (the window scrolls over a picture as it always did); ctrl / cmd + wheel and a trackpad pinch
 *     zoom, about the pointer (the Shopify studio's `wheelZoom: false` rule, with the modifier kept for zooming);
 *   · touch: a tap is a click, one finger drags a zoomed picture, two fingers pinch about their midpoint;
 *   · keyboard on the focused frame: Enter / Space zoom in at its centre and back out, + and − step, 0 resets, the arrows pan a
 *     zoomed picture, Esc puts it back whole (and is taken, so the window around it stays open for that press);
 *   · the picture is measured by what is drawn (object-fit: contain), so a wide photo in a square frame is never panned off into
 *     the empty bands, and a click lands where the picture is;
 *   · the zoom state is kept per slot and key (`id`, `key`): a window drawn again for the same piece keeps its zoom and pan, a
 *     different piece (or picture) starts whole;
 *   · a picture that should stay sharp is drawn again, larger, when the zoom settles (`hires`), and the small one is put back
 *     when it is whole again.
 * Only `transform` animates (a short ease on a click, key or reset; none while dragging, pinching or wheeling).
 *
 *   CNZoomPan.attach(box, opts) -> controller      (again on the same box: updates it, never a second set of listeners)
 *       box          the frame: it clips (overflow hidden) and is the focusable element
 *       opts.id      the slot's name (its state is kept under it);  opts.key  what the slot shows now (a change starts whole)
 *       opts.media   selector of the picture inside the frame (default "img, canvas, svg"); it is looked for each time, so a
 *                    frame whose picture is drawn again (or arrives later) keeps working
 *       opts.label   the frame's accessible name (when it has none);  opts.persistent  the frame is never taken out of the page
 *       opts.wheel   "modifier" (default: ctrl / cmd + wheel and pinch), "always" (the old rule: the wheel zooms, but only when
 *                    there is zoom left to give, so the page's scroll is never trapped at the ends) or "off"
 *       opts.hires   async ({scale, px, media, box, key}) -> {el} | {src} | {srcs:[…], px?} | null: a sharper picture for the
 *                    zoom now (px: the pixels needed across the frame, capped at opts.maxPx = 2048)
 *       opts.onChange(state)   after every change
 *     controller: { box, key(k), reset(animate), state(), zoomAt(clientX, clientY), toggle(), refresh(), destroy() }
 *   CNZoomPan.resetWithin(root) -> true when something under `root` was zoomed (it is put back whole): for an Esc
 *   CNZoomPan.zoomedWithin(root) -> boolean;  CNZoomPan.forget(prefix) puts the slots whose id starts so back whole and drops
 *   their state (a window closing);  CNZoomPan.stats() -> { live, kept, ids };  CNZoomPan.math (pure: limits, clamp, clickTarget, zoomAbout)
 */
(function (root, factory) { const api = factory(root); if (typeof module === 'object' && module.exports) module.exports = api; else if (root) root.CNZoomPan = api; })(typeof window !== 'undefined' ? window : null, function (root) {
  'use strict';
  const MAX_SCALE = 8, DRAG_THRESHOLD = 3, DESIRED = 1.33, DAMPING = 0.5, WHEEL_STEP = 1.1, KEY_STEP = 1.5, ARROW_STEP = 5, SETTLE_MS = 260, EASE = 'transform 240ms cubic-bezier(.2,.7,.2,1)';

  /* ── the maths, pure: v = { vw, vh, bw, bh } is the frame's inner size and the picture's drawn size ── */
  const lim = (v, m) => Math.max(-m, Math.min(m, v));
  const limits = (s, v) => ({ x: Math.max(0, (v.bw * s - v.vw) / 2 / s), y: Math.max(0, (v.bh * s - v.vh) / 2 / s) });
  /** The state kept inside what the frame can show (a size not known yet clamps nothing: it is clamped when it is). */
  function clamp(st, v) {
    if (!v || !(v.vw && v.vh && v.bw && v.bh)) return { s: st.s, x: st.x, y: st.y };
    const m = limits(st.s, v); return { s: st.s, x: lim(st.x, m.x), y: lim(st.y, m.y) };
  }
  /** The click rule: where the state goes for a click at (dx, dy) screen pixels from the frame's centre. */
  function clickTarget(st, v, dx, dy) {
    const tx = -(dx / st.s - st.x), ty = -(dy / st.s - st.y);
    // the scale at which this point can be brought to the middle without leaving the frame empty (the picture's own size counts, so
    // a picture that does not fill its frame is zoomed until it does); a click outside the picture (in the bands) asks for nothing
    const need = (t, b, f) => b - 2 * Math.abs(t) <= 0 ? 1 : f / (b - 2 * Math.abs(t));
    let s1 = Math.max(DESIRED, st.s * 1.5, need(tx, v.bw, v.vw), need(ty, v.bh, v.vh));
    if (!Number.isFinite(s1) || s1 > MAX_SCALE) s1 = MAX_SCALE;
    if (st.s > 1.2 && Math.abs(s1 - st.s) < 0.5) return { s: 1, x: 0, y: 0 };
    return clamp({ s: s1, x: tx, y: ty }, v);
  }
  /** A new scale about a point (ax, ay screen pixels from the centre): that point of the picture stays under the pointer. */
  function zoomAbout(st, v, s1, ax, ay) {
    s1 = Math.min(MAX_SCALE, s1);
    if (s1 <= 1) return { s: 1, x: 0, y: 0 };
    const px = ax / st.s - st.x, py = ay / st.s - st.y;
    return clamp({ s: s1, x: ax / s1 - px, y: ay / s1 - py }, v);
  }
  const math = { MAX_SCALE, limits, clamp, clickTarget, zoomAbout };
  if (!root) return { math };

  /* ── slots ── */
  const num = (v, d) => { const n = parseFloat(v); return Number.isFinite(n) ? n : d; };
  const kept = new Map();                 // id -> { key, s, x, y }: the state of a slot, for a frame drawn again
  const live = new Set();                 // the controllers
  const byBox = new WeakMap();
  const still = () => { try { return root.Motion && root.Motion.reduced ? root.Motion.reduced() : root.matchMedia('(prefers-reduced-motion: reduce)').matches; } catch (_) { return false; } };
  // one observer for every frame: a frame resized (a window answering its width) is clamped again
  let ro = null; try { if (root.ResizeObserver) ro = new root.ResizeObserver(es => { for (const e of es) { const c = byBox.get(e.target); if (c) c._resized(); } }); } catch (_) {}
  const keyStr = k => (k == null ? '' : String(k));
  // (while a seal is being pressed the page is still: the shared Seal layer holds every click and key; a drag or a wheel waits too)
  const pressing = () => { try { return !!(root.Seal && root.Seal.busy && root.Seal.busy()); } catch (_) { return false; } };

  function make(box, opts0) {
    const doc = box.ownerDocument, abort = new AbortController();
    let opts = Object.assign({ media: 'img, canvas, svg', wheel: 'modifier' }, opts0 || {});
    const st = { s: 1, x: 0, y: 0 };
    let curKey = keyStr(opts.key), lastMedia = null, owned = false, destroyed = false, btn = null;
    let grabbing = false, dragged = false, down = null, pinch = null, quietUntil = 0, settleT = 0, hi = null, hiJob = 0;
    const pts = new Map();
    const on = (type, fn, o) => box.addEventListener(type, fn, Object.assign({ signal: abort.signal }, o));
    const findMedia = () => { for (const m of box.querySelectorAll(opts.media)) if (!m.closest('.zpReset')) return m; return null; };

    /* what is drawn: the frame's inner size, and the picture's size inside its element (object-fit: contain keeps its own shape) */
    function view(m) {
      m = m || findMedia(); if (!m) return null;
      const vw = box.clientWidth, vh = box.clientHeight; let ew = m.clientWidth, eh = m.clientHeight;
      if (!ew || !eh) { const r = m.getBoundingClientRect(), s = num(m.dataset.scale, 1) || 1; ew = r.width / s; eh = r.height / s; }
      let bw = ew, bh = eh;
      const nw = m.naturalWidth || (m.tagName === 'CANVAS' ? m.width : 0), nh = m.naturalHeight || (m.tagName === 'CANVAS' ? m.height : 0);
      let fit = ''; try { fit = root.getComputedStyle(m).objectFit; } catch (_) {}
      if (nw && nh && ew && eh && /contain|scale-down/.test(fit)) { const k = Math.min(ew / nw, eh / nh); bw = nw * k; bh = nh * k; }
      return { vw, vh, bw, bh };
    }
    function paint(animate) {
      const m = findMedia(); if (!m) return;
      m.dataset.scale = st.s; m.dataset.offsetX = st.x; m.dataset.offsetY = st.y;
      m.style.transition = animate && !still() ? EASE : 'none';
      m.style.transform = st.s === 1 && !st.x && !st.y ? '' : `scale(${st.s}) translate(${st.x}px, ${st.y}px)`;
    }
    /** The frame says what it is (a button that zooms), and what the pointer will do. */
    function chrome() {
      const m = findMedia(), zoomed = st.s > 1.0001;
      box.dataset.zp = !m ? 'none' : zoomed ? 'zoomed' : 'rest';
      if (m) {
        box.style.cursor = grabbing ? 'grabbing' : zoomed ? 'grab' : 'zoom-in';
        box.style.touchAction = zoomed ? 'none' : 'pan-y';   // (whole: a finger still scrolls the window; zoomed: it pans the picture)
        if (!owned) {
          box.style.userSelect = 'none'; box.style.webkitUserSelect = 'none'; box.style.webkitTapHighlightColor = 'transparent';
          if (root.getComputedStyle(box).position === 'static') box.style.position = 'relative';
          owned = true; box.tabIndex = 0; box.setAttribute('role', 'button');
          if (opts.label && !box.hasAttribute('aria-label')) box.setAttribute('aria-label', opts.label);
        }
        box.setAttribute('aria-pressed', String(zoomed));
      } else {
        box.style.cursor = ''; box.style.touchAction = '';
        // (a frame that says "Unavailable · Retry" has its own button ways: they are left alone)
        if (owned) { owned = false; box.removeAttribute('aria-pressed'); if (typeof box.onclick !== 'function') { box.removeAttribute('role'); box.removeAttribute('tabindex'); } }
      }
      if (btn) btn.hidden = !(m && zoomed);
    }
    function remember() {
      if (!opts.id) return;
      if (st.s > 1.0001) kept.set(opts.id, { key: curKey, s: st.s, x: st.x, y: st.y }); else kept.delete(opts.id);
    }
    function emit() { if (typeof opts.onChange === 'function') { try { opts.onChange({ s: st.s, x: st.x, y: st.y, key: curKey }); } catch (_) {} } }
    function set(to, animate) {
      const v = view(); if (!v) return false;
      const c = clamp(to, v); st.s = c.s; st.x = c.x; st.y = c.y;
      paint(animate); chrome(); remember(); settle(); emit(); return true;
    }
    const setRest = animate => set({ s: 1, x: 0, y: 0 }, animate);
    const centre = () => { const r = box.getBoundingClientRect(); return { x: r.left + box.clientLeft + box.clientWidth / 2, y: r.top + box.clientTop + box.clientHeight / 2 }; };

    /* ── a sharper picture for the zoom now, and the small one back when it is whole ── */
    function settle() { clearTimeout(settleT); settleT = setTimeout(() => { settleT = 0; if (!destroyed && box.isConnected) st.s > 1.001 ? sharpen() : unsharp(); }, SETTLE_MS); }
    function swap(from, to) {
      to.style.transition = 'none'; to.style.transform = from.style.transform;
      for (const k of ['scale', 'offsetX', 'offsetY']) if (from.dataset[k] != null) to.dataset[k] = from.dataset[k];
      lastMedia = to; from.replaceWith(to);
    }
    function unsharp() {
      if (!hi) return; const was = hi; hi = null; hiJob++;
      const cur = findMedia(); if (cur && cur === was.el && was.orig) swap(cur, was.orig);
    }
    async function loadFirst(srcs, like) {
      for (const src of srcs.filter(Boolean)) {
        const im = new root.Image(); if (like && like.crossOrigin != null) im.crossOrigin = like.crossOrigin; im.decoding = 'async'; im.referrerPolicy = 'no-referrer'; im.src = src;
        try { await im.decode(); return im; } catch (_) {}
      }
      return null;
    }
    async function sharpen() {
      if (typeof opts.hires !== 'function') return;
      const m = findMedia(); if (!m || m.complete === false) return;
      const dpr = Math.min(2, root.devicePixelRatio || 1), want = Math.min(opts.maxPx || 2048, Math.ceil(Math.max(box.clientWidth, box.clientHeight) * st.s * dpr));
      if (hi && hi.px >= want * 0.95) return;
      const job = ++hiJob, key = curKey, stale = () => job !== hiJob || key !== curKey || destroyed || st.s <= 1.001;
      let out = null; try { out = await opts.hires({ scale: st.s, px: want, media: m, box, key }); } catch (_) {}
      if (!out || stale()) return;
      let el = out.el || null;
      if (!el) el = await loadFirst(out.srcs || [out.src], m);
      if (!el || stale()) return;
      const cur = findMedia(); if (!cur) return;
      const orig = hi ? hi.orig : cur;
      el.className = cur.className; if ('alt' in cur) el.alt = cur.alt || ''; el.draggable = false;
      swap(cur, el); hi = { orig, el, px: out.px || want };
    }

    /* ── gestures ── */
    function zoomBy(factor, ax, ay, animate) {
      const v = view(); if (!v) return;
      set(zoomAbout(st, v, st.s * factor, ax, ay), animate);
    }
    function clickAt(clientX, clientY) {
      const v = view(); if (!v) return; const c = centre();
      set(clickTarget(st, v, clientX - c.x, clientY - c.y), true);
    }
    function toggle() { if (st.s > 1.0001) setRest(true); else { const v = view(); if (v) set(zoomAbout(st, v, 2, 0, 0), true); } }
    const beginPinch = () => {
      const [a, b] = [...pts.values()], c = centre();
      pinch = { d0: Math.hypot(a.x - b.x, a.y - b.y) || 1, s0: st.s, mx: (a.x + b.x) / 2 - c.x, my: (a.y + b.y) / 2 - c.y, x0: st.x, y0: st.y };
      down = null; grabbing = false;
    };
    function movePinch() {
      const [a, b] = [...pts.values()], v = view(); if (!pinch || !v || !b) return; const c = centre();
      const d = Math.hypot(a.x - b.x, a.y - b.y) || 1, s1 = Math.min(MAX_SCALE, Math.max(1, pinch.s0 * d / pinch.d0)), mx = (a.x + b.x) / 2 - c.x, my = (a.y + b.y) / 2 - c.y;
      // the point of the picture that was under the fingers' midpoint stays under it, as the midpoint moves
      const px = pinch.mx / pinch.s0 - pinch.x0, py = pinch.my / pinch.s0 - pinch.y0;
      set(s1 <= 1 ? { s: 1, x: 0, y: 0 } : { s: s1, x: mx / s1 - px, y: my / s1 - py }, false);
    }
    on('pointerdown', ev => {
      if (!findMedia() || pressing()) return;
      dragged = false;
      if (ev.pointerType === 'mouse' && ev.button !== 0) return;
      if (ev.target.closest && ev.target.closest('.zpReset')) return;
      pts.set(ev.pointerId, { x: ev.clientX, y: ev.clientY });
      if (pts.size === 2) { beginPinch(); for (const id of pts.keys()) { try { box.setPointerCapture(id); } catch (_) {} } return; }
      if (pts.size === 1 && st.s > 1.0001) {
        down = { sx: ev.clientX, sy: ev.clientY, lx: ev.clientX, ly: ev.clientY };
        try { box.setPointerCapture(ev.pointerId); } catch (_) {}
        if (ev.pointerType === 'mouse') ev.preventDefault();
      }
    });
    on('pointermove', ev => {
      const p = pts.get(ev.pointerId); if (!p) return;
      p.x = ev.clientX; p.y = ev.clientY;
      if (pinch) { if (pts.size >= 2) { ev.preventDefault(); movePinch(); } return; }
      if (!down) return; ev.preventDefault();
      if (!dragged && (Math.abs(ev.clientX - down.sx) > DRAG_THRESHOLD || Math.abs(ev.clientY - down.sy) > DRAG_THRESHOLD)) { dragged = true; grabbing = true; chrome(); }
      set({ s: st.s, x: st.x + (ev.clientX - down.lx) * DAMPING, y: st.y + (ev.clientY - down.ly) * DAMPING }, false);
      down.lx = ev.clientX; down.ly = ev.clientY;
    });
    const release = ev => {
      if (!pts.has(ev.pointerId)) return;
      pts.delete(ev.pointerId);
      try { if (box.hasPointerCapture && box.hasPointerCapture(ev.pointerId)) box.releasePointerCapture(ev.pointerId); } catch (_) {}
      if (pinch && pts.size < 2) {
        pinch = null; quietUntil = Date.now() + 400;   // (the finger that lifts last is not a tap)
        if (st.s < 1.05) setRest(true);
        const rest = [...pts.values()][0]; if (rest && st.s > 1.0001) down = { sx: rest.x, sy: rest.y, lx: rest.x, ly: rest.y };
      }
      if (!pts.size) { down = null; if (grabbing) { grabbing = false; chrome(); } }
    };
    on('pointerup', release); on('pointercancel', release); on('lostpointercapture', release);
    on('click', ev => {
      if (!findMedia()) return;
      if (ev.target.closest && ev.target.closest('.zpReset')) return;
      ev.preventDefault();
      if (dragged) { dragged = false; return; }   // (a pan is not a click)
      if (Date.now() < quietUntil) return;
      try { box.focus({ preventScroll: true }); } catch (_) {}
      if (ev.detail === 0 && !ev.clientX && !ev.clientY) { toggle(); return; }   // (a click made by a script, or a key: the middle)
      clickAt(ev.clientX, ev.clientY);
    });
    on('dragstart', ev => ev.preventDefault());
    on('wheel', ev => {
      const mode = opts.wheel; if (mode === 'off' || !findMedia() || pressing()) return;
      const mod = ev.ctrlKey || ev.metaKey;
      if (mode !== 'always' && !mod) return;            // the plain wheel scrolls the window
      const inwards = ev.deltaY < 0;
      // (the page's own zoom is not wanted over a picture; any other wheel with nothing left to zoom is left to the page)
      if (!ev.ctrlKey && (!inwards && st.s <= 1.0001 || inwards && st.s >= MAX_SCALE)) return;
      ev.preventDefault();
      const c = centre(), step = mode === 'always' ? (inwards ? WHEEL_STEP : 1 / WHEEL_STEP) : Math.pow(WHEEL_STEP, Math.max(-1, Math.min(1, -ev.deltaY / 40)));
      zoomBy(step, ev.clientX - c.x, ev.clientY - c.y, false);
    }, { passive: false });
    on('keydown', ev => {
      if (ev.target !== box || !findMedia() || ev.ctrlKey || ev.metaKey || ev.altKey) return;
      const k = ev.key, zoomed = st.s > 1.0001; let used = true;
      if (k === 'Escape') { if (zoomed) setRest(true); else used = false; }
      else if (k === 'Enter' || k === ' ') { if (!ev.repeat) toggle(); }
      else if (k === '+' || k === '=') zoomBy(KEY_STEP, 0, 0, true);
      else if (k === '-' || k === '_') zoomBy(1 / KEY_STEP, 0, 0, true);
      else if (k === '0') setRest(true);
      else if (zoomed && k === 'ArrowLeft') set({ s: st.s, x: st.x + ARROW_STEP, y: st.y }, false);
      else if (zoomed && k === 'ArrowRight') set({ s: st.s, x: st.x - ARROW_STEP, y: st.y }, false);
      else if (zoomed && k === 'ArrowUp') set({ s: st.s, x: st.x, y: st.y + ARROW_STEP }, false);
      else if (zoomed && k === 'ArrowDown') set({ s: st.s, x: st.x, y: st.y - ARROW_STEP }, false);
      else used = false;
      if (used) { ev.preventDefault(); ev.stopPropagation(); }
    });

    /* the way back to the whole picture for a mouse (a small round button, only while zoomed): inside the frame, so nothing moves */
    function button() {
      if (opts.reset === false || btn) return;
      btn = doc.createElement('button'); btn.type = 'button'; btn.className = 'zpReset'; btn.hidden = true; btn.setAttribute('aria-label', 'Show the whole picture'); btn.textContent = '↺';
      btn.addEventListener('click', e => { e.preventDefault(); e.stopPropagation(); setRest(true); try { box.focus({ preventScroll: true }); } catch (_) {} }, { signal: abort.signal });
      box.appendChild(btn);
    }
    /** The frame's picture came, went or was swapped by someone else: the state is put on whatever is there now. */
    function sync() {
      if (destroyed) return;
      if (btn && btn.parentNode !== box) box.appendChild(btn);
      const m = findMedia();
      if (m === lastMedia) return;
      if (hi && m !== hi.el) { hi = null; hiJob++; }
      lastMedia = m;
      if (m) {
        m.draggable = false; m.style.webkitUserDrag = 'none';
        if (m.tagName === 'IMG' && !m.complete) m.addEventListener('load', () => { if (!destroyed && findMedia() === m) { reclamp(); if (st.s > 1.001) settle(); } }, { once: true });
        reclamp(true);
        if (st.s > 1.001) settle();
      } else { grabbing = false; down = null; }
      chrome();
    }
    function reclamp(force) {
      const m = findMedia(); if (!m) return;
      const v = view(m), c = clamp(st, v);
      if (c.x !== st.x || c.y !== st.y) { st.x = c.x; st.y = c.y; remember(); }
      if (force || st.s > 1.0001 || m.dataset.scale != null) paint(false);
    }
    const mo = root.MutationObserver ? new root.MutationObserver(() => sync()) : null;
    if (mo) mo.observe(box, { childList: true, subtree: true });
    if (ro) ro.observe(box);

    const ctl = {
      box,
      /** What the slot shows now; another picture (another piece) starts whole. */
      key(k) {
        k = keyStr(k); if (k === curKey) return;
        curKey = k; unsharp(); hiJob++; hi = null;
        st.s = 1; st.x = 0; st.y = 0; kept.delete(opts.id || ''); paint(false); chrome(); emit();
      },
      reset(animate) { if (st.s > 1.0001 || st.x || st.y) { setRest(!!animate); } else { kept.delete(opts.id || ''); } },
      state: () => ({ s: st.s, x: st.x, y: st.y, key: curKey }),
      zoomAt(clientX, clientY) { clickAt(clientX, clientY); },
      toggle,
      refresh() { lastMedia = undefined; sync(); },
      update(next) {
        if (next) { opts = Object.assign(opts, next); if (next.key !== undefined) ctl.key(next.key); }
        button(); sync();
      },
      persistent: !!opts.persistent,
      _resized() { reclamp(); chrome(); },
      destroy() {
        if (destroyed) return; destroyed = true; clearTimeout(settleT); hiJob++;
        abort.abort(); if (mo) mo.disconnect(); if (ro) ro.unobserve(box);
        if (btn) btn.remove(); live.delete(ctl);
        if (byBox.get(box) === ctl) byBox.delete(box);
        pts.clear();
      },
      get id() { return opts.id || ''; }
    };
    // a frame drawn again for the same piece and picture keeps the zoom and pan it had
    const was = opts.id && kept.get(opts.id);
    if (was && was.key === curKey) { st.s = was.s; st.x = was.x; st.y = was.y; } else if (opts.id) kept.delete(opts.id);
    box.dataset.zp = 'none';
    button(); lastMedia = undefined; sync();
    return ctl;
  }

  function prune() { for (const c of [...live]) if (!c.box.isConnected && !c.persistent) c.destroy(); }
  function attach(box, opts) {
    if (!box || !box.addEventListener) return null;
    prune();
    let c = byBox.get(box);
    if (c) { c.update(opts); return c; }
    c = make(box, opts); byBox.set(box, c); live.add(c); return c;
  }
  const within = (rootEl, fn) => { let any = false; for (const c of [...live]) if (c.box.isConnected && (!rootEl || rootEl.contains(c.box)) && fn(c)) any = true; return any; };
  return {
    attach, math,
    resetWithin: (rootEl, animate) => within(rootEl, c => { if (c.state().s > 1.0001) { c.reset(animate !== false); return true; } return false; }),
    zoomedWithin: rootEl => within(rootEl, c => c.state().s > 1.0001),
    forget(prefix) { prefix = String(prefix || ''); for (const id of [...kept.keys()]) if (id.startsWith(prefix)) kept.delete(id); for (const c of [...live]) if (c.id && c.id.startsWith(prefix)) c.reset(false); },
    stats: () => ({ live: live.size, kept: kept.size, ids: [...kept.keys()] }),
    version: 1
  };
});
