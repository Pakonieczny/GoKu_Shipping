/*  send-flight.js — the send animation, shared by the inbox (etsy-mail-1) and the Charm Sorter's customer messages.
 *  ═══════════════════════════════════════════════════════════════════════
 *  A sent message's words travel from the box they were written in into their places in the message's own bubble in the
 *  thread above. One component, so both apps send in exactly the same way (the inbox's component, moved here unchanged:
 *  Paul, 10 Oct 2026: "These animations must feel exactly the same so that the user naturally feels that they're using
 *  the same application").
 *
 *  The caller draws the message's own bubble first (the local copy of a send), then
 *
 *      const flight = SendFlight.prepare({ surface, source, textarea, text });   // before the box is emptied
 *      ...empty the box, draw the bubble...
 *      SendFlight.launch(flight, bubbleId);                                      // the bubble has data-mid="bubbleId"
 *      SendFlight.retract(bubbleId).then(putTheTextBack);                        // a send that failed: the bubble folds away
 *      SendFlight.flash(box);                                                    // the failed text is back: draw the eye to it
 *      SendFlight.holding() / SendFlight.afterFlight(fn)                         // renders wait while the words are in the air
 *
 *  Three layouts are known. 'desktop' and 'mobile' are the inbox's own (they read its elements by id, as they always did).
 *  'pane' is any other: the history scrolls above a box that stays beneath it, newest message at the bottom (the phone's
 *  way), and the caller names the elements: { list, scroller, host, bounds, textSelector, bubbleSelector, fit, layerHost }.
 *  A list that scrolls itself (list === scroller) slides its rows instead of the list. All of these are optional for the
 *  inbox; nothing about its behaviour depends on them.
 *
 *  The bubble opens just above the box while the history makes room. A copy of each word lifts from exactly where it sat
 *  in the box and flies on a soft arc onto the same word in the bubble, re-wrapping into the bubble's lines. When the last
 *  word lands the bubble's own text takes over, the copies fade and the box eases back to its resting height. Words move
 *  with transforms; only their colour repaints. Renders wait while a flight runs (holding/afterFlight) so the bubble the
 *  words aim at stays where it is. Every step is guarded: if anything fails, the bubble simply shows. With
 *  prefers-reduced-motion the bubble just appears and the box is fitted at once.
 */
(function (root) {
  'use strict';

  // The component's own rules (class names kept as the inbox always had them).
  const CSS = `
    .em-flight-layer {
      position: fixed; left: 0; top: 0; right: 0; bottom: 0;
      pointer-events: none; z-index: 950; overflow: hidden;
    }
    .em-flight-x { position: absolute; left: 0; top: 0; will-change: transform; }
    .em-flight-y {
      display: block; white-space: pre; line-height: normal;
      transform-origin: 0 0; will-change: transform, opacity;
    }
    .em-flight-text-hidden .em-msg-text,
    .em-flight-text-hidden .em-msg-text *,
    .em-flight-text-hidden .m-msg-text {
      color: transparent !important;
      text-decoration-color: transparent !important;
    }
    /* A layout the caller describes ('pane'): its text element is marked and hidden the same way. */
    .em-flight-text-hidden [data-flight-text],
    .em-flight-text-hidden [data-flight-text] * {
      color: transparent !important;
      text-decoration-color: transparent !important;
    }
    /* The emptied reply box keeps its hint hidden while the words lift off it. */
    .em-flight-quiet::placeholder { color: transparent !important; }
    /* A copy of a box edge that slides while the emptied reply box settles. */
    .em-flight-band {
      position: absolute; box-sizing: border-box; margin: 0; padding: 0;
      border: 0 solid transparent; pointer-events: none;
    }
  `;
  // Put the rules where the script is (the page's own order of rules), once.
  (function installCss() {
    try {
      if (typeof document === 'undefined' || document.getElementById('sendFlightCss')) return;
      const s = document.createElement('style');
      s.id = 'sendFlightCss';
      s.textContent = CSS;
      const me = document.currentScript;
      if (me && me.parentNode) me.parentNode.insertBefore(s, me);
      else (document.head || document.documentElement).appendChild(s);
    } catch (_) {}
  })();

  const EASE_X    = 'cubic-bezier(.45,0,.2,1)';   // words drift sideways late…
  const EASE_Y    = 'cubic-bezier(.22,.7,.25,1)'; // …after rising early: a soft arc
  const MAX_WORDS = 140;

  let active = null;              // the flight in progress
  const deferred = new Set();     // renders waiting for it to land

  function reducedMotion() {
    try { return window.matchMedia('(prefers-reduced-motion: reduce)').matches; } catch (_) { return false; }
  }
  function sel(id) {
    return '[data-mid="' + (window.CSS && CSS.escape ? CSS.escape(id) : String(id).replace(/"/g, '\\"')) + '"]';
  }
  function listFor(surface) {
    return document.getElementById(surface === 'mobile' ? 'mMessagesList' : 'emThreadBox');
  }
  function scrollerFor(surface, list) {
    if (surface === 'mobile') return document.getElementById('mConvBody');
    for (let p = list && list.parentElement; p && p !== document.body; p = p.parentElement) {
      const oy = getComputedStyle(p).overflowY;
      if (oy === 'auto' || oy === 'scroll') return p;
    }
    return null;
  }
  function anchorFor(surface) {
    return surface === 'desktop' ? document.getElementById('emComposerCard') : null;
  }
  /** 'desktop' keeps the reply box still and moves the history; every other layout keeps the newest message in view. */
  function followsNewest(surface) { return surface !== 'desktop'; }
  /** The selectors that find a bubble's text and its coloured box (the inbox's own, unless the caller names them). */
  function selectorsOf(prep) {
    const mobile = prep.surface === 'mobile';
    return {
      text: prep.textSelector || (mobile ? '.m-msg-text' : '.em-msg-text'),
      bubble: prep.bubbleSelector || (mobile ? '.m-msg-bubble' : null)
    };
  }
  /** The rows of a list that scrolls itself, those near enough to the visible part to be seen sliding. */
  function rowsNear(scroller, pad) {
    const q = scroller.getBoundingClientRect();
    return Array.from(scroller.children).filter(el => {
      const r = el.getBoundingClientRect();
      return r.bottom > q.top - pad && r.top < q.bottom + pad;
    });
  }
  /** What slides when a bubble comes or goes: the list, or (a list that scrolls itself) its rows. */
  function slidersOf(prep, node, d) {
    if (!prep.rows) return [prep.list];
    const near = rowsNear(prep.scroller, Math.abs(d) + 4);
    if (near.indexOf(node) < 0) near.push(node);
    return near;
  }
  /** Part of el actually on screen (inside every clipping ancestor). */
  function visibleRect(el) {
    const r = el.getBoundingClientRect();
    let t = r.top, l = r.left, b = r.bottom, rt = r.right;
    for (let p = el.parentElement; p && p !== document.documentElement; p = p.parentElement) {
      const cs = getComputedStyle(p);
      if (cs.overflowX !== 'visible' || cs.overflowY !== 'visible') {
        const q = p.getBoundingClientRect();
        t = Math.max(t, q.top); l = Math.max(l, q.left); b = Math.min(b, q.bottom); rt = Math.min(rt, q.right);
      }
    }
    t = Math.max(t, 0); l = Math.max(l, 0);
    b = Math.min(b, window.innerHeight); rt = Math.min(rt, window.innerWidth);
    return b > t && rt > l ? { top: t, left: l, bottom: b, right: rt } : null;
  }

  /** Where each word of a textarea's value is drawn: a hidden mirror with
   *  the same box, font and wrapping, offset by the textarea's scroll. */
  function textareaWords(ta) {
    const cs = getComputedStyle(ta);
    const r = ta.getBoundingClientRect();
    const m = document.createElement('div');
    ['boxSizing', 'fontFamily', 'fontSize', 'fontWeight', 'fontStyle', 'fontVariant', 'letterSpacing',
     'wordSpacing', 'lineHeight', 'textTransform', 'textIndent', 'tabSize', 'direction', 'wordBreak',
     'paddingTop', 'paddingRight', 'paddingBottom', 'paddingLeft',
     'borderTopWidth', 'borderRightWidth', 'borderBottomWidth', 'borderLeftWidth',
     'borderTopStyle', 'borderRightStyle', 'borderBottomStyle', 'borderLeftStyle'
    ].forEach(k => { m.style[k] = cs[k]; });
    Object.assign(m.style, {
      position: 'fixed', left: r.left + 'px', top: r.top + 'px', width: r.width + 'px',
      height: 'auto', margin: '0', visibility: 'hidden', pointerEvents: 'none', zIndex: '-1',
      whiteSpace: 'pre-wrap', overflowWrap: 'break-word', wordWrap: 'break-word',
      overflow: 'hidden', borderColor: 'transparent'
    });
    // A textarea's scrollbar narrows its text; the mirror has none.
    const gutter = ta.offsetWidth - ta.clientWidth -
      (parseFloat(cs.borderLeftWidth) || 0) - (parseFloat(cs.borderRightWidth) || 0);
    if (gutter > 0) m.style.paddingRight = ((parseFloat(cs.paddingRight) || 0) + gutter) + 'px';
    const text = ta.value;
    const spans = [];
    const re = /\S+/g;
    let last = 0, mm;
    while ((mm = re.exec(text))) {
      if (mm.index > last) m.appendChild(document.createTextNode(text.slice(last, mm.index)));
      const sp = document.createElement('span');
      sp.textContent = mm[0];
      m.appendChild(sp);
      spans.push(sp);
      last = mm.index + mm[0].length;
    }
    if (last < text.length) m.appendChild(document.createTextNode(text.slice(last)));
    document.body.appendChild(m);
    const sx = ta.scrollLeft, sy = ta.scrollTop;
    const out = spans.map(sp => {
      const q = sp.getClientRects()[0] || sp.getBoundingClientRect();
      return { text: sp.textContent, left: q.left - sx, top: q.top - sy, width: q.width, height: q.height };
    });
    m.remove();
    return out;
  }

  /** Where each whitespace-separated word inside an element is drawn
   *  (words may span text nodes, e.g. around a link). */
  function elementWords(el) {
    const nodes = [];
    let full = '';
    const w = document.createTreeWalker(el, NodeFilter.SHOW_TEXT);
    for (let n = w.nextNode(); n; n = w.nextNode()) { nodes.push({ n, at: full.length }); full += n.nodeValue; }
    if (!nodes.length) return [];
    const locate = pos => {
      let hit = nodes[0];
      for (const x of nodes) { if (x.at <= pos) hit = x; else break; }
      return hit;
    };
    const out = [];
    const range = document.createRange();
    const re = /\S+/g;
    let mm;
    while ((mm = re.exec(full))) {
      const a = locate(mm.index), b = locate(mm.index + mm[0].length - 1);
      try {
        range.setStart(a.n, mm.index - a.at);
        range.setEnd(b.n, mm.index + mm[0].length - b.at);
      } catch (_) { continue; }
      const q = range.getClientRects()[0] || range.getBoundingClientRect();
      out.push({ text: mm[0], left: q.left, top: q.top, width: q.width, height: q.height });
    }
    return out;
  }

  function releaseHold() {
    active = null;
    const fns = Array.from(deferred);
    deferred.clear();
    fns.forEach(fn => { try { fn(); } catch (e) { console.warn('[SendFlight] deferred render:', e); } });
  }

  function clearGrowth(node) {
    ['height', 'paddingTop', 'paddingBottom', 'marginBottom', 'overflow', 'opacity'].forEach(k => { node.style[k] = ''; });
  }

  function abort(f) {
    if (!f || f.done) return;
    f.done = true;
    clearTimeout(f.safety);
    try { if (f.layer) f.layer.remove(); } catch (_) {}
    (f.anims || []).forEach(a => { try { a.cancel(); } catch (_) {} });
    try { if (f.node) { f.node.classList.remove('em-flight-text-hidden'); clearGrowth(f.node); } } catch (_) {}
    try { if (f.quiet) f.quiet.classList.remove('em-flight-quiet'); } catch (_) {}
    try { if (f.scroller) f.scroller.style.overflowAnchor = f.oldAnchor || ''; } catch (_) {}
    if (active === f) releaseHold();
  }

  /** Keep the view steady while the layout changes under it: the reply
   *  box stays still on desktop, the phone stays on the newest message. */
  function keeper(f) {
    if (!f || !f.scroller) return () => {};
    if (followsNewest(f.surface)) {
      return () => { f.scroller.scrollTop = f.scroller.scrollHeight; };
    }
    return () => {
      if (!f.anchorEl || !f.anchorEl.isConnected || !f.scroller.isConnected) return;
      const d = f.anchorEl.getBoundingClientRect().bottom - f.anchorBottom;
      if (Math.abs(d) >= 0.5) f.scroller.scrollTop += d;
    };
  }

  /** Measure what a flight needs from the reply box. Call before the box
   *  is emptied. Returns null when nothing should fly. */
  function prepare(o) {
    if (!o || !o.text || document.hidden) return null;
    const surface = o.surface === 'mobile' ? 'mobile' : o.surface === 'pane' ? 'pane' : 'desktop';
    const list = o.list || listFor(surface);
    if (!list || !list.isConnected) return null;
    const scroller = o.scroller || scrollerFor(surface, list);
    const anchorEl = anchorFor(surface);
    const ta = o.textarea || null;
    const prep = {
      surface, list, scroller, anchorEl, textarea: ta,
      anchorBottom: anchorEl ? anchorEl.getBoundingClientRect().bottom : 0,
      reduced: reducedMotion(),
      words: [], font: null, clip: null,
      // what a layout the caller describes names (the inbox's two read their own elements)
      rows: !!scroller && scroller === list,
      host: o.host || null, bounds: o.bounds || null, layerHost: o.layerHost || null,
      textSelector: o.textSelector || null, bubbleSelector: o.bubbleSelector || null, fit: o.fit || null
    };
    if (prep.reduced) return prep;
    const src = o.source && o.source.isConnected ? o.source : ta;
    if (!src) return prep;
    const clip = visibleRect(src);
    if (!clip) return prep;
    const cs = getComputedStyle(src);
    prep.font = {
      family: cs.fontFamily, size: cs.fontSize, weight: cs.fontWeight, style: cs.fontStyle,
      spacing: cs.letterSpacing, color: cs.color
    };
    const words = src.tagName === 'TEXTAREA' ? textareaWords(src) : elementWords(src);
    prep.words = words.map(w => Object.assign(w, {
      visible: w.top + w.height > clip.top && w.top < clip.bottom && w.left < clip.right && w.left + w.width > clip.left
    }));
    prep.clip = clip;
    return prep;
  }

  /** Copies of the reply box's words, lined up on them, each paired with
   *  its place in the bubble. Returns { layer, pairs } or null. */
  function buildWords(prep, dest, destSize, scroller) {
    const n = Math.min(prep.words.length, dest.length, MAX_WORDS);
    if (!prep.font || !n) return null;
    // Clip to the conversation and the reply box, never over headers.
    const areas = [prep.clip];
    if (scroller) areas.push(scroller.getBoundingClientRect());
    const bounds = prep.bounds || (prep.surface === 'mobile' ? document.getElementById('mComposer') : null);
    if (bounds && bounds.isConnected) areas.push(bounds.getBoundingClientRect());
    const top = Math.min.apply(null, areas.map(a => a.top));
    const left = Math.min.apply(null, areas.map(a => a.left));
    const bottom = Math.max.apply(null, areas.map(a => a.bottom));
    const right = Math.max.apply(null, areas.map(a => a.right));

    const layer = document.createElement('div');
    layer.className = 'em-flight-layer';
    const pairs = [];
    for (let i = 0; i < n; i++) {
      const s = prep.words[i], d = dest[i];
      if (!s.visible && !(d.top < bottom && d.top + d.height > top)) continue;
      const x = document.createElement('span');
      x.className = 'em-flight-x';
      const y = document.createElement('span');
      y.className = 'em-flight-y';
      y.textContent = s.text;
      Object.assign(y.style, {
        fontFamily: prep.font.family, fontSize: prep.font.size, fontWeight: prep.font.weight,
        fontStyle: prep.font.style, letterSpacing: prep.font.spacing, color: prep.font.color
      });
      // Words scrolled out of the box start at its edge, unseen.
      const sTop = s.visible ? s.top
        : Math.max(prep.clip.top, Math.min(prep.clip.bottom - s.height, s.top));
      x.style.transform = 'translate(' + s.left + 'px,' + sTop + 'px)';
      x.appendChild(y);
      layer.appendChild(x);
      pairs.push({ s, d, x, y, sTop });
    }
    if (!pairs.length) return null;
    // (A modal dialog sits above the page's own layers: the words go inside it, still covering the viewport.)
    const layerHost = prep.layerHost && prep.layerHost.isConnected ? prep.layerHost : null;
    (layerHost || document.body).appendChild(layer);
    if (layerHost) {
      const q = layer.getBoundingClientRect();
      if (Math.abs(q.left) >= 0.5 || Math.abs(q.top) >= 0.5) {
        Object.assign(layer.style, { left: (-q.left) + 'px', top: (-q.top) + 'px', right: 'auto', bottom: 'auto',
          width: window.innerWidth + 'px', height: window.innerHeight + 'px' });
      }
    }
    // The layer covers the layout viewport; clip it in its own terms.
    const L = layer.getBoundingClientRect();
    layer.style.clipPath = 'inset(' + Math.max(0, top - L.top) + 'px ' + Math.max(0, L.right - right) + 'px ' +
                           Math.max(0, L.bottom - bottom) + 'px ' + Math.max(0, left - L.left) + 'px)';
    // Line each copy's glyphs up exactly on the word it replaces.
    const range = document.createRange();
    const got = pairs.map(pr => { range.selectNodeContents(pr.y); return range.getBoundingClientRect(); });
    const k = destSize > 0 && parseFloat(prep.font.size) > 0 ? destSize / parseFloat(prep.font.size) : 1;
    pairs.forEach((pr, i) => {
      const g = got[i];
      const ox = g.left - pr.s.left, oy = g.top - pr.sTop;   // glyphs inside the copy's box
      pr.baseL = pr.s.left - ox;
      pr.baseT = pr.sTop - oy;
      pr.k = k;
      // The copy scales from its corner, so its glyph offset scales too.
      pr.dx = (pr.d.left - pr.s.left) + ox * (1 - k);
      pr.dy = (pr.d.top - pr.sTop) + oy * (1 - k);
      pr.x.style.transform = 'translate(' + pr.baseL + 'px,' + pr.baseT + 'px)';
    });
    return { layer, pairs };
  }

  /** Fly prep's words into the bubble drawn for localId. */
  function launch(prep, localId) {
    if (!prep) return;
    if (active) abort(active);
    const node = prep.list && prep.list.isConnected && prep.list.querySelector(sel(localId));
    if (!node || prep.reduced) { settleComposer(prep, null, true); return; }
    const f = {
      surface: prep.surface, node, list: prep.list, scroller: prep.scroller && prep.scroller.isConnected ? prep.scroller : null,
      anchorEl: prep.anchorEl, anchorBottom: prep.anchorBottom, done: false
    };
    const keep = keeper(f);
    const scroller = f.scroller;
    f.oldAnchor = scroller ? scroller.style.overflowAnchor : '';
    if (scroller) scroller.style.overflowAnchor = 'none';

    // 1. Final layout: where everything ends up once the bubble is in.
    keep();
    const sels = selectorsOf(prep);
    const bubble = sels.bubble ? (node.querySelector(sels.bubble) || node) : node;
    const textEl = node.querySelector(sels.text);
    if (textEl && prep.textSelector) textEl.setAttribute('data-flight-text', '');
    const dest = textEl && prep.words.length ? elementWords(textEl) : [];
    const bubbleColor = getComputedStyle(bubble).backgroundColor;
    const destColor = getComputedStyle(textEl || bubble).color;
    const destSize = textEl ? parseFloat(getComputedStyle(textEl).fontSize) : 0;

    // 2. The words, lined up on the reply box.
    let built = null;
    try { built = buildWords(prep, dest, destSize, scroller); } catch (e) { console.warn('[SendFlight] words:', e); }
    if (built) {
      f.layer = built.layer;
      if (prep.textarea && prep.textarea.isConnected) {
        f.quiet = prep.textarea;
        f.quiet.classList.add('em-flight-quiet');
      }
    }

    // 3. The bubble opens just above the reply box while the history
    //    makes room. The layout is final from the start: the history
    //    (and, when it can't scroll, the reply box) slides from where it
    //    was before Send and the bubble is uncovered from its top, with
    //    transform, clip-path and opacity only, so no frame re-lays out
    //    the conversation.
    if (built) node.classList.add('em-flight-text-hidden');
    const comp = followsNewest(prep.surface) ? null : (f.anchorEl && f.anchorEl.isConnected ? f.anchorEl : null);
    // (a list that scrolls itself moves its rows: the row above the bubble stands for the history)
    const above = prep.rows ? node.previousElementSibling : null;
    const topOfList = () => prep.rows ? (above && above.isConnected ? above.getBoundingClientRect().top : 0)
                                      : prep.list.getBoundingClientRect().top;
    const listEnd = topOfList();
    const compEnd = comp ? comp.getBoundingClientRect().top : 0;
    const oldDisplay = node.style.display;
    node.style.display = 'none';
    keep();
    const dList = topOfList() - listEnd;
    const dComp = comp ? comp.getBoundingClientRect().top - compEnd : 0;
    node.style.display = oldDisplay;
    keep();
    const room = Math.max(0, dList - dComp);

    active = f;
    f.safety = setTimeout(() => abort(f), 2600);

    const GROW = { duration: 300, easing: 'cubic-bezier(.33,1,.68,1)' };
    f.anims = [];
    try {
      // (the bubble opens to the opacity it rests at: 1 in the inbox)
      const rest = parseFloat(getComputedStyle(node).opacity);
      const full = rest >= 0 && rest <= 1 ? rest : 1;
      if (Math.abs(dList) >= 0.5) slidersOf(prep, node, dList).forEach(el => f.anims.push(el.animate([
        { transform: 'translateY(' + dList + 'px)' }, { transform: 'translateY(0px)' }], GROW)));
      if (comp && Math.abs(dComp) >= 0.5) f.anims.push(comp.animate([
        { transform: 'translateY(' + dComp + 'px)' }, { transform: 'translateY(0px)' }], GROW));
      f.anims.push(node.animate([
        { clipPath: 'inset(0px 0px ' + room + 'px 0px)', opacity: 0 },
        { opacity: full, offset: 0.625 },
        { clipPath: 'inset(0px 0px 0px 0px)', opacity: full }], GROW));
    } catch (e) { console.warn('[SendFlight] grow:', e); }

    // 4. Flight: each word rises early and drifts sideways late (a soft
    //    arc), takes the bubble's colour and size on the way.
    let landAt = 0;
    if (built) {
      const pairs = built.pairs;
      const spread = Math.min(170, pairs.length * 14);
      const step = pairs.length > 1 ? spread / (pairs.length - 1) : 0;
      pairs.forEach((pr, i) => {
        const dist = Math.hypot(pr.dx, pr.dy);
        const dur = Math.round(Math.max(440, Math.min(640, 400 + dist * 0.35)));
        const delay = Math.round(i * step);
        landAt = Math.max(landAt, delay + dur);
        const base = 'translate(' + pr.baseL + 'px,' + pr.baseT + 'px)';
        pr.x.animate([
          { transform: base + ' translateX(0px)' },
          { transform: base + ' translateX(' + pr.dx + 'px)' }
        ], { duration: dur, delay, easing: EASE_X, fill: 'both' });
        pr.y.animate([
          { transform: 'translateY(0px) scale(1)', opacity: pr.s.visible ? 1 : 0 },
          { transform: 'translateY(' + pr.dy + 'px) scale(' + pr.k + ')', opacity: 1 }
        ], { duration: dur, delay, easing: EASE_Y, fill: 'both' });
        // The word keeps its ink until it is nearly in place, then takes
        // the bubble's colour quickly (a long blend reads as grey).
        pr.y.animate([
          { color: prep.font.color },
          { color: prep.font.color, offset: 0.6 },
          { color: destColor, offset: 0.85 },
          { color: destColor }
        ], { duration: dur, delay, easing: 'linear', fill: 'both' });
      });
    }

    // 5. Landing: the bubble's own text takes over and the copies fade.
    setTimeout(() => {
      if (f.done) return;
      if (!node.isConnected) { abort(f); return; }
      f.done = true;
      clearTimeout(f.safety);
      (f.anims || []).forEach(a => { try { a.cancel(); } catch (_) {} });
      node.classList.remove('em-flight-text-hidden');
      // The reply box's hint stays hidden until the box has settled.
      clearGrowth(node);
      if (scroller) scroller.style.overflowAnchor = f.oldAnchor || '';
      // The heavy work (held renders, the reply box's resize) waits until
      // the copies have faded, so it never stalls the landing.
      let settled = false;
      const afterLanding = () => {
        if (settled) return;
        settled = true;
        // Renders held during the flight run now; they may swap the node.
        if (active === f) releaseHold();
        try {
          const live = (prep.list && prep.list.querySelector(sel(localId))) || node;
          const target = sels.bubble ? (live.querySelector(sels.bubble) || live) : live;
          const glow = String(bubbleColor || 'rgb(3,102,214)').replace(/^rgba?\(([^,]+),([^,]+),([^,)]+).*$/, 'rgba($1,$2,$3,');
          target.animate([
            { boxShadow: '0 0 0 0 ' + glow + '0.38)' },
            { boxShadow: '0 0 0 7px ' + glow + '0)' }
          ], { duration: 650, easing: 'ease-out' });
        } catch (_) {}
        settleComposer(prep, f, false);
      };
      if (f.layer) {
        const layer = f.layer;
        f.layer = null;
        const gone = () => { layer.remove(); afterLanding(); };
        layer.animate([{ opacity: 1 }, { opacity: 0 }], { duration: 140, easing: 'ease-out', fill: 'forwards' })
          .finished.then(gone, gone);
        setTimeout(gone, 400);
      } else afterLanding();
    }, Math.max(320, landAt));
  }

  /** Everything that rides on the reply box's edges: what sits beside it
   *  up to its card, the card's neighbours, and the phone's messages. */
  function ridersOf(ta, host, scroller, near) {
    const out = [];
    const beside = el => {
      const p = el.parentElement;
      if (p) for (const s of p.children) if (s !== el) out.push(s);
    };
    for (let el = ta; el && el !== host; el = el.parentElement) beside(el);
    beside(host);
    if (scroller && !scroller.contains(host)) out.push.apply(out, near ? rowsNear(scroller, 240) : scroller.children);
    return out.filter(el => !/^(SCRIPT|STYLE|TEMPLATE)$/.test(el.tagName) && el.getClientRects().length);
  }

  /** A box's height has just snapped smaller. Over its uniform middle, a
   *  copy of each edge that moved (top or bottom: border, corners and
   *  fill) slides from where the edge was to where it is, with transform
   *  and clip-path only. The copy goes in host (positioned) at z. */
  function edgeBands(host, box, was, now, z, T, anims, bands) {
    const cs = getComputedStyle(box);
    const hr = host.getBoundingClientRect();
    const px = v => parseFloat(v) || 0;
    const line = s => cs['border' + s + 'Width'] + ' ' + cs['border' + s + 'Style'] + ' ' + cs['border' + s + 'Color'];
    [['Top', now.top - was.top], ['Bottom', was.bottom - now.bottom]].forEach(([side, m]) => {
      if (m < 0.5) return;
      const r1 = px(cs['border' + side + 'LeftRadius']), r2 = px(cs['border' + side + 'RightRadius']);
      const cover = px(cs['border' + side + 'Width']) + Math.max(r1, r2, 2);
      const top = side === 'Top';
      const y0 = top ? was.top : now.bottom - cover, y1 = top ? now.top + cover : was.bottom;
      const b = document.createElement('div');
      b.className = 'em-flight-band';
      Object.assign(b.style, {
        left: (now.left - hr.left - host.clientLeft) + 'px', width: now.width + 'px',
        top: (y0 - hr.top - host.clientTop) + 'px', height: (y1 - y0) + 'px', zIndex: z,
        backgroundColor: cs.backgroundColor, borderLeft: line('Left'), borderRight: line('Right')
      });
      b.style['border' + side] = line(side);
      b.style['border' + side + 'LeftRadius'] = r1 + 'px';
      b.style['border' + side + 'RightRadius'] = r2 + 'px';
      host.appendChild(b);
      bands.push(b);
      anims.push(b.animate([
        { transform: 'translateY(0px)', clipPath: 'inset(0px)' },
        { transform: 'translateY(' + (top ? m : -m) + 'px)', clipPath: top ? 'inset(0px 0px ' + m + 'px 0px)' : 'inset(' + m + 'px 0px 0px 0px)' }
      ], T));
    });
  }

  /** Ease the emptied reply box back to its resting height. The height
   *  is set once (the view held steady); what moved slides from where it
   *  was, and the box's edges are drawn sliding in, with transform and
   *  clip-path only. */
  function settleComposer(prep, f, instant) {
    const ta = prep && prep.textarea;
    const unquiet = () => { if (f && f.quiet) f.quiet.classList.remove('em-flight-quiet'); };
    if (!ta || !ta.isConnected || ta.value) { unquiet(); return; }   // the operator is already typing
    const mobile = prep.surface === 'mobile';
    const fit = () => {
      if (prep.fit) prep.fit(ta);
      else if (mobile && typeof window.mAutoGrow === 'function') window.mAutoGrow(ta);
      else { ta.style.height = 'auto'; ta.style.height = ta.scrollHeight + 'px'; }
    };
    const host = prep.host || (mobile ? document.getElementById('mComposer') : prep.anchorEl);
    const scroller = prep.scroller && prep.scroller.isConnected ? prep.scroller : null;
    const animate = !instant && !reducedMotion() && !document.hidden && host && host.isConnected && host.contains(ta);
    if (!animate) { fit(); unquiet(); return; }
    const anims = [], bands = [];
    const oldPos = host.style.position, oldIso = host.style.isolation;
    let over = false;
    const end = () => {
      if (over) return;
      over = true;
      ta.removeEventListener('input', end);
      anims.forEach(a => { try { a.cancel(); } catch (_) {} });
      bands.forEach(b => b.remove());
      host.style.position = oldPos;
      host.style.isolation = oldIso;
      unquiet();
    };
    try {
      const was = ta.getBoundingClientRect(), hostWas = host.getBoundingClientRect();
      const riders = ridersOf(ta, host, scroller, prep.rows);
      const before = riders.map(el => el.getBoundingClientRect().top);
      const keep = keeper({ surface: prep.surface, scroller, anchorEl: host, anchorBottom: hostWas.bottom });
      fit();
      keep();
      const now = ta.getBoundingClientRect();
      if (now.height > was.height - 2) { end(); return; }
      // The card's edge copy goes under its contents (z -1); the box's over it.
      if (getComputedStyle(host).position === 'static') host.style.position = 'relative';
      host.style.isolation = 'isolate';
      const T = { duration: 220, easing: 'cubic-bezier(.33,1,.68,1)', fill: 'forwards' };
      riders.forEach((el, i) => {
        const d = el.getBoundingClientRect().top - before[i];
        if (Math.abs(d) >= 0.5) anims.push(el.animate([{ transform: 'translateY(' + (-d) + 'px)' }, { transform: 'translateY(0px)' }], T));
      });
      edgeBands(host, host, hostWas, host.getBoundingClientRect(), '-1', T, anims, bands);
      edgeBands(host, ta, was, now, 'auto', T, anims, bands);
    } catch (e) { console.warn('[SendFlight] settle:', e); end(); return; }
    // Typing mid-way hands the box straight back.
    ta.addEventListener('input', end);
    Promise.all(anims.map(a => a.finished)).then(end, end);
    setTimeout(end, 700);
  }

  /** A failed send's bubble folds away. Resolves when it is gone. It stays
   *  in the layout while it folds: the history slides to where it will be
   *  and the bubble is covered from its bottom as it fades (transform,
   *  clip-path and opacity only); then it leaves the layout once. */
  function retract(localId, o) {
    // (o: { surface: 'pane', list, scroller }, for a layout the caller describes; the inbox passes nothing)
    const pane = !!o && o.surface === 'pane' && !!o.list;
    const nodes = Array.from((pane ? o.list : document).querySelectorAll(sel(localId)));
    if (active && nodes.indexOf(active.node) >= 0) abort(active);
    if (!nodes.length || reducedMotion() || document.hidden) return Promise.resolve();
    return Promise.all(nodes.map(node => new Promise(resolve => {
      const surface = pane ? 'pane' : node.closest('#mMessagesList') ? 'mobile' : 'desktop';
      const list = pane ? o.list : listFor(surface);
      const f = { surface, scroller: pane ? (o.scroller || scrollerFor(surface, list)) : scrollerFor(surface, list), anchorEl: anchorFor(surface) };
      f.anchorBottom = f.anchorEl ? f.anchorEl.getBoundingClientRect().bottom : 0;
      const rows = !!f.scroller && f.scroller === list;   // a list that scrolls itself moves its rows
      const keep = followsNewest(surface) ? () => {} : keeper(f);
      const comp = f.anchorEl && f.anchorEl.isConnected ? f.anchorEl : null;
      const anims = [];
      let over = false;
      const end = () => {
        if (over) return;
        over = true;
        node.style.display = 'none';
        anims.forEach(a => { try { a.cancel(); } catch (_) {} });
        keep();
        resolve();
      };
      try {
        // Where everything ends up once the bubble is gone, then put back.
        const after = [];
        for (let s = node.nextElementSibling; s; s = s.nextElementSibling) after.push(s);
        const above = rows ? node.previousElementSibling : null;
        const tops = () => [rows ? above : list, comp].concat(after).map(el => el && el.isConnected ? el.getBoundingClientRect().top : 0);
        const t0 = tops();
        const st = f.scroller ? f.scroller.scrollTop : 0;
        const oldDisplay = node.style.display;
        node.style.display = 'none';
        keep();
        const t1 = tops();
        node.style.display = oldDisplay;
        if (f.scroller) f.scroller.scrollTop = st;
        const dList = t1[0] - t0[0], dComp = t1[1] - t0[1];
        const room = Math.max(0, dList - dComp);
        const T = { duration: 260, easing: 'cubic-bezier(.4,0,.2,1)', fill: 'forwards' };
        const slide = (el, d) => {
          if (el && Math.abs(d) >= 0.5) anims.push(el.animate([{ transform: 'translateY(0px)' }, { transform: 'translateY(' + d + 'px)' }], T));
        };
        if (rows) {
          const kept = rowsNear(f.scroller, Math.abs(dList) + 4).filter(el => after.indexOf(el) < 0);
          if (kept.indexOf(node) < 0) kept.push(node);
          kept.forEach(el => slide(el, dList));
        } else slide(list, dList);
        slide(comp, dComp);
        after.forEach((el, i) => slide(el, (t1[i + 2] - t0[i + 2]) - (rows ? 0 : dList)));
        anims.push(node.animate([
          { clipPath: 'inset(0px)', opacity: 1 },
          { opacity: 0, offset: 0.7 },
          { clipPath: 'inset(0px 0px ' + room + 'px 0px)', opacity: 0 }], T));
      } catch (e) { console.warn('[SendFlight] retract:', e); end(); return; }
      Promise.all(anims.map(a => a.finished)).then(end, end);
      setTimeout(end, 700);
    }))).then(() => {});
  }

  /** Draw the eye to the reply box when a failed reply comes back. */
  function flash(el) {
    try {
      if (!el || reducedMotion()) return;
      el.animate([
        { boxShadow: '0 0 0 3px rgba(207,34,46,.30)' },
        { boxShadow: '0 0 0 3px rgba(207,34,46,.30)', offset: 0.4 },
        { boxShadow: '0 0 0 0 rgba(207,34,46,0)' }
      ], { duration: 1400, easing: 'ease-out' });
    } catch (_) {}
  }

  const api = {
    prepare, launch, retract, flash,
    /** True while a flight is in the air. Given a list, only a flight into that list holds it. */
    holding(list) {
      if (!active) return false;
      if (!active.node.isConnected) { abort(active); return false; }
      return !list || active.list === list;
    },
    afterFlight(fn) { if (typeof fn === 'function') deferred.add(fn); }
  };
  if (typeof module === 'object' && module.exports) module.exports = api;
  if (root && typeof root === 'object') root.SendFlight = api;
})(typeof window !== 'undefined' ? window : this);
