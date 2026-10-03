/* Charm Nest · Library flight (window.LibraryFx).
   Paul, 3 Oct 04:47 UTC: "a beautiful animation comes right after dropping a given sheet that shows it moving from its
   current location with all the parts and pieces on it until its new location".

   What is seen. The thing that flies is a lifted copy of the REAL card: its preview, its parts and pieces, its seals and
   stamps, its counters (a clone, never a placeholder; the seals stay on it, nothing is hidden or removed). It lifts off
   (a soft shadow grows, it rises a few percent), travels along one smooth arc to where the card now belongs (the target
   may be another section, another set card, a sheet's new place inside a set, or a tab), and sets down with a gentle
   settle and a pulse on the place it landed. The real card appears at its new place as the copy lands (the copy fades
   out over it: no duplicate, no flicker); the place it left closes by the neighbours gliding into the room.
   About 700 ms in all (duration: the last 180 ms of it are the settle), on the sheet window's easing, cubic-bezier(.3,0,.1,1).
   prefers-reduced-motion: no travel, a short cross-fade. A hard cap of 1.5 s, then it snaps to the end state.

   Calls (each returns a Promise that always resolves, never rejects, never throws; every error is swallowed):
     LibraryFx.fly(fromEl, toEl, { kind:'sheet'|'set', duration:700, onDone })
        fromEl  the card that moves. Either the lifted copy a drag carries (a position:fixed box holding a clone of the
                card: it is flown from where it is on screen, and removed at the end), or the card in its list (a copy
                is made of it, and the card is hidden while the copy is away).
        toEl    where it goes: the drop zone, a section (data-laser-area), a set card, or a tab. The zone may vanish at the
                drop: its last place is used, and the real card, found by the sheets it shows, is the true target as soon
                as the page has drawn it there (hidden until the copy lands). Until then the copy waits above its place.
        opts    kind, duration (the whole flight, ms), onDone(result), home (the card's own place in its list; found by its
                sheets when omitted), absorb (shrink into toEl and fade: for a tab), glide:false (the neighbours are not
                animated), target() (finds toEl again when the page draws it anew), point {x,y}.
     LibraryFx.flyBack(fromEl, toEl, opts)   a card that could not move goes home: fromEl is the copy resting at the
        target (or the place it rests), toEl the original card in the list, which is only landed on. Same options.
     LibraryFx.pulse(el)                     the soft ring a landing leaves on its target (callable alone).
     LibraryFx.active()                      flights under way (0 when nothing is left on the page).   LibraryFx.stop()  ends all.
   The result: { ok, how:'landed'|'absorbed'|'faded'|'none'|'cap'|'hidden'|'superseded'|'stopped'|'error', reason, ms }.
   onDone is called once, just before the copy's last fade (so the caller can clear what it left in the card's place).

   It never delays a write: it only draws. Whatever it hides (the card's old place, the card at its new place until the copy
   lands) is given back on every path: the end, the cap, a hidden page, a target that vanished, a newer flight of the same
   card. Built on the page's own pieces where they exist: Motion.ghost (a copy laid out as in its list), Motion.pulse (a
   tab's answer); without them it makes its own. */
(function (root) {
  "use strict";
  const doc = root.document;
  if (!doc || root.LibraryFx) return;

  const CAP = 1500;                         // hard cap: a flight is over by then, whatever happened
  const D = { travel: 700, settle: 180, fade: 160, reduced: 180, lift: 130, glide: 760 };   // travel: the whole flight, settling included
  const MAX_LIVE = 6, WAIT_UNTIL = CAP - D.settle - 140;
  const SHEET = ".librarySheet, .ldItem[data-kind=sheet]", SET = ".setCard, .ldItem[data-kind=set]";
  const GLIDE = ".laserSection, .laserAreaItems > .librarySheet, .laserAreaItems > .setCard, .sheetsRow > .librarySheet";
  const live = new Set();
  let seq = 0;

  const clock = () => { try { return root.performance.now(); } catch (_) { return Date.now(); } };
  const raf = fn => { try { return { r: root.requestAnimationFrame(fn) }; } catch (_) { return { t: setTimeout(() => fn(clock()), 16) }; } };
  const unraf = h => { try { if (h && h.r != null) root.cancelAnimationFrame(h.r); else if (h) clearTimeout(h.t); } catch (_) {} };
  const isEl = e => !!e && e.nodeType === 1;
  const clamp = (v, a, b) => Math.max(a, Math.min(b, v));
  const rectOf = e => { const r = e.getBoundingClientRect(); return { left: r.left, top: r.top, width: r.width, height: r.height, right: r.left + r.width, bottom: r.top + r.height }; };
  const centre = r => ({ x: r.left + r.width / 2, y: r.top + r.height / 2 });
  const sized = r => !!r && r.width > 1 && r.height > 1;
  const reducedNow = () => { try { return !!root.matchMedia("(prefers-reduced-motion: reduce)").matches; } catch (_) { return false; } };
  const hiddenPage = () => { try { return !!doc.hidden; } catch (_) { return false; } };
  const esc = s => String(s).replace(/["\\]/g, "\\$&");
  const animate = (el, frames, o) => { try { if (el && el.animate) return el.animate(frames, o); } catch (_) {} return null; };
  const isFixed = e => { try { return getComputedStyle(e).position === "fixed"; } catch (_) { return false; } };
  const small = r => r.width < 260 && r.height < 90;

  /** A CSS cubic-bezier as a function of time (0..1). */
  function bezier(x1, y1, x2, y2) {
    const cx = 3 * x1, bx = 3 * (x2 - x1) - cx, ax = 1 - cx - bx, cy = 3 * y1, by = 3 * (y2 - y1) - cy, ay = 1 - cy - by;
    const X = t => ((ax * t + bx) * t + cx) * t, Y = t => ((ay * t + by) * t + cy) * t, dX = t => (3 * ax * t + 2 * bx) * t + cx;
    return x => {
      if (x <= 0) return 0; if (x >= 1) return 1;
      let t = x;
      for (let i = 0; i < 8; i++) { const e = X(t) - x; if (Math.abs(e) < 1e-5) return Y(t); const d = dX(t); if (Math.abs(d) < 1e-6) break; t -= e / d; }
      let lo = 0, hi = 1; t = x;
      for (let i = 0; i < 24; i++) { const e = X(t); if (Math.abs(e - x) < 1e-5) break; if (e < x) lo = t; else hi = t; t = (lo + hi) / 2; }
      return Y(t);
    };
  }
  const ease = bezier(.3, 0, .1, 1), easeOut = bezier(.3, .1, .2, 1);

  function css() {
    if (doc.getElementById("fxCss") || !doc.head) return;
    const s = doc.createElement("style"); s.id = "fxCss";
    s.textContent = `.fxBox{position:fixed;margin:0;pointer-events:none;will-change:transform,opacity}
.fxBox>.fxShadow{position:absolute;left:0;top:0;pointer-events:none;opacity:0;box-shadow:0 26px 50px rgba(30,24,16,.27),0 8px 16px rgba(30,24,16,.13)}
.fxLayer{position:fixed;inset:0;z-index:140;pointer-events:none;overflow:hidden}
[data-fx-hide]{visibility:hidden!important}`;
    doc.head.appendChild(s);
  }

  /* ── what a card is: by the sheets it shows, so a card drawn anew is still the same card ── */
  const inLists = n => { const l = doc.getElementById("libBody") || doc.getElementById("libDone"); return !!n && n.isConnected && (l ? !!n.closest("#libBody, #libDone") : !n.closest(".fxBox, .mGhost, #motionLayer, .motionLayer")); };
  const IDS = ".libCard[data-id], .ldItem[data-id]";
  function sigOf(el, kind) {
    const ids = [...(el.matches && el.matches(IDS) ? [el.dataset.id] : []), ...[...el.querySelectorAll(IDS)].map(n => n.dataset.id)].filter(Boolean);
    const set = kind === "set" || (kind !== "sheet" && !!(el.matches && el.matches(SET) || el.querySelector(SET)));
    const row = el.matches && el.matches(".ldItem[data-kind=set]") ? el : null;
    return { kind: set ? "set" : "sheet", ids: [...new Set(ids)], setId: row ? row.dataset.set : null };
  }
  const keyOf = sig => sig.kind + ":" + (sig.setId || sig.ids.slice().sort().join("|"));
  /** The cards on the page that show these sheets (best match first). */
  function candidates(sig) {
    const out = new Map();
    for (const id of sig.ids) for (const n of doc.querySelectorAll(`.libCard[data-id="${esc(id)}"], .ldItem[data-id="${esc(id)}"]`)) {
      if (!inLists(n)) continue;
      const it = sig.kind === "set" ? n.closest(".setCard") : (n.closest(".librarySheet") || n);
      if (it && inLists(it)) out.set(it, (out.get(it) || 0) + 1);
    }
    if (sig.setId) for (const n of doc.querySelectorAll(`.ldItem[data-kind=set][data-set="${esc(sig.setId)}"]`)) if (inLists(n)) out.set(n, 99);
    return [...out.entries()].sort((a, b) => b[1] - a[1]).map(x => x[0]);
  }
  /** The card an element stands for: a sheet's card with its QR label, a whole set card. */
  function itemOf(el, kind) {
    if (!isEl(el)) return null;
    if (kind === "set") return el.closest(".setCard, .ldItem[data-kind=set]") || el;
    if (kind === "sheet") return el.closest(".librarySheet, .ldItem[data-kind=sheet]") || el.closest(".libCard") || el;
    return el.closest(".setCard, .librarySheet, .ldItem") || el.closest(".libCard") || el;
  }

  /* ── the copy ── */
  function layerOf() {
    let l = doc.getElementById("motionLayer");
    if (!l) { l = doc.createElement("div"); l.id = "motionLayer"; l.className = "fxLayer"; l.setAttribute("aria-hidden", "true"); doc.body.appendChild(l); }
    return l;
  }
  /** A copy of `item` where it stands (canvases carry their pixels over), laid out as in its list. */
  function copyOf(item, r) {
    let box = null;
    if (root.Motion && root.Motion.ghost) { try { box = root.Motion.ghost(item, r, null, item); } catch (_) { box = null; } }
    if (!box) {
      const g = item.cloneNode(true);
      g.removeAttribute("id"); for (const x of g.querySelectorAll("[id]")) x.removeAttribute("id");
      const a = item.querySelectorAll("canvas"), b = g.querySelectorAll("canvas");
      a.forEach((c, i) => { try { const d = b[i]; d.width = c.width; d.height = c.height; d.getContext("2d").drawImage(c, 0, 0); } catch (_) {} });
      Object.assign(g.style, { width: r.width + "px", height: r.height + "px", margin: "0", boxSizing: "border-box", animation: "none", transform: "none" });
      g.classList.add("mCopy");
      box = doc.createElement("div"); box.className = "mGhost";
      Object.assign(box.style, { left: r.left + "px", top: r.top + "px", width: r.width + "px", height: r.height + "px", transformOrigin: r.width / 2 + "px " + r.height / 2 + "px" });
      box.appendChild(g); box.inert = true; layerOf().appendChild(box);
      box._rect = r; box._card = g;
    }
    box.classList.add("fxBox");
    // (the card as it looked at rest: no spinner on its button, no search glow, nothing to hover or tab to)
    const card = box._card || box.firstElementChild;
    if (card) for (const n of [card, ...card.querySelectorAll(".busy, .ldHit, .ldIn")]) n.classList.remove("busy", "ldHit", "ldIn");
    for (const n of box.querySelectorAll("[data-fx-hide]")) n.removeAttribute("data-fx-hide");
    for (const n of box.querySelectorAll("[title]")) n.removeAttribute("title");
    for (const n of box.querySelectorAll("[tabindex]")) n.removeAttribute("tabindex");
    return box;
  }
  /** A box already on screen (the copy a drag carries) takes the flight over as it is. */
  function adopt(box) {
    const r = rectOf(box);
    box.classList.add("fxBox"); box.inert = true;
    box.style.transformOrigin = r.width / 2 + "px " + r.height / 2 + "px";
    return r;
  }
  function shadowOf(F) {
    const sh = doc.createElement("i"); sh.className = "fxShadow";
    const core = F.box._card || F.box.firstElementChild;
    let br = "12px"; try { const v = core && core.nodeType === 1 ? getComputedStyle(core).borderRadius : ""; if (v && v !== "0px") br = v; } catch (_) {}
    Object.assign(sh.style, { width: F.r0.width + "px", height: F.r0.height + "px", borderRadius: br });
    F.box.insertBefore(sh, F.box.firstChild);
    return sh;
  }

  /* ── hiding what is not there to see: the card's old place, the card at its new place until the copy lands ── */
  function hide(F, el) { try { if (!isEl(el) || el.hasAttribute("data-fx-hide")) return; el.setAttribute("data-fx-hide", String(F.id)); F.hidden.add(el); } catch (_) {} }
  function show(F, el, fade, ms) {
    try {
      if (!isEl(el) || el.getAttribute("data-fx-hide") !== String(F.id)) return;
      el.removeAttribute("data-fx-hide"); F.hidden.delete(el);
      if (fade) animate(el, [{ opacity: 0 }, { opacity: 1 }], reducedNow() ? { duration: D.reduced, easing: "ease" } : { duration: ms || D.settle + D.fade, easing: "cubic-bezier(.3,0,.1,1)" });
    } catch (_) {}
  }

  /* ── what stays glides into the room left (and out of the way of the room opened): transform only ── */
  function gkey(el) {
    if (el.classList.contains("laserSection")) return "area:" + (el.dataset.laserArea || "");
    if (el.classList.contains("setCard")) { const s = sigOf(el, "set"); return s.ids.length ? keyOf(s) : ""; }
    const c = el.querySelector(":scope > .libCard[data-id]"); return c ? "sheet:" + c.dataset.id : "";
  }
  function snapshot() {
    const body = doc.getElementById("libBody"), m = new Map(); if (!body) return m;
    let n = 0;
    for (const el of body.querySelectorAll(GLIDE)) {
      if (++n > 240) break;
      const k = gkey(el); if (!k || m.has(k)) continue;
      const r = rectOf(el); if (r.width || r.height) m.set(k, r);
    }
    return m;
  }
  function glideFrom(F) {
    try {
      const body = doc.getElementById("libBody"), before = F.before;
      if (!body || !before || !before.size || reducedNow() || F.opts.glide === false) return;
      const now = [];
      for (const el of body.querySelectorAll(GLIDE)) { const k = gkey(el); if (k && before.has(k) && !el.closest("[data-fx-hide]")) now.push([k, el]); }
      for (const [, el] of now) if (el._fxGlide) { el._fxGlide.forEach(a => { try { a.cancel(); } catch (_) {} }); el._fxGlide = null; }
      // (every place measured first, then every glide started: a card inside a glider is moved with it, so it only glides what the glider does not)
      const at = new Map(); for (const [, el] of now) at.set(el, rectOf(el));
      const moved = new Map();
      for (const [k, el] of now) {
        if (el._glide) continue;                                  // (the Library's own glide has it)
        const b = before.get(k), r = at.get(el); if (!sized(r)) continue;
        let p = el.parentElement; while (p && p !== body && !moved.has(p)) p = p.parentElement;
        const pd = p && p !== body ? moved.get(p) : { dx: 0, dy: 0 };
        const dx = b.left - r.left, dy = b.top - r.top, ox = dx - pd.dx, oy = dy - pd.dy;
        moved.set(el, { dx, dy });
        if (Math.abs(ox) < 1 && Math.abs(oy) < 1) continue;
        const a = animate(el, [{ transform: `translate(${ox}px,${oy}px)` }, { transform: "none" }], { duration: D.glide, easing: "cubic-bezier(.3,.1,.2,1)", fill: "backwards" });
        if (a) el._fxGlide = [a];
      }
      F.before = snapshot();
    } catch (_) {}
  }

  /* ── where it goes ── */
  function region() {
    let r = { left: 0, top: 0, right: root.innerWidth || 1024, bottom: root.innerHeight || 768 };
    try { const st = doc.getElementById("stage"); if (st) { const s = st.getBoundingClientRect(); if (s.width > 1 && s.height > 1) r = { left: Math.max(0, s.left), top: Math.max(0, s.top), right: Math.min(r.right, s.right), bottom: Math.min(r.bottom, s.bottom) }; } } catch (_) {}
    return r;
  }
  const AREAS = { progress: "pending", pending: "pending", laser: "ready", ready: "ready" };
  /** The list a card dropped on `to` joins: the section it is, holds or lies in; a set card's row of sheets. */
  function listOf(to) {
    try {
      let sec = to.closest && to.closest("[data-laser-area]") || (to.querySelector && to.querySelector("[data-laser-area]"));
      const d = to.dataset || {}, a = AREAS[String(d.area || d.zone || d.target || d.laserArea || "").toLowerCase()];
      if (!sec && a) sec = doc.querySelector(`#libBody [data-laser-area="${a}"]`);
      if (sec) return sec.querySelector(".laserAreaItems") || sec;
      const set = to.closest && to.closest(".setCard") || (to.matches && to.matches(".setCard") ? to : null);
      if (set) return set.querySelector(".sheetsRow") || set;
      if (to.matches && to.matches(".laserAreaItems, .sheetsRow")) return to;
    } catch (_) {}
    return null;
  }
  /** Where the next card of `list` will stand: after the last one on its row, or on a new row. */
  function slotIn(list, w, h) {
    const lr = rectOf(list), kids = [...list.children].filter(k => !k.hasAttribute("data-fx-hide") && sized(rectOf(k)));
    if (!kids.length) return { x: lr.left + w / 2, y: lr.top + h / 2 };
    const last = rectOf(kids[kids.length - 1]), gap = 14;
    let x = last.right + gap, y = last.top;
    if (x + w > lr.right + 1) { x = lr.left; y = last.bottom + gap; }
    return { x: x + w / 2, y: y + h / 2 };
  }
  function locatorOf(el) {
    if (!isEl(el)) return null;
    const d = el.dataset || {};
    const sel = d.laserArea ? `[data-laser-area="${esc(d.laserArea)}"]` : el.id ? `[id="${esc(el.id)}"]` : d.t ? `#libTab [data-t="${esc(d.t)}"]` : null;
    return sel ? () => doc.querySelector(sel) : null;
  }

  /** The target now: { x, y, w, h, how:'land'|'absorb'|'edge', real, el } in viewport coordinates, or null. */
  function targetOf(F) {
    const reg = F.reg = region();
    const inside = t => {                                      // (never under the top bar; a place scrolled out of sight is flown to its edge)
      const ox = Math.min(t.x + t.w / 2, reg.right) - Math.max(t.x - t.w / 2, reg.left), oy = Math.min(t.y + t.h / 2, reg.bottom) - Math.max(t.y - t.h / 2, reg.top);
      if (ox > 0 && oy > 0 && (ox * oy) / (t.w * t.h) >= (t.how === "absorb" ? .5 : .3)) return t;
      return { x: clamp(t.x, reg.left + 24, reg.right - 24), y: clamp(t.y, reg.top + 24, reg.bottom - 24), w: t.w, h: t.h, how: "edge", real: false, el: t.el };
    };
    const a = findArriving(F);
    if (a) { const r = rectOf(a); if (sized(r)) return inside(Object.assign({ w: r.width, h: r.height, how: "land", real: true, el: a }, centre(r))); }
    if (F.back) return null;
    let to = F.to;
    if ((!to || !to.isConnected) && F.relocate) { try { const n = F.relocate(); if (isEl(n) && n.isConnected) to = F.to = n; } catch (_) {} }
    let r = null;
    if (to && to.isConnected) { r = rectOf(to); if (sized(r)) F.lastTo = r; else r = null; }
    if (!r && F.lastTo) { r = F.lastTo; to = null; }
    if (!r) return null;
    const w = F.r0.width, h = F.r0.height;
    if (F.opts.absorb || small(r) || (to && to.matches && to.matches("button, [role=tab], a"))) return inside(Object.assign({ w: r.width, h: r.height, how: "absorb", real: false, el: to }, centre(r)));
    const list = to && listOf(to);
    if (list) return inside(Object.assign({ w, h, how: "land", real: false, el: to }, slotIn(list, w, h)));
    const k = Math.min(1, r.width / w, r.height / h);
    return inside(Object.assign({ w: w * k, h: h * k, how: "land", real: false, el: to }, centre(r)));
  }
  /** The real card at its new place, hidden the moment it is drawn, until the copy lands on it. (A card sent back is
      only landed on: it is the card in the list.) */
  function findArriving(F) {
    if (F.arriving && F.arriving.isConnected) return F.arriving;
    if (F.back) return (F.arriving = F.home && F.home.isConnected ? F.home : (candidates(F.sig)[0] || null));
    const stays = F.home && F.home.isConnected && F.home.parentNode === F.homeParent && !F.homeGone;
    const list = candidates(F.sig).filter(n => !(stays && n === F.home) && !(F.box && F.box.contains(n)));
    if (!list.length) return (F.arriving = null);
    const a = (F.to && F.to.isConnected ? list.find(n => F.to.contains(n)) : null) || list[0];
    F.arriving = a; hide(F, a);
    return a;
  }

  /* ── the flight ── */
  function launch(mode, fromEl, toEl, opts) {
    return new Promise(resolve => {
      const F = { id: ++seq, back: mode === "back", opts: opts || {}, hidden: new Set(), cleanups: [], t0: clock(), ended: false, resolve, state: "travel" };
      live.add(F);
      try { setup(F, fromEl, toEl); }
      catch (e) { finish(F, "error", e && e.message); }
    });
  }
  function setup(F, fromEl, toEl) {
    css();
    const o = F.opts, back = F.back;
    F.to = isEl(toEl) ? toEl : null;
    F.relocate = typeof o.target === "function" ? o.target : locatorOf(F.to);
    if (F.to && F.to.isConnected) { const r = rectOf(F.to); if (sized(r)) F.lastTo = r; }
    // the hard cap, and a hidden page, end it where it is (a hidden tab draws no frames)
    F.cap = setTimeout(() => finish(F, "cap"), CAP);
    const vis = () => { if (hiddenPage()) finish(F, "hidden"); };
    doc.addEventListener("visibilitychange", vis);
    if (root.addEventListener) root.addEventListener("pagehide", vis);
    F.cleanups.push(() => { doc.removeEventListener("visibilitychange", vis); if (root.removeEventListener) root.removeEventListener("pagehide", vis); });
    if (hiddenPage()) return finish(F, "hidden");

    // who it is: by what the carried copy (or the card) shows
    const carried = isEl(fromEl) && fromEl.isConnected && isFixed(fromEl) && sized(rectOf(fromEl)) ? fromEl : null;
    const item = !carried && isEl(fromEl) ? itemOf(fromEl, o.kind) : null;
    const homeEl = back && isEl(toEl) ? itemOf(toEl, o.kind) : null;
    const looks = carried || item || homeEl;
    F.sig = looks ? sigOf(looks, o.kind) : { kind: o.kind === "set" ? "set" : "sheet", ids: [], setId: null };
    if (!F.sig.ids.length && !F.sig.setId && homeEl) F.sig = sigOf(homeEl, o.kind);
    if (!looks) return noSource(F);
    F.key = keyOf(F.sig);
    // a newer flight of the same card ends the older one where it is; too many at once end the oldest
    if (F.sig.ids.length || F.sig.setId) for (const x of [...live]) if (x !== F && x.key === F.key) finish(x, "superseded");
    while (live.size > MAX_LIVE) { const old = live.values().next().value; if (!old || old === F) break; finish(old, "crowded"); }

    // the home: the card's own place in its list
    let home = null;
    if (back) home = homeEl && homeEl.isConnected ? homeEl : (candidates(F.sig)[0] || null);
    else if (isEl(o.home) && o.home.isConnected) home = itemOf(o.home, o.kind);
    else if (item) home = item;
    else if (carried) home = candidates(F.sig).find(n => !(F.to && F.to.isConnected && F.to.contains(n))) || null;
    F.home = home; F.homeParent = home ? home.parentNode : null;

    // the copy, where it is now (F.cb: where its box stands; F.c0: where the flight sets off from; F.k0: its size then)
    let nat = null;
    if (carried) { F.box = carried; nat = adopt(carried); F.adopted = true; F.cb = F.c0 = centre(nat); F.k0 = 1; }
    else if (back) {
      const from = isEl(fromEl) && fromEl.isConnected ? rectOf(fromEl) : null, hr = home && home.isConnected ? rectOf(home) : null;
      if (!sized(from) || !sized(hr)) return noSource(F);
      F.box = copyOf(home, hr); nat = F.box._rect || hr; F.cb = centre(nat);
      F.k0 = clamp(Math.min(from.width / nat.width, from.height / nat.height), .3, .6);   // (it sets off small, from the middle of where it was)
      F.c0 = o.point && isFinite(o.point.x) && isFinite(o.point.y) ? { x: +o.point.x, y: +o.point.y } : centre(from);
    } else {
      const rr = rectOf(item), reg = region();
      // (a card scrolled out of sight is not flown from the edge of the page: the card at its new place only shows, with a pulse)
      const vx = Math.min(rr.right, reg.right) - Math.max(rr.left, reg.left), vy = Math.min(rr.bottom, reg.bottom) - Math.max(rr.top, reg.top);
      if (!sized(rr) || vx <= 0 || vy <= 0 || (vx * vy) / (rr.width * rr.height) < .12) return noSource(F);
      F.box = copyOf(item, rr); nat = F.box._rect || rr; F.cb = F.c0 = centre(nat); F.k0 = 1;
    }
    F.r0 = nat;
    F.sh = shadowOf(F);
    F.dur = clamp(+o.duration || D.travel, 450, 1100);
    F.travel = F.dur - D.settle;                               // (the settle is the end of the duration)
    F.ts = clamp(520 / Math.max(nat.width, 1.4 * nat.height), .45, .92);   // (how small it travels: a wide set card gathers itself in the air)
    F.reduced = reducedNow();
    F.before = back || o.glide === false ? null : snapshot();
    // a carried copy may lean or be lifted in its own way; it comes level as it flies
    if (carried) {
      const core = carried.firstElementChild;
      if (core && core !== F.sh && core.nodeType === 1) { let t = "none"; try { t = getComputedStyle(core).transform; } catch (_) {} if (t && t !== "none") animate(core, [{ transform: t }, { transform: "none" }], { duration: Math.round(F.dur * .55), easing: "cubic-bezier(.3,0,.1,1)", fill: "forwards" }); }
    }
    // the card's old place is empty while its copy is away (a sent-back card is not hidden: its copy lands on it)
    if (!back && home) hide(F, home);
    findArriving(F);                                           // (the page may have drawn the card at its new place already)
    watch(F);
    F.frame = raf(() => tick(F));
  }
  /** Nothing to fly (no card in sight): the card, if it is at its new place, shows with a short fade and a pulse. */
  function noSource(F) {
    const a = F.sig.ids.length || F.sig.setId ? findArriving(F) : null;
    if (a) { show(F, a, true); pulse(a); }
    else if (F.to && F.to.isConnected) pulse(F.to);
    F.cap2 = setTimeout(() => finish(F, "none"), reducedNow() ? D.reduced : D.settle + D.fade);
  }
  /** Watches the page draw: the card at its new place is hidden before it can be painted, the card's old place gives
      way, and what stays glides. */
  function watch(F) {
    if (typeof MutationObserver === "undefined") return;
    const roots = ["libBody", "libDone"].map(id => doc.getElementById(id)).filter(Boolean);
    if (!roots.length && doc.body) roots.push(doc.body);
    const obs = new MutationObserver(() => {
      try {
        if (F.ended) return;
        let layout = false;
        if (F.home && !F.homeGone && (!F.home.isConnected || F.home.parentNode !== F.homeParent)) { F.homeGone = true; layout = true; if (F.home.isConnected) hide(F, F.home); else F.hidden.delete(F.home); }
        if (!F.back && !(F.arriving && F.arriving.isConnected)) { F.arriving = null; if (findArriving(F)) layout = true; }
        if (layout) glideFrom(F);
        if (F.tail && F.homeGone) finish(F, "absorbed");
      } catch (_) {}
    });
    for (const r of roots) { try { obs.observe(r, { childList: true, subtree: true }); } catch (_) {} }
    F.cleanups.push(() => obs.disconnect());
  }

  /** One frame. */
  function tick(F) {
    F.frame = null;
    if (F.ended) return;
    try {
      if (hiddenPage()) return finish(F, "hidden");
      const now = clock(), el = now - F.t0;
      if (!F.box || !F.box.isConnected) return finish(F, "cap", "the copy was taken away");
      const T = targetOf(F);
      if (F.state !== "out") {
        if (F.reduced) reducedStep(F, T, el);
        else if (!T && !F.last) lost(F);
        else step(F, T || F.last, el, now);
      }
    } catch (e) { return finish(F, "error", e && e.message); }
    if (!F.ended && F.state !== "out") F.frame = raf(() => tick(F));
  }
  /** (reduced motion) The copy stays where it is and fades out as the card fades in at its place. */
  function reducedStep(F, T, el) {
    const real = findArriving(F);
    if (!real && el < WAIT_UNTIL && !F.opts.absorb && !(T && T.how === "absorb")) return;   // (it waits for the page to draw the card)
    F.state = "out";
    if (real) show(F, real, true);
    callDone(F, "faded");
    fadeOut(F, D.reduced, () => finish(F, "faded"));
  }
  /** The target is gone for good: the copy fades where it is and the card is given back its place. */
  function lost(F) { F.state = "out"; callDone(F, "faded"); fadeOut(F, D.fade + 40, () => finish(F, "faded", "no target")); }
  function fadeOut(F, ms, then) {
    const a = animate(F.box, [{ opacity: 1 }, { opacity: 0 }], { duration: ms, easing: "ease-out", fill: "forwards" });
    if (a) a.finished.then(then, then); else then();
  }
  function step(F, T, el, now) {
    F.last = T;
    const x0 = F.c0.x, y0 = F.c0.y, w0 = F.r0.width, h0 = F.r0.height, k0 = F.k0;
    // a target that jumps (the page drew the card at its place, a list closed up) is eased to, not jumped to
    const adj = F.adj || (F.adj = { x: 0, y: 0, t: now, px: T.x, py: T.y });
    const dt = Math.max(0, now - adj.t); adj.t = now;
    const jx = T.x - adj.px, jy = T.y - adj.py;
    if (Math.hypot(jx, jy) > 44) { adj.x -= jx; adj.y -= jy; }
    adj.px = T.x; adj.py = T.y;
    const decay = Math.exp(-dt / 110); adj.x *= decay; adj.y *= decay;
    const x1 = T.x + adj.x, y1 = T.y + adj.y, settling = F.state === "settle";
    const span = T.how === "land" ? F.travel : F.dur;
    let p = settling ? 1 : clamp(el / span, 0, 1);
    const q = ease(p);
    // the way: one arc, bulging up (or toward the middle of the page when the move is level or straight down)
    const dx = x1 - x0, dy = y1 - y0, d = Math.hypot(dx, dy) || 1, mx = (x0 + x1) / 2, my = (y0 + y1) / 2;
    let nx = -dy / d, ny = dx / d; if (ny > 0) { nx = -nx; ny = -ny; }
    if (Math.abs(ny) < .4) { nx = mx < (root.innerWidth || 1000) / 2 ? 1 : -1; ny = -.3; const l = Math.hypot(nx, ny); nx /= l; ny /= l; }
    const dev = Math.min(120, 18 + d * .14), cx = mx + nx * dev * 2, cy = my + ny * dev * 2, u = 1 - q;
    let x = u * u * x0 + 2 * u * q * cx + q * q * x1, y = u * u * y0 + 2 * u * q * cy + q * q * y1;
    const sEnd = T.how === "absorb" ? clamp(Math.min((T.h * 1.4) / h0, (T.w * 1.2) / w0), .03, .4) : T.how === "edge" ? .5 : clamp(T.w / w0, .5, 1.6);
    const base = (k0 + (sEnd - k0) * q) * (1 - (1 - F.ts) * Math.sin(Math.PI * q));
    const lift = easeOut(clamp(el / D.lift, 0, 1));
    const rise = Math.min(.03, 10 / (Math.max(w0, h0) / 2));   // (lifted by a few percent, never more than ~10 px at a corner of a wide card)
    let hover = 1 + rise * lift, shadow = lift * (.7 + .3 * Math.sin(Math.PI * q)), op = 1;
    let rot = (dx >= 0 ? 1 : -1) * 1.5 * Math.sin(Math.PI * q) * Math.min(1, d / 180);
    if (!settling && T.how !== "land") op = p < .84 ? 1 : Math.max(0, 1 - (p - .84) / .16);
    const reg = F.reg || region();
    y = Math.max(y, reg.top - 20 + (h0 * base) / 2);           // (its top edge never rises above the bar that holds the list)
    if (settling) {
      const s = clamp((now - F.tT) / D.settle, 0, 1), o = easeOut(s);
      hover = 1 + rise * (1 - o) - rise * .2 * Math.sin(Math.PI * s); shadow = .65 * (1 - o); rot = 0; x = x1; y = y1;
      // (set down; for the last of it the real card comes in under the copy and the copy fades out over it; the caller clears what it left in the card's place just before)
      if (s >= .3) {
        callDone(F, "landed"); op = Math.max(0, 1 - (s - .3) / .7);
        if (!F.revealed) { F.revealed = true; if (T.real && T.el) show(F, T.el, true, Math.round(D.settle * .7)); }
      }
      if (s >= 1) { F.state = "out"; return finish(F, T && T.real ? "landed" : "faded"); }
    } else if (el >= span) {
      // down: at once on a tab (it is absorbed); on a card as soon as the card is there, or the wait for the page is over
      if (T.how === "absorb" || T.how === "edge") { put(F, x1, y1, sEnd, 0, 0, 0); return arrived(F, T); }
      if (T.real || el >= WAIT_UNTIL) touchdown(F, T, now);
      else { hover = 1 + rise; shadow = .7; }                  // (waiting above its place for the page to draw the card)
    }
    put(F, x, y, base * hover, rot, op, shadow);
  }
  function put(F, x, y, s, rot, op, sh) {
    const b = F.box; if (!b) return;
    b.style.transform = `translate(${(x - F.cb.x).toFixed(2)}px,${(y - F.cb.y).toFixed(2)}px) rotate(${rot.toFixed(3)}deg) scale(${s.toFixed(4)})`;
    b.style.opacity = String(op);
    if (F.sh) F.sh.style.opacity = String(clamp(sh, 0, 1));
  }
  function touchdown(F, T, now) {
    F.state = "settle"; F.tT = now;
    if (T.el && T.el.isConnected) pulse(T.el, F.back ? { tone: "back" } : null);
  }
  /** Into a tab: the copy has shrunk into it and faded, the tab answers. The card's old place stays hidden until the
      page has taken it away (or the cap). */
  function arrived(F, T) {
    F.state = "out";
    if (T.el && T.el.isConnected) pulse(T.el);
    callDone(F, "absorbed");
    try { if (F.box) F.box.remove(); } catch (_) {}
    try { F.resolve({ ok: true, how: "absorbed", reason: "", ms: Math.round(clock() - F.t0) }); } catch (_) {}
    if (F.home && !F.homeGone && F.home.isConnected && F.home.parentNode === F.homeParent) F.tail = true; else finish(F, "absorbed");
  }
  function callDone(F, how) {
    if (F.doneCalled) return; F.doneCalled = true;
    try { if (typeof F.opts.onDone === "function") F.opts.onDone({ ok: how !== "error", how, ms: Math.round(clock() - F.t0) }); } catch (_) {}
  }

  /** The end, on every path: the copy leaves, what was hidden is given back, nothing stays listening. */
  function finish(F, how, why) {
    if (F.ended) return; F.ended = true;
    try { unraf(F.frame); clearTimeout(F.cap); clearTimeout(F.cap2); } catch (_) {}
    for (const c of F.cleanups.splice(0)) { try { c(); } catch (_) {} }
    try { if (F.box) { for (const a of (F.box.getAnimations ? F.box.getAnimations({ subtree: true }) : [])) { try { a.cancel(); } catch (_) {} } F.box.remove(); } } catch (_) {}
    // the card at its new place shows now; the card's old place is given back if the card never left it (a move that did not happen)
    for (const el of [...F.hidden]) show(F, el, el.isConnected, D.settle + D.fade);
    F.hidden.clear();
    live.delete(F);
    callDone(F, how);
    try { F.resolve({ ok: how !== "error", how, reason: why || "", ms: Math.round(clock() - F.t0) }); } catch (_) {}
    F.box = F.sh = F.home = F.homeParent = F.arriving = F.to = F.last = F.relocate = F.before = null;
  }

  /* ── the pulse a landing leaves ── */
  function pulse(el, o) {
    try {
      if (!isEl(el) || !el.isConnected || reducedNow()) return false;
      const r = rectOf(el); if (!sized(r)) return false;
      if (small(r) && root.Motion && root.Motion.pulse) { root.Motion.pulse(el); return true; }
      const ring = o && o.tone === "back" ? "60,52,40" : "169,130,63";
      let base = ""; try { base = getComputedStyle(el).boxShadow; } catch (_) {}
      const b = base && base !== "none" ? base + "," : "", grow = r.width < 520;
      const frames = [
        Object.assign({ boxShadow: `${b}0 0 0 0 rgba(${ring},.5)` }, grow ? { transform: "scale(1)" } : {}),
        Object.assign({ boxShadow: `${b}0 0 0 5px rgba(${ring},.3)`, offset: .35 }, grow ? { transform: "scale(1.012)" } : {}),
        Object.assign({ boxShadow: `${b}0 0 0 13px rgba(${ring},0)` }, grow ? { transform: "scale(1)" } : {})
      ];
      return !!animate(el, frames, { duration: 900, easing: "cubic-bezier(.3,0,.1,1)" });
    } catch (_) { return false; }
  }

  const safe = (mode, a, b, o) => {
    try { return launch(mode, a, b, o); }
    catch (e) { try { if (o && typeof o.onDone === "function") o.onDone({ ok: false, how: "error" }); } catch (_) {} return Promise.resolve({ ok: false, how: "error", reason: String(e && e.message || e) }); }
  };
  root.LibraryFx = {
    fly: (from, to, o) => safe("fly", from, to, o),
    flyBack: (from, to, o) => safe("back", from, to, o),
    pulse,
    active: () => live.size,
    stop: () => { for (const F of [...live]) finish(F, "stopped"); },
    caps: { cap: CAP, travel: D.travel, settle: D.settle }
  };
})(typeof window !== "undefined" ? window : globalThis);
