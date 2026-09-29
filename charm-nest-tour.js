/* Charm Nest · the Send to Sheet tour (Paul, 29 Sep 00:25).
   "It should show me a beautiful animation as that order is animating away from the Review tab into the nest tab and
   then into the appropriate sheet inside the Nest tab … one animation at a time … and then fully coming back to where
   the user started." The placement is done first (CustomSheet.send); this only shows what happened:
     1 · lift     the card lifts out of its place in the Review list, and its designs rise out of it as a coin;
     2 · to Nest  the coin arcs to the Nest tab, which lights, and the view crossfades there under it (a real tab switch);
     3 · sheets   for each sheet holding pieces of the order, one at a time: the sheet is opened (NestFocus, part B, or
                  here its card brought into view and spotlit), each piece flies from the coin onto its own spot at its
                  own turn and size, lands with the ring and pop, "+N" rises, the sheet eases back;
     4 · waiting  a metal with nothing on a sheet yet: the coin settles where that metal's pieces wait, and says why;
     5 · home     back to the Review tab as it was left (its sub-tab, scroll and filters), where the card went pulses.
   Only transform, opacity and clip-path are animated; pieces are drawn before they fly. A click, a key or Esc skips
   to the end (everything lands at once) and goes home; a tab picked during the tour ends it there. */
(function (root) {
  "use strict";
  const doc = root.document;
  const GROW = "cubic-bezier(.3,0,.1,1)", SOFT = "cubic-bezier(.4,0,.2,1)", RING = "#b8893a";
  const reduced = () => { try { return root.matchMedia("(prefers-reduced-motion: reduce)").matches; } catch (_) { return false; } };
  const $ = (s, n) => (n || doc).querySelector(s);
  const tryDo = f => { try { return f(); } catch (_) { return null; } };
  // where the tour is, for tests and a profile (tests/charm-nest/adv-custom-send-flight.cjs): performance.mark "tour:…"
  const mark = n => { try { performance.mark("tour:" + n); } catch (_) {} };
  const CSS = `
#tourLayer{position:fixed;inset:0;z-index:150;pointer-events:none;overflow:hidden;contain:strict}
#tourLayer>*{position:fixed;left:0;top:0;will-change:transform,opacity}
#tourTop{position:fixed;inset:0;width:100vw;height:100vh;max-width:none;max-height:none;margin:0;border:0;padding:0;background:transparent;box-shadow:none;overflow:hidden;pointer-events:none;outline:none}
#tourTop::backdrop{background:transparent}
/* the flying design is never under a caption of its own tour (the captions sit over the card and the sheets) */
#tourLayer>.tourCoin,#tourLayer>.tourPiece{z-index:3}
#tourLayer>.tourCap,#tourLayer>.tourPlus{z-index:2}
.tourCard{border-radius:14px}
.tourLift{position:absolute!important;inset:0;border-radius:14px;box-shadow:0 22px 48px rgba(30,24,16,.24),0 0 0 1.5px rgba(202,168,97,.55);opacity:0}
.tourCoin{width:72px;height:72px;margin:-36px 0 0 -36px}
.tourCoin>.tourFace{left:0;top:0;width:100%;height:100%}
.tourFace{position:absolute;display:block;border-radius:50%;background:radial-gradient(circle at 50% 50%,#fff 0,#fff 64%,#fbf6ea 80%,#efe2c2 100%);border:1.5px solid #caa861;box-shadow:0 14px 30px rgba(60,40,90,.24),0 0 0 5px rgba(125,86,168,.10),inset 0 -3px 8px rgba(169,130,63,.16)}
.tourFace>img,.tourFace>canvas{position:absolute;left:17%;top:17%;width:66%;height:66%;object-fit:contain}
.tourPiece>img,.tourPiece>canvas{display:block}
.tourCoin>b{position:absolute;right:-5px;top:-5px;font:800 10.5px var(--sans,system-ui);background:#7d56a8;color:#fff;border-radius:999px;padding:2px 7px;box-shadow:0 3px 8px rgba(60,40,90,.3)}
.tourCap{font:600 15px var(--serif,Georgia,serif);letter-spacing:.01em;color:var(--ink,#221f1b);background:rgba(255,254,250,.96);border:1px solid var(--goldLine,#e3d3a6);border-radius:999px;padding:6px 15px 6px 12px;box-shadow:0 8px 22px rgba(30,24,16,.14);white-space:nowrap;display:flex;align-items:center;gap:8px}
.tourCap>i{width:9px;height:9px;border-radius:50%;background:#caa861;box-shadow:0 0 0 2px rgba(255,255,255,.9)}
.tourCap>small{font:600 11px var(--sans,system-ui);color:var(--ink70,#6b645a);max-width:440px;overflow:hidden;text-overflow:ellipsis}
.tourPiece{transform-origin:50% 50%;filter:drop-shadow(0 6px 10px rgba(60,40,90,.28))}
.tourRing{border-radius:50%;border:2px solid ${RING};box-shadow:0 0 14px rgba(184,137,58,.5)}
.tourSpark{width:8px;height:8px;margin:-4px 0 0 -4px;border-radius:50%;background:radial-gradient(circle,#fffaf0 0,#e9cf8e 45%,rgba(202,168,97,0) 72%)}
.tourHole{border-radius:16px;box-shadow:0 0 0 200vmax rgba(34,31,27,.30),0 0 0 2px rgba(202,168,97,.8),0 24px 60px rgba(30,24,16,.3)}
.tourPlus{font:800 13px var(--sans,system-ui);color:#3f5b3a;background:var(--card,#fff);border:1px solid #cddcc9;border-radius:999px;padding:2px 11px;box-shadow:0 6px 16px rgba(30,24,16,.16);white-space:nowrap}
.tourSkip{font:600 11px var(--sans,system-ui);color:rgba(255,255,255,.92);background:rgba(34,31,27,.62);border-radius:999px;padding:5px 12px;white-space:nowrap}`;
  function sheet() { if ($("#tourCss")) return; const s = doc.createElement("style"); s.id = "tourCss"; s.textContent = CSS; doc.head.appendChild(s); }
  function layer() { let l = $("#tourLayer"); if (!l) { l = doc.createElement("div"); l.id = "tourLayer"; l.setAttribute("aria-hidden", "true"); doc.body.appendChild(l); } return l; }

  /* ── easing, sampled: a path is a curve with the app's GROW timing along it, as one animation of transform alone ── */
  function bezier(x1, y1, x2, y2) {
    const cx = 3 * x1, bx = 3 * (x2 - x1) - cx, ax = 1 - cx - bx, cy = 3 * y1, by = 3 * (y2 - y1) - cy, ay = 1 - cy - by;
    const X = u => ((ax * u + bx) * u + cx) * u, Y = u => ((ay * u + by) * u + cy) * u, dX = u => (3 * ax * u + 2 * bx) * u + cx;
    return t => { if (t <= 0) return 0; if (t >= 1) return 1; let u = t; for (let i = 0; i < 8; i++) { const d = dX(u); if (Math.abs(d) < 1e-6) break; u -= (X(u) - t) / d; } u = Math.max(0, Math.min(1, u)); return Y(u); };
  }
  const EASE = { grow: bezier(.3, 0, .1, 1), arc: bezier(.45, .05, .25, 1), soft: bezier(.4, 0, .2, 1) };
  const tf = p => `translate(${p.x.toFixed(1)}px,${p.y.toFixed(1)}px) rotate(${(p.r || 0).toFixed(2)}deg) scale(${Math.max(.001, p.s).toFixed(4)})`;
  /** Keyframes along a quadratic arc from a to b (bend: how far the middle rises, px), eased by `ease`; with the
   *  opacity and scale given at each end, and an optional overshoot pop at the end (landing). */
  function path(a, b, { bend = 0, ease = EASE.arc, n = 18, oa = 1, ob = 1, pop = 0 } = {}) {
    const mx = (a.x + b.x) / 2, my = (a.y + b.y) / 2 - bend, out = [];
    for (let i = 0; i <= n; i++) {
      const u = i / n, t = ease(u), q = 1 - t;
      const x = q * q * a.x + 2 * q * t * mx + t * t * b.x, y = q * q * a.y + 2 * q * t * my + t * t * b.y;
      let s = a.s + (b.s - a.s) * t; if (pop && t > .82) s *= 1 + pop * Math.sin(Math.PI * (t - .82) / .18);
      out.push({ offset: u, transform: tf({ x, y, r: (a.r || 0) + ((b.r || 0) - (a.r || 0)) * t, s }), opacity: oa + (ob - oa) * Math.min(1, t * 1.4) });
    }
    return out;
  }
  const at = (x, y, s = 1, r = 0) => ({ x, y, s, r });
  const mid = r => at(r.left + r.width / 2, r.top + r.height / 2);
  const rectOf = e => { const r = e && e.isConnected ? e.getBoundingClientRect() : null; return r && r.width > 0 && r.height > 0 ? r : null; };
  const onScreen = r => !!r && r.bottom > 0 && r.top < innerHeight && r.right > 0 && r.left < innerWidth;

  /* ── one tour at a time ── */
  let T = null;
  function make() {
    const t = { ff: false, userTab: false, anims: new Set(), wakes: new Set(), nodes: new Set(), off: [] };
    t.wait = ms => t.ff ? Promise.resolve() : new Promise(res => { const w = () => { clearTimeout(h); t.wakes.delete(w); res(); }, h = setTimeout(w, ms); t.wakes.add(w); });
    t.anim = (node, frames, opts) => {
      if (!node || !node.isConnected) return Promise.resolve();
      const a = node.animate(frames, Object.assign({ fill: "forwards" }, opts));
      if (t.ff) a.finish(); else t.anims.add(a);
      return a.finished.catch(() => {}).then(() => { t.anims.delete(a); });
    };
    t.node = (cls, html) => { const n = doc.createElement("div"); n.className = cls; if (html) n.innerHTML = html; layer().appendChild(n); t.nodes.add(n); return n; };
    // skipped: the sheet standing open (NestFocus) shows everything at once and eases back; no other sheet is opened
    t.forward = () => { if (t.ff) return; t.ff = true; for (const a of [...t.anims]) tryDo(() => a.finish()); for (const w of [...t.wakes]) w(); tryDo(() => root.NestFocus && root.NestFocus.isOpen() && root.NestFocus.close()); };
    // bounded: a promise from outside (NestFocus) that never settles never holds the tour, nor one still pending on a skip
    t.within = (p, ms) => Promise.race([Promise.resolve(p).catch(() => null), new Promise(res => { if (t.ff) { setTimeout(() => res(null), Math.min(ms, 600)); return; } const w = () => { clearTimeout(h); t.wakes.delete(w); res(null); }, h = setTimeout(w, ms); t.wakes.add(w); })]);
    return t;
  }
  /** A click, a key or Esc anywhere: the rest lands at once and the tour goes home. A tab picked: the tour ends there. */
  function listen(t, home) {
    const tabOf = e => e.target && e.target.closest && e.target.closest(".topbar [data-mode], #moreMenu [data-mode]");
    let swallow = 0;
    const down = e => {
      if (tabOf(e)) { t.userTab = true; t.forward(); return; }
      if (t.ff) return;
      // a press on the view it goes home to, while that view is still in sight, is the person's own: it goes through
      // (not while a window waits to come back over it: it would open a second one)
      const v = viewOf(home.mode);
      if (!t.held && modeNow() === home.mode && v && e.target && v.contains(e.target)) { t.forward(); return; }
      swallow = Date.now() + 700; e.preventDefault(); e.stopPropagation(); t.forward();
    };
    const click = e => { if (Date.now() < swallow && !tabOf(e)) { e.preventDefault(); e.stopPropagation(); swallow = 0; } };
    const key = e => { if (e.key === "Escape") { e.preventDefault(); e.stopPropagation(); } t.forward(); };
    const hash = () => { t.userTab = true; t.forward(); };
    addEventListener("pointerdown", down, true); addEventListener("click", click, true); addEventListener("keydown", key, true); addEventListener("popstate", hash);
    t.off.push(() => { removeEventListener("pointerdown", down, true); removeEventListener("keydown", key, true); removeEventListener("popstate", hash); setTimeout(() => removeEventListener("click", click, true), 750); });
  }

  /* ── the app's pieces ── */
  const CN = () => root.CN || null;
  const modeNow = () => tryDo(() => CN().S.mode) || "";
  const setMode = m => tryDo(() => CN().setMode(m));
  const viewOf = m => m === "nest" ? $("#sheets") : $("#" + ({ review: "reviewView", orders: "ordersView", engrave: "engraveView", library: "libView" }[m] || m + "View"));
  const nestTab = () => { const b = $('#modeSeg [data-mode="nest"]'); return b && b.getClientRects().length ? b : null; };
  const scroller = () => $("#reviewView .egPane.scroll");
  const nestCard = m => $(`.sheetCard[data-m="${m}"]`);
  const charmOf = id => tryDo(() => root.Pool && root.Pool.charmOf(id));
  /** A piece drawn once, before it flies: its outline and lines in the custom plum, at the size it lands at. */
  function pieceCanvas(c, pxPerPt) {
    const P = root.CharmNestPDF; if (!c || !P || !c.outline || !c.widthPt || !c.heightPt) return null;
    const dpr = Math.min(2, root.devicePixelRatio || 1), q = pxPerPt * dpr, w = Math.max(2, Math.ceil(c.widthPt * q)), h = Math.max(2, Math.ceil(c.heightPt * q));
    if (w * h > 1600 * 1600) return null;
    const cv = doc.createElement("canvas"); cv.width = w; cv.height = h; cv.style.width = c.widthPt * pxPerPt + "px"; cv.style.height = c.heightPt * pxPerPt + "px";
    const ctx = cv.getContext("2d"), cx = c.centerPt[0], cy = c.centerPt[1], tx = (x, y) => [(x - cx) * q, (cy - y) * q];
    ctx.translate(w / 2, h / 2);
    try { ctx.fillStyle = "rgba(125,86,168,.24)"; ctx.beginPath(); P.pathToCanvas(ctx, c.outline, tx); ctx.fill("evenodd"); P.drawCharm(ctx, c, tx, q); } catch (_) { return null; }
    return cv;
  }

  /** The sheet opened here when part B's NestFocus is not on the page: its card brought into view and spotlit, a caption,
   *  and each piece's spot read from the sheet as drawn (its active page only; otherwise the middle of the sheet). */
  async function openHere(t, leg, caption) {
    const card = nestCard(leg.metal); if (!card) return null;
    const r0 = card.getBoundingClientRect();
    if (!onScreen(r0) || r0.top < 60 || r0.bottom > innerHeight - 20) { card.scrollIntoView({ block: "center", behavior: t.ff ? "auto" : "smooth" }); await t.wait(460); }
    const r = card.getBoundingClientRect(), hole = t.node("tourHole");
    Object.assign(hole.style, { width: r.width + 12 + "px", height: r.height + 12 + "px", transform: `translate(${r.left - 6}px,${r.top - 6}px)`, opacity: 0 });
    t.anim(hole, [{ opacity: 0 }, { opacity: 1 }], { duration: 380, easing: SOFT });
    const cap = capOf(t, caption, leg.metal), cw = cap.offsetWidth;
    placeCap(t, cap, r.left + r.width / 2 - cw / 2, Math.max(8, r.top - 44));
    const cv = $('[data-r="canvas"]', card), sh = tryDo(() => allSheetsOf().find(p => (leg.sheetId && p.sheetId === leg.sheetId) || (p.metal === leg.metal && (p.page || 1) === (leg.page || 1)))), shown = sh && tryDo(() => (typeof activePage === "function" ? activePage(sh.metal) === sh : true)); // eslint-disable-line no-undef
    const spots = [];
    const cr = rectOf(cv);
    for (const id of leg.poolIds || []) {
      let spot = null;
      if (sh && cr && sh._view && shown !== false) {
        const c = (sh.charms || []).find(x => x.poolId === id), p = c && (sh.placements || []).find(x => x.id === c.id);
        if (p) { const sc = cr.width / cv.width, k = sh._view.k * sc, x = cr.left + (sh._view.R + p.cxPt * sh._view.k) * sc, y = cr.top + (sh._view.R + p.cyPt * sh._view.k) * sc, s = k * (p.scale || 1);
          spot = { poolId: id, rect: { left: x - c.widthPt * s / 2, top: y - c.heightPt * s / 2, width: c.widthPt * s, height: c.heightPt * s }, rot: p.angle || 0, scale: s }; }
      }
      spots.push(spot || { poolId: id, rect: null, rot: 0, scale: 0 });
    }
    return {
      card, spots, here: true,
      land: id => { const s = spots.find(x => x.poolId === id); ring(t, s && s.rect ? s.rect : rectOf(cv) || r); return Promise.resolve(); },
      close: () => t.anim(hole, [{ opacity: 1 }, { opacity: 0 }], { duration: 360, easing: SOFT }).then(() => hole.remove())
    };
  }
  const allSheetsOf = () => tryDo(() => (typeof allSheets === "function" ? allSheets() : [])) || []; // eslint-disable-line no-undef
  function capOf(t, text, metal) {
    const cap = t.node("tourCap"), dot = doc.createElement("i"), col = tryDo(() => CN().METALS.find(m => m.key === metal).color);
    if (col) dot.style.background = col;
    const [a, b] = String(text || "").split(/ — /); cap.appendChild(dot); cap.appendChild(doc.createTextNode(a)); if (b) { const s = doc.createElement("small"); s.textContent = b; cap.appendChild(s); }
    cap.style.opacity = 0; return cap;
  }
  function placeCap(t, cap, x, y) {
    x = Math.max(8, Math.min(innerWidth - cap.offsetWidth - 8, x));
    cap.style.transform = `translate(${x}px,${y}px)`;
    return t.anim(cap, [{ opacity: 0, transform: `translate(${x}px,${y + 8}px) scale(.96)` }, { opacity: 1, transform: `translate(${x}px,${y}px)` }], { duration: 420, easing: GROW });
  }
  const fadeOut = (t, n, ms = 260) => n && n.isConnected ? t.anim(n, [{ opacity: getComputedStyle(n).opacity }, { opacity: 0 }], { duration: ms, easing: "ease" }).then(() => { n.remove(); t.nodes.delete(n); }) : Promise.resolve();
  /** The landing: the app's ring (#b8893a) opening out from the spot (pill: round a button, its own shape). */
  function ring(t, r, pill) {
    if (!r || t.ff) return;
    const d = Math.max(r.width, r.height) + 10, w = pill ? r.width + 8 : d, h = pill ? r.height + 8 : d, n = t.node("tourRing"), x = r.left + r.width / 2 - w / 2, y = r.top + r.height / 2 - h / 2;
    Object.assign(n.style, { width: w + "px", height: h + "px" }); if (pill) n.style.borderRadius = "999px";
    const s = pill ? [.92, 1.04, 1.22] : [.6, 1.08, 1.5];
    t.anim(n, [{ transform: `translate(${x}px,${y}px) scale(${s[0]})`, opacity: .95 }, { transform: `translate(${x}px,${y}px) scale(${s[1]})`, opacity: .8, offset: .45 }, { transform: `translate(${x}px,${y}px) scale(${s[2]})`, opacity: 0 }], { duration: pill ? 760 : 640, easing: "ease-out" }).then(() => n.remove());
  }
  function plus(t, text, x, y) {
    if (t.ff) return;
    const n = t.node("tourPlus"); n.textContent = text; const w = n.offsetWidth; x -= w / 2;
    t.anim(n, [{ transform: `translate(${x}px,${y}px) scale(.7)`, opacity: 0 }, { transform: `translate(${x}px,${y - 16}px) scale(1.08)`, opacity: 1, offset: .28 }, { transform: `translate(${x}px,${y - 22}px) scale(1)`, opacity: 1, offset: .7 }, { transform: `translate(${x}px,${y - 38}px) scale(1)`, opacity: 0 }], { duration: 1500, easing: "ease-out" }).then(() => n.remove());
  }
  /** Gold sparks left behind along a flight (a few, each fading on its own: nothing laid out per frame). */
  function sparks(t, a, b, ms, bend) {
    if (t.ff) return;
    const mx = (a.x + b.x) / 2, my = (a.y + b.y) / 2 - bend;
    for (let i = 1; i <= 5; i++) {
      const u = i / 6, e = EASE.arc(u), q = 1 - e, x = q * q * a.x + 2 * q * e * mx + e * e * b.x, y = q * q * a.y + 2 * q * e * my + e * e * b.y, n = t.node("tourSpark"), j = (i % 2 ? 1 : -1) * 6;
      n.style.opacity = 0;
      t.anim(n, [{ transform: `translate(${x}px,${y}px) scale(.4)`, opacity: 0 }, { transform: `translate(${x}px,${y}px) scale(1.2)`, opacity: .95, offset: .25 }, { transform: `translate(${x + j}px,${y + 10}px) scale(.2)`, opacity: 0 }], { duration: 620, delay: ms * u, easing: "ease-out" }).then(() => n.remove());
    }
  }

  /* ── the tour ── */
  /** A copy of the card lifted where it stands, made before the list is redrawn (so the eye keeps where it started). */
  function snap(card) {
    if (!card || !card.isConnected || reduced() || !root.Motion) return null;
    const r = rectOf(card); if (!r || !onScreen(r)) return null;
    sheet();
    const th = card.querySelector(".cuDzThumbs"), g = tryDo(() => root.Motion.ghost(card, r));
    if (!g) return null;
    g.remove(); g.classList.add("tourCard"); g.style.left = "0"; g.style.top = "0"; g.style.transform = `translate(${r.left}px,${r.top}px)`; layer().appendChild(g);
    const lift = doc.createElement("i"); lift.className = "tourLift"; g.insertBefore(lift, g.firstChild);
    lift.animate([{ opacity: 0 }, { opacity: 1 }], { duration: 300, easing: SOFT, fill: "forwards" });
    g.animate([{ transform: `translate(${r.left}px,${r.top}px)` }, { transform: `translate(${r.left}px,${r.top - 5}px) scale(1.012)` }], { duration: 300, easing: GROW, fill: "forwards" });
    const imgs = th ? [...th.querySelectorAll("img")].map(i => i.src).filter(Boolean) : [];
    // the list's own update (Motion.reconcile) makes no copy of its own for this card: this one is it
    card._mLeaving = true;
    setTimeout(() => { if (!g._claimed) g.remove(); }, 9000);
    return { ghost: g, rect: r, thumbs: rectOf(th), imgs, mkey: card.dataset.mkey || "", rid: card.dataset.rid || "", node: card, at: Date.now() };
  }
  async function play(o = {}) {
    if (T) { T.forward(); await T.done.catch(() => {}); }
    const legs = (o.legs || []).filter(l => l && (l.poolIds || []).length), waiting = o.waiting || [];
    const s0 = o.from && o.from.ghost ? o.from : null; if (s0) s0.ghost._claimed = true;
    if (reduced() || !root.Motion || (!legs.length && !waiting.length)) { if (s0) s0.ghost.remove(); if (o.note && root.Motion) tryDo(() => root.Motion.pulse(nestTab(), { note: o.note })); return; }
    sheet();
    const t = T = make();
    let finish; t.done = new Promise(r => { finish = r; });
    const home = { mode: modeNow() || "review", scroll: scroller() ? scroller().scrollTop : 0 };
    listen(t, home);
    // pressed in a window (the order window, on any of its tabs): it steps out of the way, alive, and the design lifts
    // off the button pressed; a copy of a card made while a window was open over it shows once that window is back in it.
    // (A window opened while the designs were read, over the card pressed, steps aside too: nothing is flown under it.)
    const pop = o.from ? popOf() : null;
    if (pop) { t.held = tuck(t, pop, o); if (t.held.inside) o = Object.assign({}, o, { from: t.held.spot }); }
    if (s0 && o.delay) s0.ghost.style.visibility = "hidden";
    // a watchdog: never longer than the whole tour should take
    const guard = setTimeout(() => t.forward(), 6000 + (o.delay || 0) + legs.length * 3200 + waiting.length * 2400);
    try {
      if (o.delay) await t.wait(o.delay);
      mark("start");
      const s1 = startFrom(t, s0, o);
      await lift(t, s1.o, s1.s0, legs, waiting);
      // (skipped before the Nest tab was reached: it is never switched to, only to come straight back)
      if (!t.ff) await toNest(t);
      const there = () => !t.userTab && modeNow() === "nest";
      // one sheet at a time, then what waits; a skip lands the rest at once (placed already, never held back) and goes home
      for (const [i, leg] of legs.entries()) { if (!there() || t.ff) break; await sheetBeat(t, leg, o, i === legs.length - 1 && !waiting.length); }
      for (const w of waiting) { if (!there() || t.ff) break; await waitBeat(t, w, o); }
      await coinAway(t);
      await goHome(t, home, o, s0);
    } catch (e) { tryDo(() => console.warn("send tour", e)); if (!t.userTab && modeNow() !== home.mode) setMode(home.mode); }
    finally {
      clearTimeout(guard); t.forward();
      for (const n of t.nodes) n.remove(); if (s0 && s0.ghost) s0.ghost.remove();
      for (const v of [viewOf("nest"), viewOf("review")]) if (v) for (const a of v.getAnimations()) if (a.id === "tour") a.cancel();
      for (const f of t.off) tryDo(f);
      // each Nest card back on the sheet it showed before the tour (it switched them to the sheets it opened); home,
      // this is out of sight
      tryDo(() => { const NF = root.NestFocus; if (!NF) return; if (NF.isOpen && NF.isOpen() && NF.close) NF.close(); if (NF.restore) NF.restore(); });
      // (skipped, a tab picked or anything amiss: the window still comes back as it was)
      if (t.held) await back(t, true).catch(() => {});
      if (T === t) T = null; mark("end"); finish();
    }
  }
  /** 1 · the card lifts, its designs rise out of it as a coin, and the card slides out of the list (or, when it stays,
   *  settles back with its designs gone to the sheets). */
  async function lift(t, o, s0, legs, waiting) {
    const card = s0 ? s0.node : o.from && o.from.nodeType ? o.from : null;
    const live = s0 && s0.mkey ? $(`#rvList [data-mkey="${CSS_ESC(s0.mkey)}"]`) : card && card.isConnected ? card : null;
    const stays = !!(live && rectOf(live));
    const th = s0 ? s0.thumbs : rectOf(card && card.querySelector(".cuDzThumbs")) || rectOf(card);
    const origin = th ? mid(th) : at(innerWidth / 2, innerHeight / 2);
    const coin = t.coin = t.node("tourCoin");
    // the design's own picture on the coin: the one its card (or its window's row) shows, the same image; drawn from
    // the piece itself only when there is none
    const pics = legs.map(l => l.designUrl).concat(waiting.map(w => w.designUrl), s0 ? s0.imgs : []).filter(Boolean);
    t.pic = pics[0] || null;
    const face = coinFace(t.pic, 72); coin.appendChild(face);
    if (!t.pic) { const c0 = legs[0] && charmOf(legs[0].poolIds[0]), pc = c0 && c0.widthPt ? pieceCanvas(c0, 46 / Math.max(c0.widthPt, c0.heightPt)) : null; if (pc) { pc.style.width = pc.style.height = ""; face.appendChild(pc); } }
    const n = legs.reduce((k, l) => k + l.poolIds.length, 0) || (o.pieces || 0);
    if (n > 1) { const b = doc.createElement("b"); b.textContent = "×" + n; coin.appendChild(b); }
    t.count = n;
    const s = th ? Math.max(.3, Math.min(1, th.height / 72)) : .4, up = at(origin.x, Math.max(90, origin.y - 78), 1.12, -6);
    coin.style.transform = tf(at(origin.x, origin.y, s)); coin.style.opacity = 0;
    const cap = capOf(t, `Order ${o.rid || ""}${n ? ` — ${n} piece${n === 1 ? "" : "s"}` : ""}`, null);
    const rise = t.anim(coin, path(at(origin.x, origin.y, s), up, { bend: 10, ease: EASE.grow, oa: 0, ob: 1, n: 14 }), { duration: 580, easing: "linear" });
    sparks(t, at(origin.x, origin.y), up, 580, 10);
    { const cw = cap.offsetWidth, ch = cap.offsetHeight || 30, p = freeSpot(cap, [at(up.x + 46, up.y - ch / 2), at(up.x - 46 - cw, up.y - ch / 2), at(up.x - cw / 2, up.y - 44 - ch), at(up.x + 46, up.y - 44 - ch), at(up.x - cw / 2, up.y + 44)]); placeCap(t, cap, p.x, p.y); t.cap = cap; }
    if (s0 && s0.ghost) {
      const r = s0.rect, g = s0.ghost;
      const nr = stays ? rectOf(live) : null;
      if (nr) t.anim(g, [{ transform: `translate(${r.left}px,${r.top - 5}px) scale(1.012)`, opacity: 1 }, { transform: `translate(${nr.left}px,${nr.top}px) scale(1)`, opacity: .55, offset: .7 }, { transform: `translate(${nr.left}px,${nr.top}px) scale(1)`, opacity: 0 }], { duration: 680, delay: 120, easing: GROW });
      else t.anim(g, [{ transform: `translate(${r.left}px,${r.top - 5}px) scale(1.012)`, opacity: 1 }, { transform: `translate(${r.left + 26}px,${r.top - 12}px) scale(.985)`, opacity: .9, offset: .35 }, { transform: `translate(${r.left + 110}px,${r.top - 18}px) scale(.94)`, opacity: 0 }], { duration: 640, delay: 200, easing: GROW });
    }
    await rise; coin._at = up; await t.wait(90);
  }
  /** 2 · the coin arcs to the Nest tab, which lights; the view crossfades to the Nest tab under it (a real tab switch). */
  async function toNest(t) {
    const tab = nestTab(), coin = t.coin, from = coin._at || fromTf(coin);
    // (it dips into the Nest tab itself, which lights, and goes on from there over the sheet: it never stands over the
    // head of the first sheet card)
    const tr = rectOf(tab), dest = tr ? at(tr.left + tr.width / 2, tr.top + tr.height / 2 + 4, .5, 0) : at(innerWidth / 2, 40, .5, 0);
    const go = t.anim(coin, path(from, dest, { bend: 60, oa: 1, ob: 1 }), { duration: 760, easing: "linear" });
    sparks(t, from, dest, 760, 60);
    fadeOut(t, t.cap, 240); t.cap = null;
    await t.wait(260);
    // notes still standing under the Review tab's things (an earlier send's) would float over the Nest tab: they go
    for (const n of doc.querySelectorAll(".mNote")) if (typeof n.close === "function") tryDo(() => n.close());
    if (tab && root.Motion) tryDo(() => root.Motion.arrive(tab, { plus: false }));
    await switchTo(t, "nest");
    await go; coin._at = dest;
  }
  const fromTf = n => { const m = /translate\(([-\d.]+)px,\s*([-\d.]+)px\).*?rotate\(([-\d.]+)deg\).*?scale\(([-\d.]+)\)/.exec(n.style.transform || ""); return m ? at(+m[1], +m[2], +m[4], +m[3]) : at(innerWidth / 2, innerHeight / 2); };
  /** The view crossfades: the one shown fades, the tab switches under it, the new one comes up. */
  async function switchTo(t, mode) {
    if (t.userTab) return;
    const a = viewOf(modeNow());
    if (a && !t.ff) { const x = a.animate([{ opacity: 1 }, { opacity: 0 }], { duration: 200, easing: "ease-in", fill: "forwards", id: "tour" }); t.anims.add(x); await x.finished.catch(() => {}); t.anims.delete(x); }
    if (t.userTab) { if (a) for (const x of a.getAnimations()) if (x.id === "tour") x.cancel(); return; }
    mark("switch:" + mode); setMode(mode);
    if (a) for (const x of a.getAnimations()) if (x.id === "tour") x.cancel();
    const b = viewOf(mode);
    // (opacity alone: a whole view moved or scaled is a whole view drawn again in each frame)
    if (b && !t.ff) b.animate([{ opacity: 0 }, { opacity: 1 }], { duration: 360, easing: GROW, id: "tour" });
  }
  /** 3 · one sheet: opened, each piece flown onto its spot and landed, "+N", eased back. */
  async function sheetBeat(t, leg, o, last) {
    const caption = leg.label || `${leg.metal} · Sheet ${leg.page || 1}`;
    const NF = root.NestFocus;
    // the coin enters over the sheet itself, never over the card's head (its name and chips); when the order's pieces
    // wait in the card's queue (a Manual run: "press Nest"), over that queue instead: no piece floats on an empty sheet
    const queued = queuedLeg(leg), card0 = nestCard(leg.metal), coin = t.coin;
    const overOf = (cv, q) => q ? at(Math.min(innerWidth - 40, q.left + Math.min(q.width - 24, 96)), q.top + q.height / 2, .6, 0)
      : at(Math.max(60, Math.min(innerWidth - 60, cv.left + cv.width / 2)), cv.top + Math.min(cv.height * .3, 66), .9, 0);
    const aimOf = cd => { const cv = rectOf(cd && $('[data-r="canvas"]', cd)), q = queued ? rectOf(cd && $('[data-r="queue"]', cd)) : null; return cv || q ? overOf(cv || q, q) : null; };
    const o0 = aimOf(card0);
    let from = coin._at || fromTf(coin), going = Promise.resolve();
    if (o0 && onScreen({ left: o0.x, right: o0.x + 1, top: o0.y, bottom: o0.y + 1 }) && !t.ff) { const d = Math.hypot(o0.x - from.x, o0.y - from.y); if (d > 4) { going = t.anim(coin, path(from, o0, { bend: Math.min(70, d * .18) }), { duration: Math.max(420, Math.min(620, 260 + d * .45)), easing: "linear" }); from = o0; } }
    let f = null;
    if (NF && typeof NF.open === "function") f = await t.within(tryDo(() => NF.open(leg.sheetId || leg.metal, { poolIds: leg.poolIds.slice(), caption, metal: leg.metal, page: leg.page || 1, rid: o.rid })), 2600);
    // (skipped while it opened: it is shown whole at once and eases back, with no flight onto it)
    if (t.ff) { if (f) tryDo(() => f.close()); return; }
    if (!f) f = await openHere(t, leg, caption);
    if (!f) return;
    const card = f.card || card0, cr = rectOf(card) || { left: innerWidth / 2 - 100, top: innerHeight / 2 - 60, width: 200, height: 120 };
    const cvr = rectOf(card && $('[data-r="canvas"]', card)) || cr, qr = queued ? rectOf(card && $('[data-r="queue"]', card)) : null;
    // the coin over the sheet (or the queue) it serves, set right once the card stands still
    const over = aimOf(card) || overOf(cvr, null);
    await going;
    if (!t.ff) { const d = Math.hypot(over.x - from.x, over.y - from.y); if (d > 4) { await t.anim(coin, path(from, over, { bend: Math.min(50, d * .18) }), { duration: Math.max(280, Math.min(460, 180 + d * .4)), easing: "linear" }); } }
    coin._at = over;
    // each piece drawn before it flies, at the size it lands at: it leaves as the coin (the design's own picture) and,
    // as it comes down, becomes the charm itself at its spot's turn and size (queued: its picture in the queue)
    const spots = new Map((f.spots || []).map(s => [s.poolId, s]));
    const flights = leg.poolIds.map((id, i) => {
      const s = spots.get(id), c = charmOf(id), r0 = s && s.rect && s.rect.width ? s.rect : null;
      const placed = !!r0 && s.placed !== false && !queued, r = r0 || (queued && qr ? { left: qr.left + 4, top: qr.top + 2, width: Math.min(34, qr.height - 4), height: Math.min(34, qr.height - 4) } : null);
      const k = placed && c ? pxPerPt(c, s, r) : 0;
      let el = k ? pieceCanvas(c, k) : null;
      if (!el) { const q = queued && card && s && s.charmId != null ? [...card.querySelectorAll('[data-r="queue"] img[data-cid]')].find(im => im.dataset.cid === String(s.charmId)) : null, src = (q && q.src) || leg.designUrl || o.designUrl || t.pic;
        if (src) { el = new Image(); el.src = src; el.alt = ""; el.decoding = "sync"; const w = r ? Math.max(r.width, r.height) : 40; el.style.width = el.style.height = w + "px"; el.style.objectFit = "contain"; } }
      if (!el) { el = doc.createElement("i"); el.style.cssText = "display:block;width:26px;height:26px;border-radius:50%;background:rgba(125,86,168,.35);border:2px solid #7d56a8"; }
      if (el.style) { el.style.position = "absolute"; el.style.left = el.style.top = "0"; }
      if (placed && s.flipped && el.style) el.style.transform = "scaleX(-1)";
      return { id, s, r, el, i, placed };
    });
    const landAt = fl => { const r = fl.r || cvr; return at(r.left + r.width / 2, r.top + r.height / 2, 1, fl.placed ? +(fl.s && fl.s.rot) || 0 : 0); };
    const many = flights.length, gap = many > 1 ? Math.max(90, Math.min(220, 900 / many)) : 0, dur = many > 6 ? 580 : 740;
    const beats = flights.map((fl, i) => (async () => {
      await t.wait(i * gap);
      const w = parseFloat(fl.el.style.width) || fl.el.offsetWidth || 30, h = parseFloat(fl.el.style.height) || fl.el.offsetHeight || 30;
      const box = t.node("tourPiece"), D = Math.max(w, h) * 1.3, face = coinFace(t.pic, D);
      Object.assign(box.style, { width: w + "px", height: h + "px", margin: `${-h / 2}px 0 0 ${-w / 2}px` });
      Object.assign(face.style, { left: (w - D) / 2 + "px", top: (h - D) / 2 + "px" });
      box.appendChild(fl.el); box.appendChild(face); fl.el.style.opacity = 0;
      const end = landAt(fl), s0 = 72 * (over.s || .9) / D;
      const start = at(over.x, over.y, s0, end.r - 18);
      // the last piece of the last sheet is the coin itself going down (the coin is not left hovering, empty)
      if (last && i === many - 1 && t.coin) { t.coin.style.visibility = "hidden"; t.coinSpent = true; }
      const go = t.anim(box, path(start, end, { bend: 40 + Math.min(60, Math.abs(start.x - end.x) * .1), ease: EASE.arc, oa: 1, ob: 1, pop: .08, n: 22 }), { duration: dur, easing: "linear" });
      t.anim(face, [{ opacity: 1 }, { opacity: 1, offset: .5 }, { opacity: 0, offset: .88 }, { opacity: 0 }], { duration: dur, easing: "linear" });
      t.anim(fl.el, [{ opacity: 0 }, { opacity: 0, offset: .42 }, { opacity: 1, offset: .86 }, { opacity: 1 }], { duration: dur, easing: "linear" });
      if (i === 0 || !t.ff) sparks(t, start, end, dur, 40);
      await go;
      // (the coin counts down what it still carries, over every sheet: it said ×2 on the second of two sheets)
      if (t.coin) { t.count = Math.max(0, (t.count || 0) - 1); const b = $("b", t.coin); if (b) b.textContent = t.count > 1 ? "×" + t.count : ""; }
      // it lands: the sheet draws it in, in full colour, as the copy that carried it goes
      mark("land"); const landing = t.within(tryDo(() => f.land(fl.id)), 900);
      if (f.here || !fl.placed) ring(t, fl.r || cvr);
      await t.anim(box, [{ opacity: 1 }, { opacity: 0 }], { duration: fl.placed ? 200 : 260, easing: "ease" }); box.remove(); t.nodes.delete(box);
      await landing;
    })());
    await Promise.all(beats);
    if (last) coinAway(t);   // (the last sheet: the coin, empty now, goes as it eases back)
    const n = leg.poolIds.length;
    if (queued) {
      // queued, not placed: said where it waits and what places it, beside the card's own Nest button, which glows
      const nb = card && $('[data-r="nest"]', card), br = rectOf(nb);
      const cap = capOf(t, `Queued on ${caption} — press Nest to place ${n === 1 ? "it" : "them"}`, leg.metal);
      const cw = cap.offsetWidth, ch = cap.offsetHeight || 30;
      const spot = freeSpot(cap, [br && at(br.right + 12, br.top + br.height / 2 - ch / 2), qr && at(qr.left + Math.min(qr.width - cw, 60), qr.top + qr.height / 2 - ch / 2), at(cr.left + cr.width / 2 - cw / 2, cr.bottom + 8), at(cr.left + cr.width / 2 - cw / 2, cr.top - ch - 8)].filter(Boolean));
      placeCap(t, cap, spot.x, spot.y); t.cap = cap;
      if (br) { ring(t, br, true); t.wait(560).then(() => ring(t, br, true)); }
      plus(t, `+${n} queued`, qr ? qr.left + 40 : cvr.left + cvr.width / 2, (qr || cvr).top - 4);
      await t.wait(last ? 520 : 480);
      fadeOut(t, cap, 300); t.cap = null;   // (it goes as the sheet eases back)
    } else {
      plus(t, `+${n} on ${caption}`, cvr.left + cvr.width / 2, cvr.top + 12);
      await t.wait(last ? 400 : 320);
    }
    // another sheet (or a waiting place) follows: this one is let go and is well on its way back before the next is
    // spotlit (only one sheet ever stands in focus), the coin already setting off for it
    if (!last) { tryDo(() => f.close()); await t.wait(260); return; }
    await t.within(tryDo(() => f.close()), 1400);
    await t.wait(120);
  }
  /** Is this leg only queued on its sheet (in its card's queue, no place on the drawn sheet yet: a Manual run)? */
  function queuedLeg(leg) {
    const pg = allSheetsOf().find(p => (p.charms || []).some(c => leg.poolIds.includes(c.poolId))); if (!pg) return false;
    const ids = new Set((pg.charms || []).filter(c => leg.poolIds.includes(c.poolId)).map(c => c.id));
    return !(pg.placements || []).some(p => ids.has(p.id)) || !!pg.roseCutAt;
  }
  /** The coin's face: the design's own picture on a white disc (the card's thumbnail), d px across. */
  function coinFace(url, d) {
    const f = doc.createElement("span"); f.className = "tourFace"; f.style.width = f.style.height = d + "px";
    if (url) { const im = new Image(); im.alt = ""; im.decoding = "sync"; im.src = url; f.appendChild(im); }
    return f;
  }
  /** Where a caption stands clear of any text on the page: the first of the places offered with no letter under it (hit
   *  tested once, before it shows; nothing measured while it moves). */
  function freeSpot(cap, cands) {
    const w = cap.offsetWidth, h = cap.offsetHeight || 30;
    const letterAt = (x, y) => {
      const r = doc.caretRangeFromPoint ? tryDo(() => doc.caretRangeFromPoint(x, y)) : null, n = r && r.startContainer;
      if (!n || n.nodeType !== 3 || !n.length || !n.textContent.trim()) return false;
      const i = Math.max(0, Math.min(n.length - 1, r.startOffset - 1)), g = doc.createRange(); g.setStart(n, i); g.setEnd(n, Math.min(n.length, i + 2));
      for (const b of g.getClientRects()) if (x >= b.left - 3 && x <= b.right + 3 && y >= b.top - 2 && y <= b.bottom + 2) return true;
      return false;
    };
    for (const c of cands) {
      const x = Math.max(8, Math.min(innerWidth - w - 8, c.x)), y = c.y;
      if (y < 4 || y + h > innerHeight - 4) continue;
      let hit = false;
      for (let i = 0; i <= 6 && !hit; i++) for (const fy of [.3, .7]) if (letterAt(x + 4 + (w - 8) * i / 6, y + h * fy)) { hit = true; break; }
      if (!hit) return at(x, y);
    }
    return cands[0] || at(8, 8);
  }
  // the piece's on-screen scale (px per pt) from its spot: the scale given, else read from the box (turned or not)
  function pxPerPt(c, s, r) {
    const w = c.widthPt, h = c.heightPt, th = (+s.rot || 0) * Math.PI / 180, C = Math.abs(Math.cos(th)), S = Math.abs(Math.sin(th));
    const flat = r.width / w, turned = r.width / (w * C + h * S || w);
    if (+s.scale > 0 && Math.abs(s.scale * w - r.width) < r.width * .6 + 4) return +s.scale;
    // the box is the piece's own (unturned) when its shape says so, else the box round the turned piece
    const aspect = r.width / r.height, aFlat = w / h, aTurn = (w * C + h * S) / (w * S + h * C || h);
    return Math.abs(Math.log(aspect / aFlat)) <= Math.abs(Math.log(aspect / aTurn)) ? flat : turned;
  }
  /** 4 · a metal whose pieces wait (no run open, a sheet still busy, or a line held): the coin settles where they wait. */
  async function waitBeat(t, w, o) {
    const card = nestCard(w.metal);
    // NestFocus glides the card into view itself and says where the place will stand once it has (the coin flies there
    // meanwhile); without it the card is brought into view first
    const NF = root.NestFocus, got = NF && typeof NF.waiting === "function" ? tryDo(() => NF.waiting(w.metal)) : null;
    if (!got && card) { const r = rectOf(card); if (!onScreen(r) || r.top < 60 || r.bottom > innerHeight - 20) { card.scrollIntoView({ block: "center", behavior: t.ff ? "auto" : "smooth" }); await t.wait(460); } }
    const r = (got && got.rect && got.rect.width ? got.rect : null) || rectOf(card && ($('[data-r="queue"]', card) && rectOf($('[data-r="queue"]', card)) ? $('[data-r="queue"]', card) : $(".shHead .name", card))) || rectOf(card);
    if (!r) return;
    const coin = t.coin, from = coin._at || fromTf(coin), dest = at(r.left + Math.min(r.width / 2, 60), r.top + r.height / 2, .5, 0);
    await t.anim(coin, path(from, dest, { bend: 50, oa: 1, ob: 1, pop: .1 }), { duration: 720, easing: "linear" });
    coin._at = dest;
    ring(t, { left: dest.x - 18, top: dest.y - 18, width: 36, height: 36 });
    // (what it waits for is said here, on the tour's own layer, where no text of the card is under it: beside its Nest
    // button, else right of its queue, else under or over the card; a note left on the Nest tab would stand alone once home)
    const cap = capOf(t, `${w.label || w.metal} — ${w.why || "goes on with the next run"}`, w.metal), cw = cap.offsetWidth, ch = cap.offsetHeight || 30;
    const br = rectOf(card && $('[data-r="nest"]', card)), qr = rectOf(card && $('[data-r="queue"]', card)), cr = rectOf(card) || r;
    const p = freeSpot(cap, [br && at(br.right + 12, br.top + br.height / 2 - ch / 2), qr && at(qr.left + 60, qr.top + qr.height / 2 - ch / 2), at(cr.left + cr.width / 2 - cw / 2, cr.bottom + 8), at(cr.left + cr.width / 2 - cw / 2, cr.top - ch - 8)].filter(Boolean));
    placeCap(t, cap, p.x, p.y); t.cap = cap;
    await t.wait(1300);
    await fadeOut(t, cap, 240); t.cap = null;
  }
  async function coinAway(t) {
    const c = t.coin; if (!c) return; t.coin = null;
    if (t.coinSpent) { c.remove(); t.nodes.delete(c); return; }   // (it went down onto the sheet itself)
    const p = c._at || fromTf(c);
    await t.anim(c, [{ transform: tf(p), opacity: 1 }, { transform: tf(at(p.x, p.y - 10, p.s * .5, p.r)), opacity: 0 }], { duration: 300, easing: "ease-in" });
    c.remove(); t.nodes.delete(c);
  }
  /** 5 · home: the Review tab as it was left, and where the card went answers. */
  async function goHome(t, home, o, s0) {
    if (t.userTab) return;
    if (modeNow() === "nest") {
      await switchTo(t, home.mode);
      const sc = scroller(); if (sc && home.mode === "review") sc.scrollTop = home.scroll;
      await t.wait(200);
    }
    if (modeNow() !== home.mode) return;
    if (t.held) return backHome(t, o);
    const M = root.Motion; if (!M) return;
    const live = liveOf(s0);
    const show = { label: "Show", title: "open the sheets", fn: () => setMode("nest") }, held = (o.waiting || []).find(w => w.held);
    const words = o.words ? o.words + (held ? ` · ${held.why}` : "") : "", ms = held ? 9000 : 7000;
    if (live && onScreen(rectOf(live))) { const strip = live.querySelector(".cuDesigns") || live; tryDo(() => M.arrive(strip, { plus: false })); if (words) tryDo(() => M.note(strip, { text: words, actions: [show], ms, tone: held ? "bad" : "" })); }
    else { const tab = nestTab(); if (tab) tryDo(() => M.arrive(tab, { plus: false, note: words ? { text: words, actions: [show], ms, tone: held ? "bad" : "" } : null })); }
  }

  /* ── the start and the way home, from a window (Paul, 29 Sep 01:30: "half the animation is not visible because it's
     blocked by the pop-up and it doesn't engage smoothly and it looks broken and disjointed"). Send to Sheet pressed in
     the order window, on any of its tabs: the window is never closed, only stepped out of the way. It shrinks and fades
     toward its card (its backdrop fading with it) as the design lifts off the button pressed, and waits out of sight,
     its order, tab, scroll, focus and typed text untouched. The flight runs in a clear, bare modal layer of the tour's
     own, over everything, the window included: nothing covers it, and nothing under it takes a press (a press or a key
     skips, as always). Home, the window grows back out of its card exactly as it was and says there what went where.
     Transform and opacity only. A card's own Send to Sheet starts from the card itself. ── */
  // (the window over the page as the flight starts, not one already going: the order window's own close, or a copy)
  const popOf = () => [...doc.querySelectorAll("dialog[open]")].reverse().find(d => d.id !== "tourTop" && !d._mdClosing && !(d.id === "orderWin" && root.OrderWin && !OrderWin.isOpen()) && tryDo(() => d.matches(":modal"))) || null;
  /** The flight's layer lifted into the top layer, over the window held (on), or back on the page. */
  function over(on) {
    const l = layer(); let top = $("#tourTop");
    if (on) {
      if (!top) { top = doc.createElement("dialog"); top.id = "tourTop"; top.setAttribute("data-no-grow", ""); top.setAttribute("aria-label", "Sending to the sheets"); top.addEventListener("cancel", e => e.preventDefault()); doc.body.appendChild(top); }
      top.appendChild(l); if (!top.open) tryDo(() => top.showModal());
    } else { if (l.parentNode !== doc.body) doc.body.appendChild(l); if (top && top.open) tryDo(() => top.close()); }
  }
  /** Where the window goes back into: its card, when in sight; else the button pressed. The window keeps its shape,
   *  scaled between the two sizes, centred on it. */
  const aimOf = k => { const h = rectOf(k.home); return h && onScreen(h) ? h : k.R; };
  function tuckTf(D, A) {
    const s = Math.max(.04, Math.min(1, Math.sqrt((A.width / D.width) * (A.height / D.height))));
    return `translate(${(A.left + A.width / 2 - D.width * s / 2 - D.left).toFixed(1)}px,${(A.top + A.height / 2 - D.height * s / 2 - D.top).toFixed(1)}px) scale(${s.toFixed(4)})`;
  }
  function tuck(t, d, o) {
    const D = d.getBoundingClientRect(), R = rectOf(o.from) || (o.from && o.from.rect) || { left: innerWidth / 2 - 20, top: innerHeight / 2 - 12, width: 40, height: 24 };
    const k = { d, D, R, from: o.from, inside: !!(o.from && o.from.nodeType && d.contains(o.from)), home: o.home && o.home.nodeType ? o.home : null, focus: doc.activeElement, st: {} };
    for (const p of ["transformOrigin", "willChange", "visibility"]) k.st[p] = d.style[p];
    // where the design lifts off: the button pressed, as it stood
    k.spot = t.node("tourSpot"); Object.assign(k.spot.style, { width: R.width + "px", height: R.height + "px", transform: `translate(${R.left}px,${R.top}px)`, opacity: 0 });
    over(true); mark("tuck"); d._tourHeld = true;
    Object.assign(d.style, { transformOrigin: "0 0", willChange: "transform,opacity" });
    k.a = d.animate([{ transform: "none", opacity: 1 }, { opacity: .45, offset: .4 }, { transform: tuckTf(D, aimOf(k)), opacity: 0 }], { duration: 480, easing: GROW, fill: "forwards" });
    k.b = tryDo(() => d.animate([{ opacity: 1 }, { opacity: 0 }], { pseudoElement: "::backdrop", duration: 440, easing: "ease", fill: "forwards" }));
    // (out of sight once gone: nothing of it drawn under the flight)
    k.a.finished.then(() => { if (!k.returning) d.style.visibility = "hidden"; }, () => {});
    return k;
  }
  /** The window back as it was, grown out of its card (fast: a skip, a tab picked). */
  async function back(t, fast) {
    const k = t.held; if (!k) return; t.held = null; k.returning = true;
    const d = k.d, ms = fast ? 320 : 560, drop = () => { tryDo(() => k.a.cancel()); tryDo(() => k.b && k.b.cancel()); };
    if (d.isConnected && d.open) {
      // what changed meanwhile is drawn while it is still out of sight (the order window: its Send to Sheet gone)
      tryDo(() => d.dispatchEvent(new CustomEvent("tour:back")));
      d.style.visibility = k.st.visibility; mark("back");
      const a = d.animate([{ transform: tuckTf(k.D, aimOf(k)), opacity: 0 }, { opacity: 1, offset: .45 }, { transform: "none", opacity: 1 }], { duration: ms, easing: GROW });
      tryDo(() => d.animate([{ opacity: 0 }, { opacity: 1 }], { pseudoElement: "::backdrop", duration: ms, easing: "ease" }));
      drop();
      await Promise.race([a.finished.catch(() => {}), new Promise(r => setTimeout(r, ms + 400))]);
    } else drop();
    for (const [p, v] of Object.entries(k.st)) d.style[p] = v;
    d._tourHeld = false;
    // (the tour's layer closing gives the focus back where it was; set again in case it went elsewhere)
    over(false);
    const f = k.focus; if (f && f !== doc.body && f.isConnected && d.contains(f) && f.getClientRects().length && doc.activeElement !== f) tryDo(() => f.focus({ preventScroll: true }));
  }
  /** 5 · home, into the window: it is back as it was, and says, where the button stood, what went where. */
  async function backHome(t, o) {
    const k = t.held; await back(t, t.ff);
    const d = k && k.d, M = root.Motion; if (!d || !d.open || !M || !o.words || !k.inside) return;
    const held = (o.waiting || []).find(w => w.held), words = o.words + (held ? ` · ${held.why}` : ""), ms = held ? 9000 : 7000, R = k.R;
    const at = doc.createElement("i"); at.setAttribute("aria-hidden", "true");
    Object.assign(at.style, { position: "fixed", left: R.left + "px", top: R.top + "px", width: R.width + "px", height: R.height + "px", pointerEvents: "none", visibility: "hidden" });
    d.appendChild(at); setTimeout(() => at.remove(), ms + 2000);
    const n = tryDo(() => M.note(at, { text: words, ms, tone: held ? "bad" : "" })); if (n && d.open) d.appendChild(n);
  }
  /** Where the design starts, from a card: one still in its list is lifted from where it stands now, its copy (made
   *  before the list was drawn again) only fading where it was: it slid over the card drawn anew under it, two cards seen
   *  at once. One gone from its list keeps its copy, which slides out. From its window (the designs window, gone back
   *  into the card): that was the card seen going, and no copy of it shows again (it stood over the window's). */
  function startFrom(t, s0, o) {
    if (!s0 || !s0.ghost) return { s0, o };
    const g = s0.ghost, hid = g.style.visibility === "hidden", live = liveOf(s0), lr = rectOf(live), there = !!(lr && onScreen(lr));
    if (hid || there) { if (hid) g.remove(); else t.anim(g, [{ opacity: 1 }, { opacity: 0 }], { duration: 220, easing: "ease" }).then(() => g.remove()); }
    return there ? { s0: null, o: Object.assign({}, o, { from: live }) } : hid ? { s0: Object.assign({}, s0, { ghost: null }), o } : { s0, o };
  }
  /** The card in the list now (drawn anew once sent, and a custom card sent is another item then: by its order too). */
  const liveOf = s0 => !s0 ? null : (s0.mkey && $(`#rvList [data-mkey="${CSS_ESC(s0.mkey)}"]`)) || (s0.rid && $(`#rvList .reviewListRow[data-rid="${CSS_ESC(s0.rid)}"]`)) || null;
  const CSS_ESC = s => (root.CSS && root.CSS.escape ? root.CSS.escape(s) : String(s).replace(/["\\]/g, "\\$&"));

  root.SendTour = { play, snap, playing: () => !!T, skip: () => { if (T) T.forward(); } };
})(typeof window !== "undefined" ? window : globalThis);
