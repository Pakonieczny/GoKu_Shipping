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
  const CSS = `
#tourLayer{position:fixed;inset:0;z-index:150;pointer-events:none;overflow:hidden;contain:strict}
#tourLayer>*{position:fixed;left:0;top:0;will-change:transform,opacity}
.tourCard{border-radius:14px}
.tourLift{position:absolute!important;inset:0;border-radius:14px;box-shadow:0 22px 48px rgba(30,24,16,.24),0 0 0 1.5px rgba(202,168,97,.55);opacity:0}
.tourCoin{width:72px;height:72px;margin:-36px 0 0 -36px}
.tourCoin>i{position:absolute;inset:0;border-radius:50%;background:radial-gradient(circle at 34% 28%,#fffefa 0,#f6eedc 58%,#eadcbc 100%);border:1.5px solid #caa861;box-shadow:0 14px 30px rgba(60,40,90,.24),0 0 0 6px rgba(125,86,168,.10),inset 0 -3px 8px rgba(169,130,63,.18)}
.tourCoin>img,.tourCoin>canvas{position:absolute;left:14%;top:14%;width:72%;height:72%;object-fit:contain}
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
    t.forward = () => { if (t.ff) return; t.ff = true; for (const a of [...t.anims]) tryDo(() => a.finish()); for (const w of [...t.wakes]) w(); };
    // bounded: a promise from outside (NestFocus) that never settles never holds the tour
    t.within = (p, ms) => Promise.race([Promise.resolve(p).catch(() => null), new Promise(res => setTimeout(() => res(null), t.ff ? Math.min(ms, 600) : ms))]);
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
      const v = viewOf(home.mode);
      if (modeNow() === home.mode && v && e.target && v.contains(e.target)) { t.forward(); return; }
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
  /** The landing: the app's ring (#b8893a) opening out from the spot. */
  function ring(t, r) {
    if (!r || t.ff) return;
    const d = Math.max(r.width, r.height) + 10, n = t.node("tourRing"), x = r.left + r.width / 2 - d / 2, y = r.top + r.height / 2 - d / 2;
    Object.assign(n.style, { width: d + "px", height: d + "px" });
    t.anim(n, [{ transform: `translate(${x}px,${y}px) scale(.6)`, opacity: .95 }, { transform: `translate(${x}px,${y}px) scale(1.08)`, opacity: .8, offset: .45 }, { transform: `translate(${x}px,${y}px) scale(1.5)`, opacity: 0 }], { duration: 640, easing: "ease-out" }).then(() => n.remove());
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
    return { ghost: g, rect: r, thumbs: rectOf(th), imgs, mkey: card.dataset.mkey || "", node: card, at: Date.now() };
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
    // a watchdog: never longer than the whole tour should take
    const guard = setTimeout(() => t.forward(), 6000 + (o.delay || 0) + legs.length * 3200 + waiting.length * 2400);
    try {
      if (o.delay) await t.wait(o.delay);
      await lift(t, o, s0, legs, waiting);
      // (skipped before the Nest tab was reached: it is never switched to, only to come straight back)
      if (!t.ff) await toNest(t);
      const there = () => !t.userTab && modeNow() === "nest";
      for (const [i, leg] of legs.entries()) { if (!there()) break; await sheetBeat(t, leg, o, i === legs.length - 1 && !waiting.length); }
      for (const w of waiting) { if (!there()) break; await waitBeat(t, w, o); }
      await coinAway(t);
      await goHome(t, home, o, s0);
    } catch (e) { tryDo(() => console.warn("send tour", e)); if (!t.userTab && modeNow() !== home.mode) setMode(home.mode); }
    finally {
      clearTimeout(guard); t.forward();
      for (const n of t.nodes) n.remove(); if (s0 && s0.ghost) s0.ghost.remove();
      for (const v of [viewOf("nest"), viewOf("review")]) if (v) for (const a of v.getAnimations()) if (a.id === "tour") a.cancel();
      for (const f of t.off) tryDo(f);
      if (T === t) T = null; finish();
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
    coin.innerHTML = "<i></i>";
    const pics = (s0 && s0.imgs.length ? s0.imgs : []).concat(legs.map(l => l.designUrl).concat(waiting.map(w => w.designUrl))).filter(Boolean);
    // the design itself on the coin, drawn in the custom plum (its thumbnail when no piece is on a sheet yet)
    const c0 = legs[0] && charmOf(legs[0].poolIds[0]), pc = c0 && c0.widthPt ? pieceCanvas(c0, 52 / Math.max(c0.widthPt, c0.heightPt)) : null;
    if (pc) { pc.style.width = pc.style.height = ""; pc.style.objectFit = "contain"; coin.appendChild(pc); }
    else if (pics[0]) { const im = new Image(); im.alt = ""; im.decoding = "sync"; im.src = pics[0]; coin.appendChild(im); }
    const n = legs.reduce((k, l) => k + l.poolIds.length, 0) || (o.pieces || 0);
    if (n > 1) { const b = doc.createElement("b"); b.textContent = "×" + n; coin.appendChild(b); }
    t.count = n;
    const s = th ? Math.max(.3, Math.min(1, th.height / 72)) : .4, up = at(origin.x, Math.max(90, origin.y - 78), 1.12, -6);
    coin.style.transform = tf(at(origin.x, origin.y, s)); coin.style.opacity = 0;
    const cap = capOf(t, `Order ${o.rid || ""}${n ? ` — ${n} piece${n === 1 ? "" : "s"}` : ""}`, null);
    const rise = t.anim(coin, path(at(origin.x, origin.y, s), up, { bend: 10, ease: EASE.grow, oa: 0, ob: 1, n: 14 }), { duration: 580, easing: "linear" });
    sparks(t, at(origin.x, origin.y), up, 580, 10);
    placeCap(t, cap, up.x + 46, up.y - 16); t.cap = cap;
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
    const tr = rectOf(tab), dest = tr ? at(tr.left + tr.width / 2, tr.bottom + 46, .82, 0) : at(innerWidth / 2, 120, .82, 0);
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
    setMode(mode);
    if (a) for (const x of a.getAnimations()) if (x.id === "tour") x.cancel();
    const b = viewOf(mode);
    if (b && !t.ff) b.animate([{ opacity: 0, transform: "translateY(8px) scale(.995)" }, { opacity: 1, transform: "none" }], { duration: 380, easing: GROW, id: "tour" });
  }
  /** 3 · one sheet: opened, each piece flown onto its spot and landed, "+N", eased back. */
  async function sheetBeat(t, leg, o, last) {
    const caption = leg.label || `${leg.metal} · Sheet ${leg.page || 1}`;
    const NF = root.NestFocus;
    // the coin sets off for the sheet's card as it opens (one continuous move), and is set right once it stands still
    const overOf = r => at(Math.max(60, Math.min(innerWidth - 60, r.left + r.width / 2)), Math.max(70, r.top - 38), .9, 0);
    const coin = t.coin, r0 = rectOf(nestCard(leg.metal) && $('[data-r="canvas"]', nestCard(leg.metal)));
    let from = coin._at || fromTf(coin), going = Promise.resolve();
    if (r0 && onScreen(r0) && !t.ff) { const o0 = overOf(r0), d = Math.hypot(o0.x - from.x, o0.y - from.y); if (d > 4) { going = t.anim(coin, path(from, o0, { bend: Math.min(70, d * .18) }), { duration: Math.max(380, Math.min(560, 240 + d * .4)), easing: "linear" }); from = o0; } }
    let f = null;
    if (NF && typeof NF.open === "function") f = await t.within(tryDo(() => NF.open(leg.sheetId || leg.metal, { poolIds: leg.poolIds.slice(), caption, metal: leg.metal, page: leg.page || 1, rid: o.rid })), 2600);
    if (!f) f = await openHere(t, leg, caption);
    if (!f) return;
    const card = f.card || nestCard(leg.metal), cr = rectOf(card) || { left: innerWidth / 2 - 100, top: innerHeight / 2 - 60, width: 200, height: 120 };
    const cvr = rectOf(card && $('[data-r="canvas"]', card)) || cr;
    // the coin over the sheet it serves
    const over = overOf(cvr);
    await going;
    if (!t.ff) { const d = Math.hypot(over.x - from.x, over.y - from.y); if (d > 4) { await t.anim(coin, path(from, over, { bend: Math.min(70, d * .18) }), { duration: Math.max(300, Math.min(540, 200 + d * .4)), easing: "linear" }); } }
    coin._at = over;
    // each piece drawn before it flies, at the size it lands at
    const spots = new Map((f.spots || []).map(s => [s.poolId, s]));
    const flights = [];
    leg.poolIds.forEach((id, i) => {
      const s = spots.get(id), c = charmOf(id), r = s && s.rect && s.rect.width ? s.rect : null;
      const k = r && c ? pxPerPt(c, s, r) : 0;
      let el = k ? pieceCanvas(c, k) : null;
      if (!el && (leg.designUrl || o.designUrl)) { el = new Image(); el.src = leg.designUrl || o.designUrl; el.alt = ""; const w = r ? Math.max(r.width, r.height) : 40; el.style.width = el.style.height = w + "px"; el.style.objectFit = "contain"; }
      if (!el) { el = doc.createElement("i"); el.style.cssText = "display:block;width:26px;height:26px;border-radius:50%;background:rgba(125,86,168,.35);border:2px solid #7d56a8"; }
      flights.push({ id, s, r, el, i });
    });
    const landAt = (fl) => { const r = fl.r || cvr; return at(r.left + r.width / 2, r.top + r.height / 2, 1, +(fl.s && fl.s.rot) || 0); };
    const many = flights.length, gap = many > 1 ? Math.max(90, Math.min(220, 900 / many)) : 0, dur = many > 6 ? 560 : 760;
    const beats = flights.map((fl, i) => (async () => {
      await t.wait(i * gap);
      const box = t.node("tourPiece"); box.appendChild(fl.el);
      const w = fl.el.offsetWidth || 30, h = fl.el.offsetHeight || 30; box.style.margin = `${-h / 2}px 0 0 ${-w / 2}px`;
      const end = landAt(fl), s0 = Math.min(.6, 44 / Math.max(w, h));
      const start = at(over.x, over.y, s0, end.r - 25);
      const lands = fl.r ? end : Object.assign({}, end, { s: .35 });
      const go = t.anim(box, path(start, lands, { bend: 40 + Math.min(60, Math.abs(start.x - lands.x) * .1), ease: EASE.arc, oa: 0, ob: 1, pop: .08, n: 20 }), { duration: dur, easing: "linear" });
      if (i === 0 || !t.ff) sparks(t, start, lands, dur, 40);
      await go;
      if (t.coin && many > 1) { const b = $("b", t.coin); if (b) b.textContent = many - i - 1 > 1 ? "×" + (many - i - 1) : ""; }
      // it lands: the sheet draws it in, in full colour, as the copy that carried it goes
      const landing = t.within(tryDo(() => f.land(fl.id)), 900);
      if (f.here || !fl.r) ring(t, fl.r || cvr);
      await t.anim(box, [{ opacity: 1 }, { opacity: 0 }], { duration: 180, easing: "ease" }); box.remove(); t.nodes.delete(box);
      await landing;
    })());
    await Promise.all(beats);
    if (last) coinAway(t);   // (the last sheet: the coin, empty now, goes as it eases back)
    plus(t, `+${leg.poolIds.length} on ${caption}`, cvr.left + cvr.width / 2, cvr.top + 12);
    await t.wait(400);
    await t.within(tryDo(() => f.close()), 1400);
    await t.wait(120);
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
    // (what it waits for is said here, on the tour's own layer: a note left on the Nest tab would stand alone once home)
    const cap = capOf(t, `${w.label || w.metal} — ${w.why || "goes on with the next run"}`, w.metal); placeCap(t, cap, dest.x - 40, Math.max(8, dest.y - 64)); t.cap = cap;
    await t.anim(coin, path(from, dest, { bend: 50, oa: 1, ob: 1, pop: .1 }), { duration: 720, easing: "linear" });
    coin._at = dest;
    ring(t, { left: dest.x - 18, top: dest.y - 18, width: 36, height: 36 });
    await t.wait(1300);
    await fadeOut(t, cap, 240); t.cap = null;
  }
  async function coinAway(t) {
    const c = t.coin; if (!c) return; t.coin = null;
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
    const M = root.Motion; if (!M) return;
    const live = s0 && s0.mkey ? $(`#rvList [data-mkey="${CSS_ESC(s0.mkey)}"]`) : null;
    const show = { label: "Show", title: "open the sheets", fn: () => setMode("nest") }, held = (o.waiting || []).find(w => w.held);
    const words = o.words ? o.words + (held ? ` · ${held.why}` : "") : "", ms = held ? 9000 : 7000;
    if (live && onScreen(rectOf(live))) { const strip = live.querySelector(".cuDesigns") || live; tryDo(() => M.arrive(strip, { plus: false })); if (words) tryDo(() => M.note(strip, { text: words, actions: [show], ms, tone: held ? "bad" : "" })); }
    else { const tab = nestTab(); if (tab) tryDo(() => M.arrive(tab, { plus: false, note: words ? { text: words, actions: [show], ms, tone: held ? "bad" : "" } : null })); }
  }
  const CSS_ESC = s => (root.CSS && root.CSS.escape ? root.CSS.escape(s) : String(s).replace(/["\\]/g, "\\$&"));

  root.SendTour = { play, snap, playing: () => !!T, skip: () => { if (T) T.forward(); } };
})(typeof window !== "undefined" ? window : globalThis);
