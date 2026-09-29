/* Charm Nest · the Send to Sheet tour (Paul, 29 Sep 00:25; calmer and clearer, 01:30).
   "It should show me a beautiful animation as that order is animating away from the Review tab into the nest tab and
   then into the appropriate sheet inside the Nest tab … one animation at a time … and then fully coming back to where
   the user started." And then: "very jarring, and not that clear … rushed. Must be a bit slower and smoother, and there
   has to be more happening in between … so that the user is more informed about what went where … simpler".
   The placement is done first (CustomSheet.send); this only shows what happened, as one calm path, each step said in
   one caption (one place, the foot of the view; each held long enough to read):
     1 · lift     the order's designs rise out of its card as a coin              "Order 4175423829 · lion · 1 piece"
     2 · to Nest  the coin glides up into the Nest tab, which lights; the view
                  the tab leaves is held where it stands while the Nest tab is
                  laid out and drawn behind it, and the two dissolve into each
                  other — the screen is never bare                              "To the Nest tab"
     3 · sheets   one sheet at a time, in the light: its name; then each piece
                  leaves the Nest tab and flies onto its own place on it, at its
                  own turn and size, and lands with the ring and pop               "RG 14/20 · Sheet 1 · 2 of 3"
                                                                                  "Placed on Sheet 1 +2"
     4 · waiting  a metal with nothing on a sheet yet: it goes where they wait    "SS · waiting · why"
     5 · home     the view crossfades back to the Review tab as it was left       "Back to Review"
   Each move takes a fixed time wherever it starts (TIME), on one soft ease-in-out curve, with no turn until it comes
   down; only transform and opacity are animated, and nothing is laid out while anything moves. With reduced motion:
   the same captions over short fades, and nothing flies. A click, a key or Esc skips to the end (everything lands at
   once) and goes home; a tab picked during the tour ends it there. */
(function (root) {
  "use strict";
  const doc = root.document;
  // one soft ease-in-out for every move; a view fades out gathering pace and in settling (so the stage is bare briefly)
  // (ASIDE: the order window's move as it steps aside and back, soft at both ends and never fast in between; its fade and
  // its backdrop's run evenly beside it, the least change the screen can take in any one frame)
  const SOFT = "cubic-bezier(.42,0,.18,1)", OUT = "cubic-bezier(.4,0,1,1)", IN = "cubic-bezier(0,0,.2,1)", ASIDE = "cubic-bezier(.25,0,.75,1)", RING = "#b8893a";
  /** How long each part takes (ms): the same whatever the tour starts from and wherever a sheet stands. */
  const TIME = Object.freeze({
    aside: 560,     //     the order window steps aside as the designs rise (and comes back, the same motion reversed)
    rise: 700,      // 1 · the designs rise out of the card as a coin
    toNest: 1150,   // 2 · the coin glides up into the Nest tab
    side: 220,      //     the rail's own things and the top bar's tools go first, while the view still stands
    fade: 520,      //     the view held in its place and the one coming in dissolve into each other
    glide: 650,     // 3 · a further sheet glides into view and takes the light
    beat: 600,     //     the sheet named, before the first piece comes down
    fly: 1050,      //     a piece flies from the Nest tab onto its place
    gap: 300,       //     between pieces going onto one sheet (closer when there are many)
    read: 1400,     //     the least time a caption stands, to be read
    cap: 300        //     a caption comes up in this; the one before has gone by then
  });
  /** With reduced motion: nothing flies; the same captions, over short fades. */
  const GENTLE = Object.freeze({ fade: 200, read: 1400 });
  const DOCK = .52;  // the coin's size while it waits in the Nest tab
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
.tourLift{position:absolute!important;inset:0;border-radius:14px;box-shadow:0 20px 44px rgba(30,24,16,.2),0 0 0 1.5px rgba(202,168,97,.5);opacity:0}
.tourCoin{width:72px;height:72px;margin:-36px 0 0 -36px}
.tourCoin>.tourFace{left:0;top:0;width:100%;height:100%}
.tourFace{position:absolute;display:block;border-radius:50%;background:radial-gradient(circle at 50% 50%,#fff 0,#fff 64%,#fbf6ea 80%,#efe2c2 100%);border:1.5px solid #caa861;box-shadow:0 12px 26px rgba(60,40,90,.22),0 0 0 5px rgba(125,86,168,.09),inset 0 -3px 8px rgba(169,130,63,.16)}
.tourFace>img,.tourFace>canvas{position:absolute;left:17%;top:17%;width:66%;height:66%;object-fit:contain}
.tourPiece>img,.tourPiece>canvas{display:block}
.tourCoin>b{position:absolute;right:-5px;top:-5px;font:800 10.5px var(--sans,system-ui);background:#7d56a8;color:#fff;border-radius:999px;padding:2px 7px;box-shadow:0 3px 8px rgba(60,40,90,.3)}
.tourCap{display:flex;align-items:center;gap:10px;max-width:calc(100vw - 28px);padding:10px 20px 10px 15px;border-radius:999px;background:rgba(255,254,250,.97);border:1px solid var(--goldLine,#e3d3a6);box-shadow:0 12px 32px rgba(30,24,16,.16),0 2px 6px rgba(30,24,16,.06);white-space:nowrap;color:var(--ink,#221f1b)}
.tourCap>i{flex:0 0 auto;width:10px;height:10px;border-radius:50%;background:#caa861;box-shadow:0 0 0 3px rgba(202,168,97,.2)}
.tourCap>b{font:600 18px/1.15 var(--serif,Georgia,serif);letter-spacing:.01em;min-width:0;overflow:hidden;text-overflow:ellipsis}
.tourCap>em{font:800 12px/1 var(--sans,system-ui);font-style:normal;color:#3f5b3a;background:#eef5ea;border:1px solid #cddcc9;border-radius:999px;padding:3px 9px}
.tourCap>small{font:600 12px/1.2 var(--sans,system-ui);color:var(--ink70,#6b645a);padding-left:11px;border-left:1px solid var(--goldLine,#e3d3a6);min-width:0;max-width:min(460px,42vw);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.tourPiece{transform-origin:50% 50%;filter:drop-shadow(0 6px 10px rgba(60,40,90,.26))}
.tourRing{border-radius:50%;border:2px solid ${RING};box-shadow:0 0 14px rgba(184,137,58,.45)}
.tourHole{border-radius:16px;box-shadow:0 0 0 200vmax rgba(34,31,27,.26),0 0 0 2px rgba(202,168,97,.8),0 24px 60px rgba(30,24,16,.3)}
#stage>.tourHold{position:fixed!important;margin:0!important;z-index:1;pointer-events:none;contain:strict}
#modeSeg.tourTabs>button{transition:background-color .5s ease,color .5s ease}
@media (prefers-reduced-motion:reduce){#modeSeg.tourTabs>button{transition:none}}`;
  function sheet() { if ($("#tourCss")) return; const s = doc.createElement("style"); s.id = "tourCss"; s.textContent = CSS; doc.head.appendChild(s); }
  function layer() { let l = $("#tourLayer"); if (!l) { l = doc.createElement("div"); l.id = "tourLayer"; l.setAttribute("aria-hidden", "true"); doc.body.appendChild(l); } return l; }

  /* ── easing, sampled: a path is a curve with the soft timing along it, as one animation of transform alone ── */
  function bezier(x1, y1, x2, y2) {
    const cx = 3 * x1, bx = 3 * (x2 - x1) - cx, ax = 1 - cx - bx, cy = 3 * y1, by = 3 * (y2 - y1) - cy, ay = 1 - cy - by;
    const X = u => ((ax * u + bx) * u + cx) * u, Y = u => ((ay * u + by) * u + cy) * u, dX = u => (3 * ax * u + 2 * bx) * u + cx;
    return t => { if (t <= 0) return 0; if (t >= 1) return 1; let u = t; for (let i = 0; i < 8; i++) { const d = dX(u); if (Math.abs(d) < 1e-6) break; u -= (X(u) - t) / d; } u = Math.max(0, Math.min(1, u)); return Y(u); };
  }
  // soft: a short move. glide: a long arc — the same time, a lower top speed (1.57× its average), so the eye keeps up.
  const EASE = { soft: bezier(.42, 0, .18, 1), glide: bezier(.37, 0, .63, 1) };
  const smooth = x => x <= 0 ? 0 : x >= 1 ? 1 : x * x * (3 - 2 * x);
  const tf = p => `translate(${p.x.toFixed(1)}px,${p.y.toFixed(1)}px) rotate(${(p.r || 0).toFixed(2)}deg) scale(${Math.max(.001, p.s).toFixed(4)})`;
  /** Keyframes along one gentle arc from a to b: it bows by `bend` of its length (up, or right when it goes straight up
   *  or down: never a zig-zag), eased by `ease`; opacity and scale go from a's to b's, `swell` lifts it a little in the
   *  middle, and it keeps its turn until `turn` of the way, then turns to b's as it comes down. */
  function path(a, b, o = {}) { return frames(curve(a, b, o), o.n || 24); }
  /** The same arc as a point at each moment u (0…1) of it: x, y, r, s and its opacity o. */
  function curve(a, b, { bend = 0, ease = EASE.soft, oa = 1, ob = 1, swell = 0, turn = 0, ramp = 1.6 } = {}) {
    const dx = b.x - a.x, dy = b.y - a.y, d = Math.hypot(dx, dy) || 1;
    let nx = dy / d, ny = -dx / d; if (ny > .2 || (Math.abs(ny) <= .2 && nx < 0)) { nx = -nx; ny = -ny; }
    const k = Math.min(110, d * bend), cx = (a.x + b.x) / 2 + nx * k, cy = (a.y + b.y) / 2 + ny * k;
    return u => {
      const t = ease(u), q = 1 - t, tr = turn ? smooth((t - turn) / (1 - turn)) : t;
      return { x: q * q * a.x + 2 * q * t * cx + t * t * b.x, y: q * q * a.y + 2 * q * t * cy + t * t * b.y, r: (a.r || 0) + ((b.r || 0) - (a.r || 0)) * tr,
        s: (a.s + (b.s - a.s) * t) * (1 + swell * Math.sin(Math.PI * t)), o: oa + (ob - oa) * Math.min(1, t * ramp) };
    };
  }
  const frames = (f, n) => Array.from({ length: n + 1 }, (_, i) => { const p = f(i / n); return { offset: i / n, transform: tf(p), opacity: p.o }; });
  const at = (x, y, s = 1, r = 0) => ({ x, y, s, r });
  const mid = r => at(r.left + r.width / 2, r.top + r.height / 2);
  const rectOf = e => { const r = e && e.isConnected ? e.getBoundingClientRect() : null; return r && r.width > 0 && r.height > 0 ? r : null; };
  const onScreen = r => !!r && r.bottom > 0 && r.top < innerHeight && r.right > 0 && r.left < innerWidth;
  // three frames: a view shown now is laid out, drawn (its sheets' pictures included) and painted before it fades in
  const painted = t => t.ff ? Promise.resolve() : new Promise(res => { let k = 0; const h = setTimeout(res, 260), f = () => { if (++k >= 3) { clearTimeout(h); res(); } else requestAnimationFrame(f); }; requestAnimationFrame(f); });

  /* ── one tour at a time ── */
  let T = null;
  function make() {
    const t = { ff: false, userTab: false, anims: new Set(), wakes: new Set(), nodes: new Set(), flying: new Set(), off: [] };
    t.wait = ms => t.ff || !(ms > 0) ? Promise.resolve() : new Promise(res => { const w = () => { clearTimeout(h); t.wakes.delete(w); res(); }, h = setTimeout(w, ms); t.wakes.add(w); });
    // until the caption shown has stood long enough to be read
    t.hold = ms => t.wait((t.capSince || 0) + (ms || (t.gentle ? GENTLE.read : TIME.read)) - performance.now());
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
      if (tabOf(e)) { t.userTab = true; unpark(t); t.forward(); return; }
      if (t.ff) return;
      // a press on the view it goes home to, while that view is still in sight, is the person's own: it goes through
      // (not while a window waits to come back over it: it would open a second one)
      const v = viewOf(home.mode);
      if (!t.win && modeNow() === home.mode && v && e.target && v.contains(e.target)) { t.forward(); return; }
      swallow = Date.now() + 700; e.preventDefault(); e.stopPropagation(); t.forward();
    };
    const click = e => { if (Date.now() < swallow && !tabOf(e)) { e.preventDefault(); e.stopPropagation(); swallow = 0; } };
    const key = e => { if (e.key === "Escape") { e.preventDefault(); e.stopPropagation(); } t.forward(); };
    const hash = () => { t.userTab = true; unpark(t); t.forward(); };
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
  const metalName = leg => String(leg.label || "").split(" · ")[0] || leg.metal;
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

  /* ── the caption: one line, in one place ── */
  /** Said at each step, centred at the foot of the view, where the eye finds it every time: the caption before fades as
   *  this one comes up in its place. main: what happens; small: a detail; plus: what arrived ("+2"); metal: its dot. */
  function say(t, main, small, o = {}) {
    if (t.ff) return;
    const old = t.cap, n = doc.createElement("div"); n.className = "tourCap";
    const dot = doc.createElement("i"), col = o.metal && tryDo(() => CN().METALS.find(m => m.key === o.metal).color); if (col) dot.style.background = col; n.appendChild(dot);
    const b = doc.createElement("b"); b.textContent = main; n.appendChild(b);
    // (a space between the parts: read aloud and copied as words; a flex box draws none of it)
    if (o.plus) { const e = doc.createElement("em"); e.textContent = o.plus; n.append(" ", e); }
    if (small) { const s = doc.createElement("small"); s.textContent = small; n.append(" ", s); }
    const still = !!t.gentle;
    n.style.opacity = "0"; layer().appendChild(n); t.nodes.add(n);
    const place = dy => capPlace(t, n, dy);
    n.style.transform = place(0);
    const lag = old ? 90 : 0;
    t.anim(n, [{ opacity: 0, transform: place(still ? 0 : 8) }, { opacity: 1, transform: place(0) }], { duration: still ? GENTLE.fade : TIME.cap, delay: lag, easing: SOFT, fill: "both" });
    if (old) unsay(t, old, still ? GENTLE.fade : 260);
    t.cap = n; t.capSince = performance.now() + lag;
  }
  /** One place, the foot of the view — kept inside the window, however long the words and however narrow it is. */
  function capPlace(t, n, dy) {
    const w = n.offsetWidth || 320, x = Math.min(Math.max(t.capAt.x, w / 2 + 10), Math.max(w / 2 + 10, innerWidth - w / 2 - 10));
    return `translate(${x.toFixed(1)}px,${(t.capAt.y + dy).toFixed(1)}px) translate(-50%,-50%)`;
  }
  const capAtOf = () => { const r = rectOf($("#stage")); return r ? { x: r.left + r.width / 2, y: Math.min(innerHeight, r.bottom) - 54 } : { x: innerWidth / 2, y: innerHeight - 54 }; };
  /** The window resized: the caption standing goes with the foot of the view (as the page does), and each piece still
   *  coming down is re-aimed. */
  function resized(t) {
    t.capAt = capAtOf();
    // (each glides there from where it is drawn now, in a moment: set there at once, it jumped across the screen)
    const glide = (n, to, op) => {
      const cs = getComputedStyle(n), from = { transform: cs.transform === "none" ? to : cs.transform, opacity: cs.opacity };
      for (const a of n.getAnimations()) tryDo(() => a.cancel());
      Object.assign(n.style, { transform: to, opacity: op });
      if (!t.ff && !t.gentle) t.anim(n, [from, { transform: to, opacity: op }], { duration: 320, easing: SOFT, fill: "none" });
    };
    const n = t.cap; if (n && n.isConnected) glide(n, capPlace(t, n, 0), "1");
    // the coin waiting in the Nest tab stays hanging from it (the pieces still to go leave from there)
    const c = t.coin, d = t.dock;
    if (c && c.isConnected && d && c._at === d && c.getAnimations().every(a => a.playState === "finished")) {
      const d1 = dockOf();
      if (Math.hypot(d1.x - d.x, d1.y - d.y) > 1) { glide(c, tf(d1), "1"); c._at = t.dock = d1; }
    }
    reaim(t);
  }
  function unsay(t, n, ms = 300) {
    if (!n || !n.isConnected) return Promise.resolve();
    if (t.cap === n) t.cap = null;
    const o = +getComputedStyle(n).opacity || 0;
    return t.anim(n, [{ opacity: o }, { opacity: 0 }], { duration: ms, easing: "ease-in" }).then(() => { n.remove(); t.nodes.delete(n); });
  }
  function whatOf(o, legs, waiting) {
    const names = [...new Set([].concat(...legs.map(l => l.designs || []), ...waiting.map(w => w.designs || [])))].filter(Boolean);
    const n = o.pieces || legs.reduce((k, l) => k + l.poolIds.length, 0);
    return [names.length > 2 ? `${names.length} designs` : names.join(", "), n ? `${n} piece${n === 1 ? "" : "s"}` : ""].filter(Boolean).join(" · ");
  }

  /** The sheet opened here when part B's NestFocus is not on the page: its card brought into view and spotlit, and each
   *  piece's spot read from the sheet as drawn (its active page only; otherwise the middle of the sheet). */
  async function openHere(t, leg, instant) {
    const card = nestCard(leg.metal); if (!card) return null;
    const r0 = card.getBoundingClientRect();
    if (!onScreen(r0) || r0.top < 60 || r0.bottom > innerHeight - 20) { card.scrollIntoView({ block: "center", behavior: t.ff || instant || t.gentle ? "auto" : "smooth" }); if (!instant) await t.wait(TIME.glide); }
    const r = card.getBoundingClientRect(), hole = t.node("tourHole");
    Object.assign(hole.style, { width: r.width + 12 + "px", height: r.height + 12 + "px", transform: `translate(${r.left - 6}px,${r.top - 6}px)`, opacity: 0 });
    t.anim(hole, [{ opacity: 0 }, { opacity: 1 }], { duration: t.gentle ? GENTLE.fade : 420, easing: SOFT });
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
  const fadeOut = (t, n, ms = 260) => n && n.isConnected ? t.anim(n, [{ opacity: getComputedStyle(n).opacity }, { opacity: 0 }], { duration: ms, easing: "ease" }).then(() => { n.remove(); t.nodes.delete(n); }) : Promise.resolve();
  /** The landing: the app's ring (#b8893a) opening out from the spot (pill: round a button, its own shape). */
  function ring(t, r, pill) {
    if (!r || t.ff || t.gentle) return;
    const d = Math.max(r.width, r.height) + 10, w = pill ? r.width + 8 : d, h = pill ? r.height + 8 : d, n = t.node("tourRing"), x = r.left + r.width / 2 - w / 2, y = r.top + r.height / 2 - h / 2;
    Object.assign(n.style, { width: w + "px", height: h + "px" }); if (pill) n.style.borderRadius = "999px";
    // (round a wide thing, it grows by a few px, not by its size)
    const sc = k => pill ? `scale(${(1 + k * 2 / w).toFixed(4)},${(1 + k * 2 / h).toFixed(4)})` : `scale(${k})`, s = pill ? [-2, 3, 12] : [.6, 1.08, 1.5];
    t.anim(n, [{ transform: `translate(${x}px,${y}px) ${sc(s[0])}`, opacity: .9 }, { transform: `translate(${x}px,${y}px) ${sc(s[1])}`, opacity: .75, offset: .45 }, { transform: `translate(${x}px,${y}px) ${sc(s[2])}`, opacity: 0 }], { duration: pill ? 900 : 700, easing: "ease-out" }).then(() => n.remove());
  }

  /* ── the tab change: a crossfade, the new view drawn before it shows ── */
  const RAIL = ".rail > .railActions, .rail > .queueHead, .rail > .railScroll, .rail > .railRecent, .rail > .railFoot";
  /** What a tab change hides or shows: its view, the rail under the ladder (the file queue is the Nest tab's own) and
   *  the top bar's tools (the Nest tab's Learned switch comes and goes, and moves the rest). */
  const barOf = () => { const bar = $(".topbar"); return bar ? [...bar.children].filter(n => !n.matches(".modeNav,.navBtn")).flatMap(n => n.matches(".topTools") && getComputedStyle(n).display === "contents" ? [...n.children] : [n]) : []; };
  const sceneOf = mode => [viewOf(mode), ...doc.querySelectorAll(RAIL), ...barOf()].filter(n => n && n.getClientRects().length);
  const dropTour = nodes => { for (const n of nodes) if (n) for (const a of n.getAnimations()) if (a.id === "tour") a.cancel(); };
  /** Every sheet card drawn now, not on the next frame: the Nest tab is whole before it is seen. */
  function drawNow() {
    tryDo(() => {
      for (const m of (CN().METALS || [])) {
        const pg = tryDo(() => root.activePage(m.key)); if (!pg) continue;
        if (pg._drawFrame) { cancelAnimationFrame(pg._drawFrame); pg._drawFrame = 0; }
        tryDo(() => root.drawPreviewNow(pg));
      }
    });
  }
  /** The view the tab is leaving, held exactly where it stands: out of the layout (so the next tab lays out behind it)
   *  and clipped to the stage, it keeps the screen filled until the new view has been drawn. */
  function holdView(t, n, mode) {
    const r = rectOf(n), st = rectOf($("#stage")); if (!r) return null;
    const h = { n, mode, style: n.getAttribute("style") || "" }, disp = getComputedStyle(n).display;
    n.classList.add("tourHold");
    // (it keeps its box while its tab is changed: hidden and shown again, it was laid out anew in the switch's own frame)
    if (disp && disp !== "none") n.style.setProperty("display", disp, "important");
    const clip = st ? `inset(${Math.max(0, st.top - r.top)}px ${Math.max(0, r.right - st.right)}px ${Math.max(0, r.bottom - st.bottom)}px ${Math.max(0, st.left - r.left)}px)` : "none";
    Object.assign(n.style, { left: r.left + "px", top: r.top + "px", width: r.width + "px", height: r.height + "px", clipPath: clip });
    // (fixed to the window, unless something up the tree makes it its own: then by the difference it landed off by)
    const r2 = n.getBoundingClientRect();
    if (Math.abs(r2.left - r.left) > .5 || Math.abs(r2.top - r.top) > .5) Object.assign(n.style, { left: 2 * r.left - r2.left + "px", top: 2 * r.top - r2.top + "px" });
    t.held = h; return h;
  }
  /** It lets go: back into the layout, and hidden again unless its own tab is the one showing. park: the view the tour
   *  comes home to stays laid out where it stood, unseen (opacity 0, no pointer), until it is switched back to: shown
   *  again, it was laid out anew in that switch's own frame (a long list: 30-40 ms). */
  function letGo(t, h, park) {
    h = h || t.held; if (!h) return; if (t.held === h) t.held = null;
    if (park && modeNow() !== h.mode) { h.n.classList.add("hidden"); h.n.style.opacity = "0"; h.n.setAttribute("aria-hidden", "true"); t.parked = h; return; }
    h.n.classList.remove("tourHold");
    if (h.style) h.n.setAttribute("style", h.style); else h.n.removeAttribute("style");
    if (modeNow() !== h.mode) h.n.classList.add("hidden");
  }
  /** The parked view back in its place (shown by its tab, or hidden as ever). */
  function unpark(t) {
    const h = t.parked; if (!h) return; t.parked = null;
    h.n.removeAttribute("aria-hidden"); letGo(t, h);
  }
  /** A view (or a part of one) fades; .done once it has. */
  function fadeView(t, n, a, b, ms) {
    const x = n.animate([{ opacity: a }, { opacity: b }], { duration: ms, easing: b ? IN : OUT, fill: "forwards", id: "tour" });
    if (t.ff) x.finish(); else t.anims.add(x);
    x.done = x.finished.catch(() => {}).then(() => { t.anims.delete(x); });
    return x;
  }
  /** The rail's things and the bar's tools go first, while the view still stands; then the view is held where it is,
   *  the tab changes behind it, the new view is set up as it will first be seen (prepare) and drawn, and the two
   *  dissolve into each other (shown: as the new one comes in). Resolves once it is in. */
  async function switchTo(t, mode, prepare, shown) {
    if (t.userTab) return;
    const g = !!t.gentle, was = modeNow(), v0 = viewOf(was), ms = g ? GENTLE.fade : TIME.fade;
    const held = t.ff ? null : holdView(t, v0, was);
    const outs = t.ff ? [] : [...doc.querySelectorAll(RAIL), ...barOf()].filter(n => n && n.getClientRects().length)
      .map(n => fadeView(t, n, 1, 0, g ? GENTLE.fade : TIME.side))
      .concat(held || !v0 ? [] : [fadeView(t, v0, 1, 0, ms)]);   // (nothing to hold: it fades, as it used to)
    await Promise.all(outs.map(x => x.done));
    if (t.userTab) { for (const x of outs) x.cancel(); letGo(t, held); return; }
    mark("switch:" + mode); setMode(mode);
    if (held) held.n.classList.remove("hidden");   // hidden by the tab change: it stays in sight where it stands
    if (t.parked && t.parked.n === viewOf(mode)) unpark(t);   // (laid out as it was left: back in its place)
    // what shows now is held out of sight (paused on its first frame) before any of it can paint; what was faded out
    // lets go only then (the rail's foot is in both)
    const ins = [];
    if (!t.ff) {
      for (const n of sceneOf(mode)) ins.push(n.animate([{ opacity: 0 }, { opacity: 1 }], { duration: ms, easing: IN, fill: "both", id: "tour" }));
      for (const a of ins) { a.pause(); t.anims.add(a); }
    }
    const fin = ins.map(a => a.finished.catch(() => {}).then(() => { t.anims.delete(a); a.cancel(); }));
    for (const x of outs) x.cancel();
    // the new view's first layout (the tab change) has this frame to itself; what it is set up with comes after it is
    // painted (a frame, then a task of its own: the switch itself runs in the frame's animation update)
    if (prepare && !t.ff) await new Promise(res => requestAnimationFrame(() => setTimeout(res, 0)));
    if (prepare) await Promise.resolve(tryDo(prepare)).catch(() => {});
    if (mode === "nest") drawNow();
    await painted(t);
    const bye = held ? fadeView(t, held.n, 1, 0, ms) : null;
    for (const a of ins) if (a.playState === "paused") a.play();
    if (shown) tryDo(shown);
    await Promise.all(fin.concat(bye ? [bye.done] : []));
    if (bye) bye.cancel();
    letGo(t, held, !!held && held.mode === t.homeMode && !t.ff && !t.userTab);
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
    lift.animate([{ opacity: 0 }, { opacity: 1 }], { duration: 360, easing: SOFT, fill: "forwards" });
    g.animate([{ transform: `translate(${r.left}px,${r.top}px)` }, { transform: `translate(${r.left}px,${r.top - 4}px) scale(1.01)` }], { duration: 360, easing: SOFT, fill: "forwards" });
    const imgs = th ? [...th.querySelectorAll("img")].map(i => i.src).filter(Boolean) : [];
    // the list's own update (Motion.reconcile) makes no copy of its own for this card: this one is it — by its key,
    // since the list is redrawn (the busy Send button) before the line leaves it, and the node then is a new one
    card._mLeaving = true;
    tryDo(() => root.Motion.carry && root.Motion.carry(card.dataset.mkey, 12000));
    setTimeout(() => { if (!g._claimed) g.remove(); }, 9000);
    return { ghost: g, rect: r, thumbs: rectOf(th), imgs, mkey: card.dataset.mkey || "", rid: card.dataset.rid || "", node: card, at: Date.now() };
  }
  async function play(o = {}) {
    if (T) { T.forward(); await T.done.catch(() => {}); }
    const legs = (o.legs || []).filter(l => l && (l.poolIds || []).length), waiting = o.waiting || [];
    const s0 = o.from && o.from.ghost ? o.from : null; if (s0) s0.ghost._claimed = true;
    if (!root.Motion || (!legs.length && !waiting.length)) { if (s0) s0.ghost.remove(); if (o.note && root.Motion) tryDo(() => root.Motion.pulse(nestTab(), { note: o.note })); return; }
    sheet();
    const t = T = make(); t.gentle = reduced(); t.legs = legs; t.waits = waiting;
    if (t.gentle && s0) s0.ghost.remove();
    let finish; t.done = new Promise(r => { finish = r; });
    // the card its list holds where it stood while the tour carries it (CustomSheet.send: Motion.carry), let go home
    t.holdKey = (s0 && s0.mkey) || (o.home && o.home.dataset && o.home.dataset.mkey) || "";
    const home = { mode: modeNow() || "review", scroll: scroller() ? scroller().scrollTop : 0 }; t.homeMode = home.mode;
    home.anchors = home.mode === "review" ? tryDo(() => anchorsOf(t.holdKey)) || [] : [];
    t.capAt = capAtOf();
    listen(t, home);
    let rz = 0; const onResize = () => { cancelAnimationFrame(rz); rz = requestAnimationFrame(() => resized(t)); };
    addEventListener("resize", onResize); t.off.push(() => { removeEventListener("resize", onResize); cancelAnimationFrame(rz); });
    // pressed in a window (the order window, on any of its tabs): it steps out of the way, alive, and the design lifts
    // off the button pressed; a copy of a card made while a window was open over it shows once that window is back in it.
    // (A window opened while the designs were read, over the card pressed, steps aside too: nothing is flown under it.
    // With reduced motion too: the gentle version's captions are never under a window.)
    const pop = o.from ? popOf() : null;
    if (pop) { t.win = tuck(t, pop, o); if (t.win.inside) o = Object.assign({}, o, { from: t.win.spot }); }
    if (s0 && o.delay) s0.ghost.style.visibility = "hidden";
    const tabs = $("#modeSeg"); if (tabs) { tabs.classList.add("tourTabs"); t.off.push(() => setTimeout(() => tabs.classList.remove("tourTabs"), 600)); }
    // a watchdog: never longer than the whole tour should take
    const pieces = legs.reduce((k, l) => k + l.poolIds.length, 0);
    const guard = setTimeout(() => t.forward(), (o.delay || 0) + (t.gentle ? 4000 + (legs.length + waiting.length) * 2200 : 7000 + legs.length * 4600 + waiting.length * 4200 + pieces * 400));
    try {
      if (o.delay) await t.wait(o.delay);
      mark("start");
      // (the gentle version lifts nothing: its card's copy went already, and home is said on the card or in its window)
      if (t.gentle) await gentle(t, o, legs, waiting, home, s0);
      else {
        // where the design starts: the card where it stands now, the copy made before the list was redrawn, or the
        // button pressed in the window stepped aside
        const s1 = startFrom(t, s0, o);
        await lift(t, s1.o, s1.s0, legs, waiting);
        // (skipped before the Nest tab was reached: it is never switched to, only to come straight back)
        if (!t.ff) await toNest(t, s1.o, legs, waiting);
        const there = () => !t.userTab && modeNow() === "nest";
        // one sheet at a time, then what waits; a skip lands the rest at once (placed already, never held back) and goes home
        for (const [i, leg] of legs.entries()) { if (!there() || t.ff) break; await sheetBeat(t, leg, s1.o, i); }
        for (const [j, w] of waiting.entries()) { if (!there() || t.ff) break; await waitBeat(t, w, j); }
        await coinAway(t);
        await goHome(t, home, o, s0);
      }
    } catch (e) { tryDo(() => console.warn("send tour", e)); if (!t.userTab && modeNow() !== home.mode) setMode(home.mode); }
    finally {
      clearTimeout(guard); t.forward();
      letGo(t); unpark(t);
      for (const n of t.nodes) n.remove(); if (s0 && s0.ghost) s0.ghost.remove();
      // the list has its row back (home, it was laid out anew already; a tab picked: the next time it is drawn)
      for (const k of [s0 && s0.mkey, t.holdKey]) if (k) tryDo(() => root.Motion.carry && root.Motion.carry(k, 0));
      dropTour([viewOf("nest"), viewOf("review"), ...doc.querySelectorAll(RAIL), ...barOf()]);
      for (const f of t.off) tryDo(f);
      // each Nest card back on the sheet it showed before the tour (it switched them to the sheets it opened); home,
      // this is out of sight
      endFocus(t);
      // (skipped, a tab picked or anything amiss: the window still comes back as it was)
      if (t.win) await back(t, true).catch(() => {});
      if (T === t) T = null; mark("end"); finish();
    }
  }
  /** 1 · the order's designs rise out of its card as a coin; the card settles into its new place (still listed), or,
   *  gone from the list, lifts away where it stood. */
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
    // it rises 64 px — further when the card sits at the foot of the window, so it never stands on the caption
    const s = th ? Math.max(.3, Math.min(1, th.height / 72)) : .4, up = at(origin.x, Math.max(110, Math.min(origin.y - 64, t.capAt.y - 104)), 1, 0);
    coin.style.transform = tf(at(origin.x, origin.y, s)); coin.style.opacity = 0;
    say(t, `Order ${o.rid || ""}`.trim(), whatOf(o, legs, waiting));
    const rise = t.anim(coin, path(at(origin.x, origin.y, s), up, { oa: 0, ob: 1, n: 16, ramp: 1 }), { duration: TIME.rise, easing: "linear" });
    if (s0 && s0.ghost) {
      const r = s0.rect, g = s0.ghost, n0 = stays ? rectOf(live) : null, a = `translate(${r.left}px,${r.top - 4}px) scale(1.01)`;
      const nr = n0 && Math.hypot(n0.left - r.left, n0.top - r.top) < 120 ? n0 : null;
      if (nr) t.anim(g, [{ transform: a, opacity: 1 }, { transform: `translate(${nr.left}px,${nr.top}px) scale(1)`, opacity: 1, offset: .6 }, { transform: `translate(${nr.left}px,${nr.top}px) scale(1)`, opacity: 0 }], { duration: 900, delay: 200, easing: SOFT });
      else t.anim(g, [{ transform: a, opacity: 1 }, { transform: `translate(${r.left}px,${r.top - 12}px) scale(.985)`, opacity: 0 }], { duration: 760, delay: 260, easing: SOFT });
    }
    await rise; coin._at = up;
    await t.hold();
  }
  /** Where the coin waits in the Nest tab: hanging from the tab, at the foot of the top bar. */
  function dockOf() {
    const r = rectOf(nestTab()), bar = rectOf($(".topbar"));
    return r ? at(r.left + r.width / 2, bar ? bar.bottom : r.bottom + 6, DOCK) : at(innerWidth / 2, 48, DOCK);
  }
  /** 2 · the coin glides up into the Nest tab, which lights; the view crossfades to the Nest tab, its first sheet
   *  already in the light (set up out of sight: nothing moves under the fade), and the coin waits there. */
  async function toNest(t, o, legs, waiting) {
    const coin = t.coin, from = coin._at || fromTf(coin), dock = t.dock = dockOf();
    say(t, "To the Nest tab", legs.length > 1 ? `onto ${legs.length} sheets` : "");
    const go = t.anim(coin, path(from, dock, { bend: .12, n: 26, swell: .05, ease: EASE.glide }), { duration: TIME.toNest, easing: "linear" });
    await t.wait(200);   // (it is seen leaving the Review tab before that fades)
    // notes still standing under the Review tab's things (an earlier send's) would float over the Nest tab: they go
    for (const n of doc.querySelectorAll(".mNote")) if (typeof n.close === "function") tryDo(() => n.close());
    const first = legs[0], w0 = !first && waiting[0];
    const switching = switchTo(t, "nest", async () => {
      if (first) t.f0 = await openLeg(t, first, o, true);
      else if (w0) t.w0 = waitPlace(t, w0, true);
    // the sheet it goes to is named as the Nest tab comes in (once "To the Nest tab" has been read)
    }, () => { t.said0 = t.hold().then(() => { if (first) sayLeg(t, first, 0); else if (w0) sayWait(t, w0); }); });
    await go; coin._at = dock;
    if (!t.ff) ring(t, rectOf(nestTab()), true);
    await switching;
  }
  const fromTf = n => { const m = /translate\(([-\d.]+)px,\s*([-\d.]+)px\).*?rotate\(([-\d.]+)deg\).*?scale\(([-\d.]+)\)/.exec(n.style.transform || ""); return m ? at(+m[1], +m[2], +m[4], +m[3]) : at(innerWidth / 2, innerHeight / 2); };
  /** What the window's foot keeps for the caption (px from its bottom): a landing is brought into view above it. */
  const footOf = t => Math.max(0, Math.round(innerHeight - (t.capAt ? t.capAt.y : innerHeight - 54) + 30));
  /** A sheet taken into the light (NestFocus, or here); the one before hands its light over. instant: out of sight. */
  async function openLeg(t, leg, o, instant) {
    const NF = root.NestFocus, prev = t.focus;
    if (prev && prev.here) tryDo(() => prev.close());
    let f = null;
    if (NF && typeof NF.open === "function") f = await t.within(tryDo(() => NF.open(leg.sheetId || leg.metal, { poolIds: leg.poolIds.slice(), caption: leg.label || `${leg.metal} · Sheet ${leg.page || 1}`, noCaption: true, instant, ms: TIME.glide, foot: footOf(t), metal: leg.metal, page: leg.page || 1, rid: o.rid })), 2600);
    if (t.ff) { if (f) tryDo(() => f.close()); return null; }
    if (!f) f = await openHere(t, leg, instant);
    if (f) t.focus = f;
    return f;
  }
  /** The sheet in the light lets go, and each Nest card goes back to the sheet it showed before (home, out of sight). */
  function endFocus(t) {
    const f = t.focus; t.focus = null; if (f) tryDo(() => f.close());
    tryDo(() => { const NF = root.NestFocus; if (!NF) return; if (NF.isOpen && NF.isOpen() && NF.close) NF.close(); if (NF.restore) NF.restore(); });
  }
  /** 3 · one sheet: in the light and named; each piece leaves the Nest tab and flies onto its place, one after another;
   *  what arrived is said. */
  const sayLeg = (t, leg, i) => say(t, leg.label || `${metalName(leg)} · Sheet ${leg.page || 1}`, t.legs.length > 1 ? `${i + 1} of ${t.legs.length}` : "", { metal: leg.metal });
  const sayWait = (t, w) => say(t, `${w.label || w.metal} · waiting`, w.why || "goes on with the next run", { metal: w.metal });
  async function sheetBeat(t, leg, o, i) {
    const n = t.legs.length, page = leg.page || 1, of = n > 1 ? `${i + 1} of ${n}` : "";
    if (i === 0 && t.said0) await t.said0; else sayLeg(t, leg, i);
    let f = i === 0 ? t.f0 : null; t.f0 = null;
    if (!f) f = await openLeg(t, leg, o, false);
    if (t.ff || !f) return;
    await t.hold(TIME.beat);   // (its name read; it stands on through the flight, well past TIME.read)
    if (t.ff) return;
    const queued = queuedLeg(leg), card = f.card || nestCard(leg.metal);
    const cvr = rectOf(card && $('[data-r="canvas"]', card)) || rectOf(card) || { left: innerWidth / 2 - 100, top: innerHeight / 2 - 60, width: 200, height: 120 };
    const qr = queued ? rectOf(card && $('[data-r="queue"]', card)) : null;
    // each piece drawn before it flies, at the size it lands at: it leaves as the coin (the design's own picture) and,
    // as it comes down, becomes the charm itself at its spot's turn and size (queued: its picture in the queue)
    const spots = new Map((f.spots || []).map(s => [s.poolId, s]));
    const flights = leg.poolIds.map(id => {
      const s = spots.get(id), c = charmOf(id), r0 = s && s.rect && s.rect.width ? s.rect : null;
      const placed = !!r0 && s.placed !== false && !queued, r = r0 || (queued && qr ? { left: qr.left + 4, top: qr.top + 2, width: Math.min(34, qr.height - 4), height: Math.min(34, qr.height - 4) } : null);
      const k = placed && c ? pxPerPt(c, s, r) : 0;
      let el = k ? pieceCanvas(c, k) : null;
      if (!el) { const q = queued && card && s && s.charmId != null ? [...card.querySelectorAll('[data-r="queue"] img[data-cid]')].find(im => im.dataset.cid === String(s.charmId)) : null, src = (q && q.src) || leg.designUrl || o.designUrl || t.pic;
        if (src) { el = new Image(); el.src = src; el.alt = ""; el.decoding = "sync"; const w = r ? Math.max(r.width, r.height) : 40; el.style.width = el.style.height = w + "px"; el.style.objectFit = "contain"; } }
      if (!el) { el = doc.createElement("i"); el.style.cssText = "display:block;width:26px;height:26px;border-radius:50%;background:rgba(125,86,168,.35);border:2px solid #7d56a8"; }
      if (el.style) { el.style.position = "absolute"; el.style.left = el.style.top = "0"; }
      if (placed && s.flipped && el.style) el.style.transform = "scaleX(-1)";
      return { id, s, r, el, placed };
    });
    const last = i === n - 1 && !t.waits.length, many = flights.length, gap = many > 1 ? Math.min(TIME.gap, 1300 / (many - 1)) : 0;
    const after = [];
    await Promise.all(flights.map((fl, j) => t.wait(j * gap).then(() => t.ff ? null : flyOnto(t, fl, f, cvr, last && j === many - 1, after))));
    if (t.ff) return;
    const k = leg.poolIds.length;
    if (queued) {
      // queued, not placed (a Manual run): said where it waits and what places it; the card's own Nest button answers
      say(t, `Queued on Sheet ${page}`, `press Nest to place ${k === 1 ? "it" : "them"}`, { metal: leg.metal, plus: `+${k}` });
      ring(t, rectOf(card && $('[data-r="nest"]', card)), true);
    } else say(t, `Placed on Sheet ${page}`, [metalName(leg), of].filter(Boolean).join(" · "), { metal: leg.metal, plus: `+${k}` });
    await Promise.all([t.hold(), ...after]);
  }
  /** One piece from the Nest tab onto its place: a copy peels off the coin (the last is the coin itself), arcs down,
   *  turns to its place as it comes down, and lands: the sheet draws it in with the ring and pop as the copy goes. */
  async function flyOnto(t, fl, f, cvr, asCoin, after) {
    const w = parseFloat(fl.el.style.width) || fl.el.offsetWidth || 30, h = parseFloat(fl.el.style.height) || fl.el.offsetHeight || 30;
    const box = t.node("tourPiece"), D = Math.max(w, h) * 1.3, face = coinFace(t.pic, D);
    Object.assign(box.style, { width: w + "px", height: h + "px", margin: `${-h / 2}px 0 0 ${-w / 2}px` });
    Object.assign(face.style, { left: (w - D) / 2 + "px", top: (h - D) / 2 + "px" });
    box.appendChild(fl.el); box.appendChild(face); fl.el.style.opacity = 0;
    // its place read as it leaves (the window may have changed since the sheet was opened), at the size drawn for it
    const d = t.dock || dockOf(), r0 = fl.r || cvr, now = fl.placed && f.spot ? tryDo(() => f.spot(fl.id)) : null, r = now && now.rect && now.rect.width ? now.rect : r0;
    const start = at(d.x, d.y, 72 * d.s / D, 0), end = at(r.left + r.width / 2, r.top + r.height / 2, r.width / r0.width || 1, fl.placed ? +(now ? now.rot : fl.s && fl.s.rot) || 0 : 0);
    box.style.transform = tf(start);
    if (asCoin && t.coin) { t.coin.style.visibility = "hidden"; t.coinSpent = true; } else countDown(t);
    const fly = { box, fl, f, r, end, t0: performance.now(), way: curve(start, end, { bend: .14, swell: .08, turn: .4, ease: EASE.glide }) };
    fly.go = t.anim(box, frames(fly.way, 28), { duration: TIME.fly, easing: "linear" });
    t.flying.add(fly);
    t.anim(face, [{ opacity: 1 }, { opacity: 1, offset: .5 }, { opacity: 0, offset: .86 }, { opacity: 0 }], { duration: TIME.fly, easing: "linear" });
    t.anim(fl.el, [{ opacity: 0 }, { opacity: 0, offset: .44 }, { opacity: 1, offset: .86 }, { opacity: 1 }], { duration: TIME.fly, easing: "linear" });
    // (re-aimed on a resize: its animation is swapped for one bending onto its new place, in the time it had left)
    for (let go = null; go !== fly.go;) { go = fly.go; await go; }
    t.flying.delete(fly);
    mark("land"); const landing = t.within(tryDo(() => f.land(fl.id)), 1200);
    if (f.here || !fl.placed) ring(t, fly.r);
    after.push(t.anim(box, [{ opacity: 1 }, { opacity: 0 }], { duration: fl.placed ? 240 : 300, easing: "ease" }).then(() => { box.remove(); t.nodes.delete(box); }), landing);
  }
  /** The window resized while pieces come down: each still flying bends, from where it is, onto where its place stands
   *  now (and its size there), in the time it had left; no jump, and it lands on its place. */
  function reaim(t) {
    if (t.ff) return;
    const now = performance.now();
    for (const fly of t.flying) {
      const s = fly.fl.placed && fly.f.spot ? tryDo(() => fly.f.spot(fly.fl.id)) : null, r = s && s.rect && s.rect.width ? s.rect : null;
      const u0 = Math.min(1, (now - fly.t0) / TIME.fly); if (!r || u0 > .96) continue;
      const e = fly.end, e1 = at(r.left + r.width / 2, r.top + r.height / 2, e.s * r.width / fly.r.width, e.r);
      if (Math.hypot(e1.x - e.x, e1.y - e.y) < 1 && Math.abs(e1.s / e.s - 1) < .01) continue;
      const old = fly.way, rest = u => { const p = old(u0 + (1 - u0) * u), w = smooth(u); return Object.assign({}, p, { x: p.x + (e1.x - e.x) * w, y: p.y + (e1.y - e.y) * w, s: p.s * (1 + (e1.s / e.s - 1) * w) }); };
      fly.way = u => u <= u0 ? old(u) : rest((u - u0) / (1 - u0));
      fly.end = e1; fly.r = r;
      // (the one running is cancelled and the next set going in the same moment: no frame between them)
      for (const a of fly.box.getAnimations()) tryDo(() => a.cancel());
      fly.go = t.anim(fly.box, frames(rest, 20), { duration: Math.max(60, TIME.fly * (1 - u0)), easing: "linear" });
    }
  }
  /** The coin gives one away: it counts down what it still holds. */
  function countDown(t) {
    if (!t.coin) return;
    t.count = Math.max(0, (t.count || 0) - 1); const b = $("b", t.coin); if (b) b.textContent = t.count > 1 ? "×" + t.count : "";
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
  // the piece's on-screen scale (px per pt) from its spot: the scale given, else read from the box (turned or not)
  function pxPerPt(c, s, r) {
    const w = c.widthPt, h = c.heightPt, th = (+s.rot || 0) * Math.PI / 180, C = Math.abs(Math.cos(th)), S = Math.abs(Math.sin(th));
    const flat = r.width / w, turned = r.width / (w * C + h * S || w);
    if (+s.scale > 0 && Math.abs(s.scale * w - r.width) < r.width * .6 + 4) return +s.scale;
    // the box is the piece's own (unturned) when its shape says so, else the box round the turned piece
    const aspect = r.width / r.height, aFlat = w / h, aTurn = (w * C + h * S) / (w * S + h * C || h);
    return Math.abs(Math.log(aspect / aFlat)) <= Math.abs(Math.log(aspect / aTurn)) ? flat : turned;
  }
  /** Where a metal's waiting pieces go: NestFocus glides its card into view and says where the place will stand (the
   *  coin flies there meanwhile); without it the card is brought into view at once. */
  function waitPlace(t, w, instant) {
    const NF = root.NestFocus, got = NF && typeof NF.waiting === "function" ? tryDo(() => NF.waiting(w.metal, { instant, ms: TIME.glide, foot: footOf(t) })) : null;
    if (got && got.rect && got.rect.width) return got;
    const card = nestCard(w.metal); if (!card) return null;
    const r0 = rectOf(card); if (!onScreen(r0) || r0.top < 60 || r0.bottom > innerHeight - 20) card.scrollIntoView({ block: "center", behavior: "auto" });
    const q = $('[data-r="queue"]', card);
    return { rect: rectOf(q) || rectOf($(".shHead .name", card)) || rectOf(card), card };
  }
  /** 4 · a metal whose pieces wait (no run open, a sheet still busy, or a line held): they go where they wait, and why
   *  is said. */
  async function waitBeat(t, w, j) {
    if (j === 0 && !t.legs.length && t.said0) await t.said0; else sayWait(t, w);
    // the sheet last in the light lets it go: the card where they wait is seen as it is, not dimmed
    if (t.focus) { const f = t.focus; t.focus = null; tryDo(() => f.close()); }
    const place = j === 0 && t.w0 ? t.w0 : waitPlace(t, w, false); t.w0 = null;
    const r = place && place.rect; if (!r || t.ff) return;
    const dest = at(r.left + Math.min(r.width / 2, 60), r.top + r.height / 2, .5, 0), d = t.dock || dockOf();
    // a copy of the coin (the coin itself when nothing else follows)
    let n;
    if (j === t.waits.length - 1 && t.coin) { n = t.coin; t.coin = null; } else { n = t.node("tourCoin"); n.appendChild(coinFace(t.pic, 72)); n.style.transform = tf(d); countDown(t); }
    await t.anim(n, path(d, dest, { bend: .14, n: 26, ease: EASE.glide }), { duration: TIME.fly, easing: "linear" });
    ring(t, { left: dest.x - 18, top: dest.y - 18, width: 36, height: 36 });
    await t.hold();
    await fadeOut(t, n, 320);
  }
  async function coinAway(t) {
    const c = t.coin; if (!c) return; t.coin = null;
    if (t.coinSpent) { c.remove(); t.nodes.delete(c); return; }   // (it went down onto the sheet itself)
    const p = c._at || fromTf(c);
    await t.anim(c, [{ transform: tf(p), opacity: 1 }, { transform: tf(at(p.x, p.y, p.s * .6, p.r)), opacity: 0 }], { duration: 360, easing: SOFT });
    c.remove(); t.nodes.delete(c);
  }
  /** The Review list as the user sees it as the tour starts: the cards in sight (not the one sent), each by its key and
   *  where it stands in the list's window; the sent card's neighbours first (the one just above, just below, and on out;
   *  with no sent card in the list, from the top of the window down). */
  function anchorsOf(sentKey) {
    const sc = scroller(), host = $("#rvList"); if (!sc || !host) return [];
    const box = sc.getBoundingClientRect(), rows = [...host.children].filter(n => n.dataset && n.dataset.mkey && n.getClientRects().length);
    const at = sentKey ? rows.findIndex(n => n.dataset.mkey === sentKey) : -1;
    const rank = i => at < 0 ? i : i < at ? (at - i) * 2 - 1 : (i - at) * 2;
    return rows.map((n, i) => ({ n, i, r: n.getBoundingClientRect() })).filter(x => x.n.dataset.mkey !== sentKey && x.r.bottom > box.top + 1 && x.r.top < box.bottom - 1)
      .sort((a, b) => rank(a.i) - rank(b.i)).map(x => ({ key: x.n.dataset.mkey, dy: x.r.top - box.top }));
  }
  /** The list scrolled so the cards the user was looking at stand where they stood, the sent one gone from among them:
   *  the nearest one that can stand there (at the list's foot, one below it); else its scroll as it was. */
  function anchor(home) {
    const sc = scroller(), host = $("#rvList"); if (!sc) return;
    const max = sc.scrollHeight - sc.clientHeight, top = sc.getBoundingClientRect().top;
    for (const a of (host && home.anchors) || []) {
      const n = host.querySelector(`[data-mkey="${CSS_ESC(a.key)}"]`), r = rectOf(n); if (!r) continue;
      const want = Math.round(sc.scrollTop + r.top - top - a.dy);
      if (want >= -1 && want <= max + 1) { sc.scrollTop = want; return; }
    }
    sc.scrollTop = home.scroll;
  }
  /** Home, the list the tour held still lets go of the card it carried and is laid out anew: out of sight (still), in
   *  one go and scrolled to the cards the user was looking at; in sight (a skip before the Nest tab), as the list moves. */
  function settleList(t, home, still) {
    const k = t.holdKey; t.holdKey = "";
    if (k) tryDo(() => root.Motion.carry(k, 0));
    if (home.mode !== "review") return;
    if (k) tryDo(() => root.Review && root.Review.render(still ? { still: true } : undefined));
    if (still) tryDo(() => anchor(home));
  }
  /** 5 · home: the Review tab as it was left (its sub-tab, scroll and filters), and where the card went answers. */
  async function goHome(t, home, o, s0) {
    if (t.userTab) return;
    if (modeNow() !== home.mode) {
      say(t, `Back to ${{ review: "Review" }[home.mode] || home.mode}`, "");
      // (the light is let go after the switch, once the Nest tab is out of sight: it is still seen during the dissolve)
      await switchTo(t, home.mode, () => settleList(t, home, true));
    } else settleList(t, home, false);
    endFocus(t);
    if (modeNow() !== home.mode) return;
    // a window held: it comes back first (and, when the press was its own, says there what went where)
    if (t.win && await backHome(t, o)) return;
    const M = root.Motion; if (!M) return;
    const live = liveOf(s0);
    const show = { label: "Show", title: "open the sheets", fn: () => setMode("nest") }, held = (o.waiting || []).find(w => w.held);
    const words = o.words ? o.words + (held ? ` · ${held.why}` : "") : "", ms = held ? 9000 : 7000, note = words ? { text: words, actions: [show], ms, tone: held ? "bad" : "" } : null;
    const at0 = live && onScreen(rectOf(live)) ? live.querySelector(".cuDesigns") || live : nestTab();
    if (at0) { ring(t, rectOf(at0), true); if (note) tryDo(() => M.note(at0, note)); }
    if (t.ff) return;
    await t.hold();
    await unsay(t, t.cap, t.gentle ? GENTLE.fade : 300);
  }
  /** With reduced motion: the same story, gently: captions over short fades, each sheet shown in the light with its
   *  pieces on it (nothing flies, nothing glides), then home. */
  async function gentle(t, o, legs, waiting, home, s0) {
    say(t, `Order ${o.rid || ""}`.trim(), whatOf(o, legs, waiting));
    await t.hold();
    if (t.ff) return goHome(t, home, o, s0);
    await switchTo(t, "nest", async () => { if (legs[0]) t.f0 = await openLeg(t, legs[0], o, true); else if (waiting[0]) t.w0 = waitPlace(t, waiting[0], true); });
    const there = () => !t.userTab && modeNow() === "nest";
    // a further sheet: the view dips while it is brought into view (it is never scrolled in sight)
    const dip = async fn => { const v = viewOf("nest"); if (!v) return fn(); const a = fadeView(t, v, 1, 0, 150); await a.done; const r = await fn(); await painted(t); const b = fadeView(t, v, 0, 1, 150); a.cancel(); await b.done; b.cancel(); return r; };
    for (const [i, leg] of legs.entries()) {
      if (!there() || t.ff) break;
      let f = i === 0 ? t.f0 : null; t.f0 = null;
      if (!f) f = await dip(() => openLeg(t, leg, o, true));
      if (!f || t.ff) break;
      const of = legs.length > 1 ? `${i + 1} of ${legs.length}` : "";
      say(t, `${queuedLeg(leg) ? "Queued on" : "Placed on"} Sheet ${leg.page || 1}`, [metalName(leg), of].filter(Boolean).join(" · "), { metal: leg.metal, plus: `+${leg.poolIds.length}` });
      await t.hold();
    }
    for (const [j, w] of waiting.entries()) {
      if (!there() || t.ff) break;
      if (!(j === 0 && t.w0)) await dip(() => waitPlace(t, w, true));
      t.w0 = null;
      sayWait(t, w);
      await t.hold();
    }
    await goHome(t, home, o, s0);
  }

  /* ── the start and the way home, from a window (Paul, 29 Sep 01:30: "half the animation is not visible because it's
     blocked by the pop-up and it doesn't engage smoothly and it looks broken and disjointed"). Send to Sheet pressed in
     the order window, on any of its tabs: the window is never closed, only stepped out of the way. In one soft motion
     (TIME.aside) it eases part of the way toward its card, a little smaller, and fades, its backdrop in step, as the
     design lifts off the button pressed in the same frame; then it waits out of sight,
     its order, tab, scroll, focus and typed text untouched. The flight runs in a clear, bare modal layer of the tour's
     own, over everything, the window included: nothing covers it, and nothing under it takes a press (a press or a key
     skips, as always). Home, the same motion reversed brings the window back exactly as it was, and it says there what
     went where.
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
  /** Where the window steps toward: its card, when in sight; else the button pressed. */
  const aimOf = k => { const h = rectOf(k.home); return h && onScreen(h) ? h : k.R; };
  /** The window's own box as laid out, whatever transform it is drawn with now. */
  const LAG = 160;   // (ms: how far the frames on screen may trail the page's own clock while a window moves)
  const boxOf = d => d.offsetWidth && d.offsetHeight ? { left: d.offsetLeft, top: d.offsetTop, width: d.offsetWidth, height: d.offsetHeight } : d.getBoundingClientRect();
  /** Stepped aside: a fifth of the way toward its card and a little smaller (at most an eighth), its shape kept. Only
   *  part of the way: the whole screen never sweeps down into a card in a few frames, and the eye stays on the design. */
  function asideTf(D, A) {
    const full = Math.max(.04, Math.min(1, Math.sqrt((A.width / D.width) * (A.height / D.height)))), s = Math.max(.875, 1 - (1 - full) * .14), way = .2;
    const x = D.left + D.width / 2 + (A.left + A.width / 2 - D.left - D.width / 2) * way, y = D.top + D.height / 2 + (A.top + A.height / 2 - D.top - D.height / 2) * way;
    return `translate(${(x - D.width * s / 2 - D.left).toFixed(1)}px,${(y - D.height * s / 2 - D.top).toFixed(1)}px) scale(${s.toFixed(4)})`;
  }
  /** One soft motion (Paul, 29 Sep 01:30, "it doesn't engage smoothly and it looks broken and disjointed"): the window
   *  eases toward its card as it scales a little down and fades, and its backdrop fades in step with it, on one clock
   *  (made in one task: they start on the same frame). The fade is even: the window is light and the rail under it dark,
   *  so an eased fade changed a third of the screen in a frame at its fastest. Back is the very same motion in reverse, landing
   *  on the window exactly as it was (nothing is left on it). With reduced motion only the short fade. Transform and
   *  opacity only: the window is laid out once, at its own size, and never reflows on the way. */
  function aside(t, k, back, ms) {
    const d = k.d, g = !!t.gentle, o = { duration: ms, easing: g ? "ease" : "linear", fill: "forwards", direction: back ? "reverse" : "normal" };
    const fade = [{ opacity: 1 }, { opacity: 0 }];
    return [
      d.animate(fade, o),
      g ? null : d.animate([{ transform: "none" }, { transform: asideTf(boxOf(d), aimOf(k)) }], Object.assign({}, o, { easing: ASIDE })),
      tryDo(() => d.animate(fade, Object.assign({ pseudoElement: "::backdrop" }, o)))
    ].filter(Boolean);
  }
  function tuck(t, d, o) {
    const R = rectOf(o.from) || (o.from && o.from.rect) || { left: innerWidth / 2 - 20, top: innerHeight / 2 - 12, width: 40, height: 24 };
    const k = { d, R, from: o.from, inside: !!(o.from && o.from.nodeType && d.contains(o.from)), home: o.home && o.home.nodeType ? o.home : null, focus: doc.activeElement, st: {} };
    for (const p of ["transformOrigin", "willChange", "visibility"]) k.st[p] = d.style[p];
    // where the design lifts off: the button pressed, as it stood
    k.spot = t.node("tourSpot"); Object.assign(k.spot.style, { width: R.width + "px", height: R.height + "px", transform: `translate(${R.left}px,${R.top}px)`, opacity: 0 });
    over(true); mark("tuck"); d._tourHeld = true;
    Object.assign(d.style, { transformOrigin: "0 0", willChange: "transform,opacity" });
    k.anims = aside(t, k, false, t.gentle ? GENTLE.fade : TIME.aside);
    // (out of sight once gone, nothing of it drawn under the flight: hidden once the frames drawn off the main thread,
    // which come a little after it has finished here, are in)
    k.anims[0].finished.then(() => setTimeout(() => { if (!k.returning && !k.ready) d.style.visibility = "hidden"; }, LAG), () => {});
    return k;
  }
  /** Before it comes back: what changed meanwhile is drawn (the order window: its Send to Sheet gone) and the window
   *  is laid out and painted, still held out of sight (the step aside holds it at nothing), so its return starts on a
   *  window ready to draw: drawn only as it came back, its first frames came late and it jumped in. */
  function ready(k) {
    if (!k || k.ready) return; k.ready = true;
    const d = k.d; if (!d.isConnected || !d.open) return;
    tryDo(() => d.dispatchEvent(new CustomEvent("tour:back")));
    d.style.visibility = k.st.visibility;
  }
  /** The window back as it was: the step aside reversed (fast: a skip, a tab picked; with reduced motion, faded in). */
  async function back(t, fast) {
    const k = t.win; if (!k) return; t.win = null; k.returning = true;
    const d = k.d, ms = t.gentle ? GENTLE.fade : fast ? 320 : TIME.aside, drop = () => { for (const a of k.anims) tryDo(() => a.cancel()); };
    if (d.isConnected && d.open) {
      ready(k); mark("back");
      // (the reverse is set going before the step aside lets go: no frame between them)
      const run = aside(t, k, true, ms);
      drop();
      await Promise.race([Promise.all(run.map(a => a.finished.catch(() => {}))), new Promise(r => setTimeout(r, ms + 400))]);
      // (landed, held there as it ends: the frames drawn off the main thread come a little after it has finished here, and
      // let go at once it jumped the last of the way; it stays on its own layer a moment longer too)
      await new Promise(r => setTimeout(r, LAG));
      for (const a of run) tryDo(() => a.cancel());
    } else drop();
    for (const [p, v] of Object.entries(k.st)) if (p !== "willChange") d.style[p] = v;
    const wc = k.st.willChange; setTimeout(() => { if (!d._tourHeld && d.style.willChange === "transform,opacity") d.style.willChange = wc; }, 700);
    d._tourHeld = false;
    // (the tour's layer closing gives the focus back where it was; set again in case it went elsewhere)
    over(false);
    const f = k.focus; if (f && f !== doc.body && f.isConnected && d.contains(f) && f.getClientRects().length && doc.activeElement !== f) tryDo(() => f.focus({ preventScroll: true }));
  }
  /** 5 · home, into the window: it is back as it was, and says, where the button stood, what went where. True when it
   *  said it there; false when the press was not its own (a card's, under it), so home is said on the card as ever. */
  async function backHome(t, o) {
    const k = t.win;
    // "Back to Review" stands its time to be read (the window made ready meanwhile), and goes as it comes back over it
    if (!t.ff && t.cap) { ready(k); await t.hold(); if (!t.ff) unsay(t, t.cap, 360); }
    await back(t, t.ff);
    const d = k && k.d, M = root.Motion; if (!k || !k.inside) return false;
    if (!d || !d.open || !M || !o.words) return true;
    const held = (o.waiting || []).find(w => w.held), words = o.words + (held ? ` · ${held.why}` : ""), ms = held ? 9000 : 7000, R = k.R;
    const at = doc.createElement("i"); at.setAttribute("aria-hidden", "true");
    Object.assign(at.style, { position: "fixed", left: R.left + "px", top: R.top + "px", width: R.width + "px", height: R.height + "px", pointerEvents: "none", visibility: "hidden" });
    d.appendChild(at); setTimeout(() => at.remove(), ms + 2000);
    const n = tryDo(() => M.note(at, { text: words, ms, tone: held ? "bad" : "" })); if (n && d.open) d.appendChild(n);
    return true;
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

  // its styles are in from the start: put in on the first send, they were a sheet new to the page in the middle of it
  // (every style worked out again, and the designs window's closing copy built all the page's styles anew)
  tryDo(() => { if (doc && doc.head) sheet(); });
  root.SendTour = { play, snap, playing: () => !!T, skip: () => { if (T) T.forward(); }, TIME, GENTLE };
})(typeof window !== "undefined" ? window : globalThis);
