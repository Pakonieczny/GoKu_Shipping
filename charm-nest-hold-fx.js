/* Charm Nest · the cinematic Hold and Release (window.OrderHoldFx).
   Paul, 5 Oct 2026: "real-time detailed game-like cinematic step by step animation so the user can see the order moving
   and animating to the next sheet in Nest tab and returning the user to the On Hold tab to continue their work."

   This only shows what the engine does. It is driven by the steps the engine really takes, as the glue pushes them
   (OrderHold.run and OrderHold.release report each step as it completes): there is no canned film that could disagree
   with the data. The engine never waits for it, and it never waits for the engine beyond a small labelled spinner. Steps
   that arrive faster than they can be shown are queued and played in order, a little quicker the longer the queue gets,
   and a safety timer always ends the film and puts the user back on Orders, On hold.

     const fx = OrderHoldFx.playHold(rid, source);       // before the run starts: the sheets are still as they were
     OrderHold.run(rid, { name, onStep: s => fx.push(s) }).then(() => fx.finish());
     await fx.done;                                       // { ok, skipped, timedOut, stalled, steps, ms, error }
     fx.skip();                                           // the same as Esc or the small Skip: the final state at once
     OrderHoldFx.playRelease(rid, source);                // the same shape, for OrderHold.release
     OrderHoldFx.returnToOnHold(rid);                     // the way home (both films end with it); safe to call twice

   source (all optional): the Plan (OrderHold.plan or releasePlan: label, pieces, sheets) or { plan, steps, subscribe,
   promise }. steps are played first (a recorded list); subscribe(fn) is called with each step and may return an
   unsubscribe; promise (the run itself) ends the film when it settles and hands over any step the glue forgot to push
   (its result's steps).

   HOLD    intro: "Putting order … on hold", a small "On hold" tray, and the page dips through paper to the Nest tab
           sheetBegin { sheetId, label }   the sheet is taken into the light (NestFocus when the page has it, else lit here)
           lift { sheetId, poolIds, rects, label }   the order's pieces glow where they lie, lift off one by one, fly to the tray
           removed { removed }             each emptied spot pulses; a dashed outline stays until it is filled
           fillBegin { spots }             "Filling the empty spots"
           fillFrom { toSheetId, fromSheetId, rid, poolIds, source }   the next order's pieces come from the tab of the newer
                                           sheet of the same metal ('newerSheet') or from the card's waiting queue ('waiting')
           fillPlaced { sheetId, rid, poolIds }   they settle where the sheet put them: a ring and a count
           qr { sheetId }                  a quiet "QR label remade" tick
           sheetDone { sheetId, charmCount, density }   "27 pieces · 74% full"
           held { rid }                    "Order … is on hold", then home; done ends the film; error { message, sheetId } says
                                           so and goes home too
   RELEASE intro: the order's card lifts out of On hold, its design rises and flies into a "Next in line" tray as the Nest
           tab comes in; queued { front } "First in line, ahead of new orders"; target { sheetId, label, spot, newSheet };
           flight { toSheetId, poolIds } the pieces fly from the tray onto the sheet; placed settles them with a gentle
           pulse; qr; released { rid }; done. Home: Orders, On hold, with a note that says where the order went.

   Where things are. rect (lift.rects, target.spot): { x, y, w, h } in sheet points from the sheet's top-left, or fractions
   0-1 of the sheet's picture, or { left, top, width, height } in screen px. A piece is placed by its own drawing on the
   page when the page holds it, else by the [data-pool-id] element in its card, else by the step's rect, else the middle of
   the card. The Plan's pieces are read at once by playHold (before the engine changes anything), so a lift that is played
   after the engine has removed the pieces still lifts off the right spot.

   Calm: slower than a click, quieter than the Send to Sheet tour. Only transform, opacity and clip-path animate. The one
   colour it adds is the orange the Hold button uses (--holdOrange, else the app's --clay). prefers-reduced-motion: the
   captions still step through, nothing flies. Esc or Skip: the final state at once. A tab picked ends the film where the
   person went. The layer never takes a press, and nothing stays on the page when the film ends. */
(function (root) {
  "use strict";
  const doc = root.document;
  if (!doc) return;
  const SOFT = "cubic-bezier(.42,0,.18,1)", OUT = "cubic-bezier(.4,0,1,1)", IN = "cubic-bezier(0,0,.2,1)", GLIDE = "cubic-bezier(.4,.05,.25,1)";
  /** How long each part takes (ms), before the pace of a long queue shortens it. */
  const T = Object.freeze({ cap: 260, read: 1050, glow: 560, gap: 240, fly: 1050, spot: 700, tick: 900, veilIn: 170, veilOut: 340, rise: 700, settle: 460, stall: 30000, grace: 900 });
  const GENTLE = Object.freeze({ cap: 140, read: 950 });
  const LOG = [];
  const ev = (type, data) => { LOG.push(Object.assign({ t: Math.round(performance.now()), ev: type }, data || {})); if (LOG.length > 600) LOG.splice(0, LOG.length - 600); };
  const tryDo = f => { try { return f(); } catch (_) { return null; } };
  const motion = () => root.Motion || null;
  const reduced = () => { const m = motion(); if (m && typeof m.reduced === "function") { try { return !!m.reduced(); } catch (_) {} } try { return root.matchMedia("(prefers-reduced-motion: reduce)").matches; } catch (_) { return false; } };
  const el = (tag, cls, html) => { const n = doc.createElement(tag); if (cls) n.className = cls; if (html != null) n.innerHTML = html; return n; };
  const rectOf = e => { const r = e && e.isConnected ? e.getBoundingClientRect() : null; return r && r.width > 0 && r.height > 0 ? r : null; };
  const rectNow = n => { const r = rectOf(n); return r ? { left: r.left, top: r.top, width: r.width, height: r.height } : null; };
  const onScreen = r => !!r && r.top + r.height > 0 && r.top < innerHeight && r.left + r.width > 0 && r.left < innerWidth;   // (a DOMRect or a { left, top, width, height })
  const mid = r => ({ x: r.left + r.width / 2, y: r.top + r.height / 2 });
  const clamp = (x, a, b) => Math.max(a, Math.min(b, x));
  const pl = (n, w) => `${n} ${w}${n === 1 ? "" : "s"}`;
  const esc = s => (root.CSS && root.CSS.escape ? root.CSS.escape(String(s)) : String(s).replace(/["\\]/g, "\\$&"));
  const frame = () => new Promise(r => { let d = false; const f = () => { if (!d) { d = true; r(); } }; requestAnimationFrame(f); setTimeout(f, 120); });   // (a hidden tab draws no frames: never wait for one)
  const own = (o, k) => Object.prototype.hasOwnProperty.call(o, k);

  const CSS = `
#hfxLayer{--hfxOrange:var(--holdOrange,var(--warn,#a2591c));--hfxSoft:var(--holdOrangeSoft,color-mix(in srgb,var(--hfxOrange) 11%,#fffefb));--hfxInk:color-mix(in srgb,var(--hfxOrange) 72%,#000);position:fixed;inset:0;z-index:139;pointer-events:none;overflow:hidden;contain:strict}
#hfxLayer>*{position:fixed}
#hfxLayer .hfxLive{position:absolute;width:1px;height:1px;overflow:hidden;clip-path:inset(50%);white-space:nowrap}
.hfxVeil{background:var(--paper,#f3f0ea);opacity:0}
.hfxCap{display:flex;align-items:center;gap:10px;max-width:min(580px,calc(100vw - 32px));padding:9px 18px 9px 14px;border-radius:999px;background:rgba(255,254,250,.97);border:1px solid var(--goldLine,#e3d3a6);box-shadow:0 12px 32px rgba(30,24,16,.14),0 2px 6px rgba(30,24,16,.06);color:var(--ink,#1c1a17);white-space:nowrap;transform:translate(-50%,0);will-change:transform,opacity}
.hfxCap>i{flex:0 0 auto;width:10px;height:10px;border-radius:50%;background:var(--hfxOrange);box-shadow:0 0 0 3px color-mix(in srgb,var(--hfxOrange) 16%,transparent)}
.hfxCap>b{font:600 16px/1.2 var(--serif,Georgia,serif);letter-spacing:.01em;min-width:0;overflow:hidden;text-overflow:ellipsis}
.hfxCap>small{font:600 12px/1.2 var(--sans,system-ui,sans-serif);color:var(--ink70,#5b554c);padding-left:11px;border-left:1px solid var(--goldLine,#e3d3a6);min-width:0;max-width:min(300px,32vw);overflow:hidden;text-overflow:ellipsis}
.hfxCap.bad{border-color:var(--hfxOrange)}.hfxCap.bad>b{color:var(--hfxInk)}
.hfxCap.wait>i{background:transparent;box-shadow:none;border:2px solid color-mix(in srgb,var(--hfxOrange) 25%,transparent);border-top-color:var(--hfxOrange);animation:hfxSpin .9s linear infinite}
@keyframes hfxSpin{to{transform:rotate(360deg)}}
.hfxSkip{pointer-events:auto;display:inline-flex;align-items:center;gap:7px;border:1px solid var(--line,#e4ddd0);background:rgba(255,254,250,.95);color:var(--ink70,#5b554c);font:600 12px/1 var(--sans,system-ui,sans-serif);padding:8px 13px;border-radius:999px;cursor:pointer;box-shadow:0 6px 18px rgba(30,24,16,.1);will-change:opacity}
.hfxSkip:hover,.hfxSkip:focus-visible{color:var(--ink,#1c1a17);border-color:var(--goldLine,#e3d3a6);outline:none}
.hfxSkip kbd{font:600 10px var(--mono,ui-monospace,monospace);color:var(--ink45,#938c80);border:1px solid var(--line,#e4ddd0);border-radius:5px;padding:2px 5px}
.hfxMark{display:flex;align-items:center;gap:8px;padding:7px 10px 7px 11px;border-radius:12px;background:var(--hfxSoft);border:1px solid var(--hfxOrange);color:var(--hfxInk);font:700 12px/1 var(--sans,system-ui,sans-serif);letter-spacing:.02em;box-shadow:0 8px 22px rgba(30,24,16,.12);will-change:transform,opacity}
.hfxMark svg{width:17px;height:17px;flex:0 0 auto}
.hfxMark em{font:800 12px/1 var(--mono,ui-monospace,monospace);font-style:normal;min-width:38px;box-sizing:border-box;text-align:center;background:var(--hfxOrange);color:#fff;border-radius:999px;padding:3px 7px}
.hfxMark em:empty{visibility:hidden}   /* (it keeps its room: the marker never shifts under a flight) */
.hfxMark.mGot{background-color:var(--hfxSoft)!important;box-shadow:0 8px 22px rgba(30,24,16,.12)!important;transition:none!important}
.hfxGlow{border-radius:9px;box-shadow:0 0 0 2px var(--hfxOrange),0 0 24px 4px color-mix(in srgb,var(--hfxOrange) 32%,transparent);opacity:0;will-change:transform,opacity}
.hfxSpot{box-sizing:border-box;border-radius:9px;border:1.5px dashed var(--hfxOrange);background:color-mix(in srgb,var(--hfxOrange) 6%,transparent);opacity:0;will-change:transform,opacity}
.hfxRing{border-radius:50%;border:2px solid var(--gold,#a9823f);box-shadow:0 0 14px rgba(184,137,58,.45);will-change:transform,opacity}
.hfxRing.pill{border-radius:999px}
.hfxChip{will-change:transform,opacity}
.hfxCard{display:block;width:100%;height:100%}
.hfxBlank{display:block;width:100%;height:100%;box-sizing:border-box;border-radius:9px;background:var(--goldSoft,#f0e6cd);border:1.5px solid var(--gold2,#caa861)}
.hfxFace{display:block;box-sizing:border-box;width:100%;height:100%;border-radius:50%;background:radial-gradient(circle at 50% 50%,#fff 0,#fff 64%,#fbf6ea 80%,#efe2c2 100%);border:1.5px solid var(--gold2,#caa861);box-shadow:0 8px 18px rgba(60,40,12,.2);overflow:hidden;position:relative}
.hfxFace>img,.hfxFace>canvas{position:absolute;left:17%;top:17%;width:66%;height:66%;object-fit:contain}
.hfxPiece{filter:drop-shadow(0 6px 8px rgba(60,40,12,.28))}
.mGhost[data-hfx^="lift"]>.mLift{display:none}
.hfxCoin{width:56px;height:56px;margin:-28px 0 0 -28px;left:0;top:0;will-change:transform,opacity}
.hfxStat{display:flex;flex-direction:column;align-items:flex-end;gap:5px}
.hfxRow{display:inline-flex;align-items:center;gap:6px;padding:4px 10px;border-radius:999px;background:rgba(255,254,250,.96);border:1px solid var(--line,#e4ddd0);box-shadow:0 5px 14px rgba(30,24,16,.1);font:600 11.5px/1.2 var(--sans,system-ui,sans-serif);color:var(--ink70,#5b554c);white-space:nowrap;will-change:transform,opacity}
.hfxRow svg{width:12px;height:12px;flex:0 0 auto;color:var(--hfxOrange)}
.hfxRow b{font-weight:700;color:var(--ink,#1c1a17)}
.hfxRow .tick{display:inline-block;color:var(--hfxOrange);clip-path:inset(0 0 0 0)}
.hfxNew{padding:3px 10px;border-radius:999px;border:1px dashed var(--hfxOrange);background:var(--hfxSoft);color:var(--hfxInk);font:700 11px/1.2 var(--sans,system-ui,sans-serif);will-change:transform,opacity}
.hfxLit{z-index:4}
.hfxDim>.sheetCard:not(.hfxLit){opacity:.42;transition:opacity .4s ease}
@media (prefers-reduced-motion:reduce){.hfxCap.wait>i{animation:none}.hfxDim>.sheetCard:not(.hfxLit){transition:none}}`;
  function sheet() { if (doc.getElementById("hfxCss")) return; const s = el("style"); s.id = "hfxCss"; s.textContent = CSS; (doc.head || doc.documentElement).appendChild(s); }

  /* ── the layer: one, shared by a film and by the way home, taken down with the last of them ── */
  let layerN = null, leases = 0;
  function lease() {
    sheet();
    if (!layerN || !layerN.isConnected) { layerN = el("div"); layerN.id = "hfxLayer"; const l = el("div", "hfxLive"); l.setAttribute("aria-live", "polite"); l.setAttribute("role", "status"); layerN.appendChild(l); doc.body.appendChild(layerN); }
    leases++; let off = false;
    return { layer: layerN, release() { if (off) return; off = true; if (--leases <= 0) { leases = 0; if (layerN) layerN.remove(); layerN = null; } } };
  }
  function stageBox() {
    const s = rectOf(doc.getElementById("stage")) || rectOf(doc.querySelector(".stage"));
    if (s) return { left: s.left, top: Math.max(0, s.top), right: s.right, bottom: s.bottom, width: s.width, height: s.height };
    const tb = rectOf(doc.querySelector(".topbar")), top = tb ? tb.bottom : 0;
    return { left: 0, top, right: innerWidth, bottom: innerHeight, width: innerWidth, height: Math.max(0, innerHeight - top) };
  }

  /* ── the app's own pieces ── */
  const CN = () => root.CN || null;
  const modeNow = () => tryDo(() => CN().S.mode) || tryDo(() => doc.querySelector(".topbar button.on[data-mode], #modeSeg button.on").dataset.mode) || "";
  function setMode(m) {
    const c = CN(); if (c && typeof c.setMode === "function") { try { c.setMode(m); return true; } catch (_) {} }
    if (typeof root.setMode === "function") { try { root.setMode(m); return true; } catch (_) {} }
    const b = doc.querySelector(`.topbar [data-mode="${m}"], #modeSeg [data-mode="${m}"]`); if (b) { b.click(); return true; } return false;
  }
  const tabBtn = m => rectOf(doc.querySelector(`#modeSeg [data-mode="${m}"]`) || doc.querySelector(`.topbar [data-mode="${m}"]`));
  function showHold() {
    const O = root.Orders;
    if (O && typeof O.showPile === "function") { try { O.showPile("hold", ""); return true; } catch (_) {} }
    const b = doc.querySelector('#ordChips [data-pile="hold"]'); if (b) { b.click(); return true; } return false;
  }
  const holdCard = rid => {
    const all = [...doc.querySelectorAll("#ordItems [data-rid], #ordBody [data-rid]")].filter(n => n.dataset.rid === String(rid));
    return all.find(n => n.querySelector(".relHold")) || all[0] || null;
  };

  /* ── where things are on a sheet ── */
  const L = () => (root.OrderHoldFx && root.OrderHoldFx.locate) || {};
  const G = {
    pages: () => tryDo(() => (typeof allSheets === "function" ? allSheets() : null)) || [],   // eslint-disable-line no-undef
    page: id => id == null ? null : G.pages().find(p => String(p.sheetId) === String(id) || (p.recalled && String(p.recalled.id) === String(id))) || null,
    metalOf(label) {
      const m = /^(.+?)\s+Sheet\b/i.exec(String(label || "")); if (!m) return null;
      const tags = tryDo(() => (typeof METAL_TAG !== "undefined" ? METAL_TAG : null)) || {}; // eslint-disable-line no-undef
      return Object.keys(tags).find(k => String(tags[k]).toLowerCase() === m[1].trim().toLowerCase()) || null;
    },
    /** The card a sheet is shown on: the page's own card, an element that names it, or its metal's card. */
    card(sheetId, hint) {
      const custom = tryDo(() => L().card && L().card(sheetId, hint)); if (custom && custom.isConnected) return custom;
      const pg = G.page(sheetId);
      if (pg) { const prim = tryDo(() => CN().S.sheets[pg.metal]), n = (prim && prim.cardEl) || pg.el; if (n && n.isConnected) return n; }
      const named = sheetId != null ? tryDo(() => doc.querySelector(`[data-sheet-id="${esc(sheetId)}"]`)) : null;
      if (named) return named.closest(".sheetCard") || named;
      const metal = (hint && hint.metal) || (pg && pg.metal) || G.metalOf(hint && hint.label);
      return metal ? doc.querySelector(`.sheetCard[data-m="${esc(metal)}"]`) : null;
    },
    cv: card => card ? (card.querySelector('[data-r="canvas"]') || card.querySelector("canvas") || card) : null,
    /** The tab of a sheet on its metal's card: where a newer sheet of the same metal waits. */
    tab(card, sheetId) {
      const custom = tryDo(() => L().tab && L().tab(card, sheetId)); if (custom) return custom;
      if (!card) return null;
      const byId = sheetId != null ? tryDo(() => card.querySelector(`[data-r="tabs"] [data-sheet-id="${esc(sheetId)}"]`)) : null; if (byId) return byId;
      const pg = G.page(sheetId), pages = pg && tryDo(() => CN().S.sheets[pg.metal].pages), i = pages ? pages.indexOf(pg) : -1;
      return (i >= 0 && card.querySelector(`[data-r="tabs"] button[data-i="${i}"]`)) || card.querySelector('[data-r="tabs"]') || card.querySelector(".shHead");
    },
    queue: card => card ? (card.querySelector('[data-r="queue"]') || card.querySelector(".shHead")) : null,
    ref(pg, cv) {
      if (cv && cv.tagName === "CANVAS" && cv.width > 0 && cv.height > 0) return { w: cv.width, h: cv.height };
      const v = pg && pg._view, st = pg && tryDo(() => stockFor(pg.metal, pg));   // eslint-disable-line no-undef
      return v && st ? { w: 2 * v.R + st.wPt * v.k, h: 2 * v.R + st.hPt * v.k } : null;
    },
    /** A rect in sheet points (from the sheet's top-left) as fractions of the sheet's picture. */
    uvOfPt(pg, cv, r, size) {
      const v = pg && pg._view, ref = G.ref(pg, cv);
      if (v && ref) return { x: (v.R + r.x * v.k) / ref.w, y: (v.R + r.y * v.k) / ref.h, w: r.w * v.k / ref.w, h: r.h * v.k / ref.h };
      const st = (size && size.wPt > 0 && size.hPt > 0 ? size : null) || (pg && tryDo(() => stockFor(pg.metal, pg))); // eslint-disable-line no-undef
      return st && st.wPt > 0 ? { x: r.x / st.wPt, y: r.y / st.hPt, w: r.w / st.wPt, h: r.h / st.hPt } : null;
    },
    /** A rect a step carries, as fractions of the sheet's picture (null when it cannot be placed). */
    uvOfStep(sheetId, card, r, size) {
      if (!r || typeof r !== "object") return null;
      const cv = G.cv(card), cr = rectOf(cv);
      if (r.left != null && r.top != null && (r.width != null || r.w != null)) {
        const w = r.width != null ? r.width : r.w, h = r.height != null ? r.height : r.h;
        return cr ? { x: (r.left - cr.left) / cr.width, y: (r.top - cr.top) / cr.height, w: w / cr.width, h: h / cr.height } : null;
      }
      const x = +r.x, y = +r.y, w = +(r.w != null ? r.w : r.width), h = +(r.h != null ? r.h : r.height);
      if (![x, y, w, h].every(Number.isFinite) || !(w > 0) || !(h > 0)) return null;
      const pt = r.space === "pt" || r.cx != null || r.cy != null;   // (the engine's rects carry their centre: sheet points)
      if (r.space === "uv" || (!pt && x >= 0 && y >= 0 && w <= 1 && h <= 1 && x + w <= 1.0001 && y + h <= 1.0001)) return { x, y, w, h };
      return G.uvOfPt(G.page(sheetId), cv, { x, y, w, h }, size);
    },
    screen(card, uv) {
      const cr = rectOf(G.cv(card)); if (!cr || !uv) return null;
      return { left: cr.left + uv.x * cr.width, top: cr.top + uv.y * cr.height, width: Math.max(8, uv.w * cr.width), height: Math.max(8, uv.h * cr.height) };
    },
    /** Pieces as the page holds them now: where each lies (fractions of the picture) and a picture of it alone. */
    data(sheetId, ids, card) {
      const out = new Map(), pg = G.page(sheetId); if (!pg || !pg.charms || !pg.placements || !pg._view) return out;
      const cv = G.cv(card);
      for (const id of ids) {
        const c = pg.charms.find(x => String(x.poolId) === String(id)), p = c && pg.placements.find(x => x.id === c.id); if (!c || !p) continue;
        const sc = p.scale || 1, w = (c.widthPt || 0) * sc, h = (c.heightPt || 0) * sc, a = (+p.angle || 0) * Math.PI / 180;
        const bw = Math.max(4, Math.abs(w * Math.cos(a)) + Math.abs(h * Math.sin(a))), bh = Math.max(4, Math.abs(w * Math.sin(a)) + Math.abs(h * Math.cos(a)));
        const uv = G.uvOfPt(pg, cv, { x: p.cxPt - bw / 2, y: p.cyPt - bh / 2, w: bw, h: bh });
        if (uv) out.set(String(id), { uv, vis: tryDo(() => pieceBitmap(c, p, bw, bh)) });
      }
      return out;
    },
    /** Pieces as the card's own elements show them ([data-pool-id]), measured now. */
    dom(card, ids) {
      const out = new Map(), cr = rectOf(G.cv(card)); if (!card || !cr) return out;
      for (const id of ids) {
        const n = tryDo(() => card.querySelector(`[data-pool-id="${esc(id)}"]`)), r = rectOf(n); if (!r) continue;
        const c = n.cloneNode(true); c.removeAttribute("id"); Object.assign(c.style, { position: "relative", left: "0", top: "0", width: "100%", height: "100%", margin: "0", transform: "none" });
        const box = el("div", "hfxCard"); box.appendChild(c);
        out.set(String(id), { uv: { x: (r.left - cr.left) / cr.width, y: (r.top - cr.top) / cr.height, w: r.width / cr.width, h: r.height / cr.height }, vis: box });
      }
      return out;
    },
    /** Where pieces lie now: the page's drawing, else the card's elements. */
    where(sheetId, ids, card) {
      const m = G.data(sheetId, ids, card);
      for (const [k, v] of G.dom(card, ids.filter(id => !m.has(String(id))))) m.set(k, v);
      return m;
    }
  };
  /** A piece alone on a clear ground, as the sheet draws it (its outline and its lines, turned and scaled as it lies). */
  function pieceBitmap(c, p, bw, bh) {
    const P = root.CharmNestPDF; if (!P || !c || !c.outline || !c.centerPt) return null;
    const q = clamp(72 / Math.max(bw, bh), 1, 4) * Math.min(2, root.devicePixelRatio || 1), cw = Math.ceil(bw * q) + 6, ch = Math.ceil(bh * q) + 6;
    if (cw * ch > 640 * 640) return null;
    const cv = el("canvas"); cv.width = cw; cv.height = ch; cv.style.cssText = "display:block;width:100%;height:100%";
    const g = cv.getContext("2d"), cx = c.centerPt[0], cy = c.centerPt[1], tx = (x, y) => [(x - cx) * q, (cy - y) * q];
    g.translate(cw / 2, ch / 2); g.rotate(((p && +p.angle) || 0) * Math.PI / 180); if (p && p.scale) g.scale(p.scale, p.scale);
    g.fillStyle = "rgba(200,162,78,.16)"; g.beginPath(); P.pathToCanvas(g, c.outline, tx);
    for (const m of (typeof P.cutLinesOf === "function" ? P.cutLinesOf(c) : [])) P.pathToCanvas(g, m, tx);
    g.fill("evenodd"); P.drawCharm(g, c, tx, q);
    const box = el("div", "hfxCard"); box.appendChild(cv); return box;
  }
  /** What a piece coming in looks like: its drawing, else its design on a disc, else a plain disc. */
  function faceFor(poolId, url) {
    const c = tryDo(() => root.Pool && root.Pool.charmOf && root.Pool.charmOf(poolId));
    if (c && c.outline && c.widthPt) { const b = tryDo(() => pieceBitmap(c, null, c.widthPt, c.heightPt)); if (b) return b; }
    const f = el("div", "hfxFace");
    if (url) { const im = new Image(); im.alt = ""; im.decoding = "sync"; im.src = url; f.appendChild(im); }
    return f;
  }
  /** A gentle arc from a to b (points) as keyframes of transform and opacity. */
  function arc(a, b, o = {}) {
    const dx = b.x - a.x, dy = b.y - a.y, d = Math.hypot(dx, dy) || 1, bend = Math.min(120, 30 + d * .1) * (dy <= 0 ? 1 : -.4);
    const s0 = o.s0 == null ? 1 : o.s0, s1 = o.s1 == null ? 1 : o.s1, o1 = o.o1 == null ? 1 : o.o1;
    const at = (t, s, op, lift, offset) => ({ offset, opacity: op, transform: `translate(${a.x + dx * t}px,${a.y + dy * t - bend * Math.sin(Math.PI * t) - lift}px) scale(${s})` });
    return [at(0, s0, 1, 0, 0), at(.2, s0 * 1.03, 1, 8, .2), at(.62, (s0 + s1) / 2, 1, 0, .62), at(.92, s1 * 1.05, Math.max(o1, .85), 0, .92), at(1, s1, o1, 0, 1)];
  }

  /* ═════ what both the film and the way home stand on: a layer, time that skipping ends, and a few shapes ═════ */
  class Stage {
    constructor() {
      this.reduced = reduced(); this.ff = false; this.wakes = new Set(); this.anims = new Set(); this.nodes = new Set(); this.ghosts = new Set();
      this.lease = lease(); this.layer = this.lease.layer;
    }
    /* time that ends at once when the film is skipped */
    sleep(ms) { if (this.ff || !(ms > 0)) return Promise.resolve(); return new Promise(res => { const w = () => { clearTimeout(h); this.wakes.delete(w); res(); }, h = setTimeout(w, ms); this.wakes.add(w); }); }
    until(p, ms) { return Promise.race([Promise.resolve(p).catch(() => null), this.sleep(ms)]); }
    ms(x) { return x; }
    anim(node, frames, opts) {
      if (!node || !node.isConnected || this.ff) return Promise.resolve();
      const a = node.animate(frames, Object.assign({ fill: "both" }, opts)); this.anims.add(a);
      const cap = (opts && (opts.duration || 0) + (opts.delay || 0) || 0) + 800;   // (a hidden tab does not advance animations: the film goes on)
      return Promise.race([a.finished.catch(() => {}), new Promise(r => setTimeout(r, cap))]).then(() => { this.anims.delete(a); });
    }
    node(cls) { const n = el("div", cls); this.layer.appendChild(n); this.nodes.add(n); return n; }
    drop(n) { if (n) { this.nodes.delete(n); n.remove(); } }
    fadeOut(n, ms = 320) { if (!n || !n.isConnected) return; if (this.ff || this.reduced) return this.drop(n); this.anim(n, [{ opacity: +getComputedStyle(n).opacity || 1 }, { opacity: 0 }], { duration: ms, easing: "ease" }).then(() => this.drop(n)); }
    /** The landing ring (the Send to Sheet tour's, in the app's gold). pill: round a button. */
    ring(r, pill) {
      if (!r || this.reduced || this.ff) return;
      const d = Math.max(r.width, r.height) + 10, w = pill ? r.width + 8 : d, h = pill ? r.height + 8 : d, n = this.node("hfxRing" + (pill ? " pill" : ""));
      Object.assign(n.style, { left: r.left + r.width / 2 - w / 2 + "px", top: r.top + r.height / 2 - h / 2 + "px", width: w + "px", height: h + "px" });
      const sc = k => pill ? `scale(${(1 + k * 2 / w).toFixed(4)},${(1 + k * 2 / h).toFixed(4)})` : `scale(${k})`, s = pill ? [-2, 3, 12] : [.6, 1.08, 1.5];
      this.anim(n, [{ transform: sc(s[0]), opacity: .9 }, { transform: sc(s[1]), opacity: .75, offset: .45 }, { transform: sc(s[2]), opacity: 0 }], { duration: pill ? 900 : 700, easing: "ease-out" }).then(() => this.drop(n));
    }
    async settleMode(mode) { for (let i = 0; i < 25 && modeNow() !== mode && !this.ff; i++) await this.sleep(40); }
    /** The page dips through paper while the tab changes: opacity only, the screen is never bare. after: runs once the tab is in. */
    async dipTo(mode, after) {
      if (modeNow() === mode || this.reduced || this.ff) { if (modeNow() !== mode) { setMode(mode); await this.settleMode(mode); } if (after) await after(); return; }
      const s = stageBox(), v = this.node("hfxVeil"); Object.assign(v.style, { left: s.left + "px", top: s.top + "px", width: s.width + "px", height: s.height + "px" });
      this.layer.insertBefore(v, this.layer.firstChild.nextSibling);   // (under everything else the layer shows)
      await this.anim(v, [{ opacity: 0 }, { opacity: 1 }], { duration: T.veilIn, easing: OUT });
      ev("switch", { to: mode }); setMode(mode); await this.settleMode(mode); await frame(); await frame();
      if (after) await after();
      await this.anim(v, [{ opacity: 1 }, { opacity: 0 }], { duration: T.veilOut, easing: IN }); this.drop(v);
    }
    /** The chip standing where a piece lands, hidden until its copy arrives: a fixed box at r (screen) holding the piece's picture. */
    chip(r, vis, key) {
      const c = this.node("hfxChip"); Object.assign(c.style, { left: r.left + "px", top: r.top + "px", width: r.width + "px", height: r.height + "px", visibility: "hidden" }); c.dataset.hfx = key || "";
      const inner = el("div", "hfxCard hfxPiece"); inner.appendChild(vis || el("div", "hfxBlank")); c.appendChild(inner); return { c, inner };
    }
    /** A copy of the chip's picture arcs in from a place and settles on the chip, which then shows. */
    async flyInto(fromRect, chip, inner, ms) {
      const M = motion(), r = rectNow(chip);
      if (!r || this.reduced || !M || !fromRect || !onScreen(fromRect) || this.ff) { chip.style.visibility = ""; if (!this.ff) await this.anim(chip, [{ opacity: 0 }, { opacity: 1 }], { duration: this.reduced ? GENTLE.cap : 300 }); return; }
      const g = M.ghost(inner, r, 0, doc.body); this.ghosts.add(g); g.dataset.hfx = chip.dataset.hfx || "";
      chip.style.visibility = "hidden";
      const fm = mid(fromRect), to = mid(r), dx = fm.x - to.x, dy = fm.y - to.y, s = clamp(Math.min(fromRect.height * 1.2 / r.height, fromRect.width * 1.2 / r.width), .06, .5);
      ev("flight", { key: chip.dataset.hfx || "", from: { x: Math.round(fm.x), y: Math.round(fm.y) }, to: { x: Math.round(to.x), y: Math.round(to.y) } });
      const a = g.animate([
        { transform: `translate(${dx}px,${dy}px) scale(${s})`, opacity: 0 },
        { transform: `translate(${dx * .55}px,${dy * .55 - 36}px) scale(${(1 + s) / 2.2})`, opacity: 1, offset: .4 },
        { transform: "translate(0,-6px) scale(1.04)", opacity: 1, offset: .84 },
        { transform: "none", opacity: 1 }], { duration: ms, easing: GLIDE, fill: "forwards" });
      this.anims.add(a); await this.until(a.finished.catch(() => {}), ms + 400); this.anims.delete(a);
      chip.style.visibility = ""; g.remove(); this.ghosts.delete(g);
    }
    /** A chip moved to where its piece truly lies (it slides there once, on transform alone). */
    moveChip(c, r) {
      const o = rectNow(c); if (!o || !r) return Promise.resolve();
      Object.assign(c.style, { left: r.left + "px", top: r.top + "px", width: r.width + "px", height: r.height + "px", transformOrigin: "0 0" });
      if (this.reduced || this.ff || (Math.abs(o.left - r.left) < 3 && Math.abs(o.top - r.top) < 3)) return Promise.resolve();
      return this.anim(c, [{ transform: `translate(${o.left - r.left}px,${o.top - r.top}px) scale(${o.width / r.width},${o.height / r.height})` }, { transform: "none" }], { duration: 360, easing: SOFT, fill: "none" });
    }
    pop(c) { if (this.reduced || this.ff || !c) return Promise.resolve(); return this.anim(c, [{ transform: "scale(1)" }, { transform: "scale(1.07)", offset: .4 }, { transform: "scale(1)" }], { duration: T.settle, easing: "ease-out", fill: "none" }); }
    /** Everything this stage put on the page is taken off it. */
    close() {
      this.ff = true; for (const w of [...this.wakes]) w();
      for (const a of [...this.anims]) tryDo(() => a.cancel());
      for (const g of this.ghosts) { for (const a of (g.getAnimations ? g.getAnimations() : [])) tryDo(() => a.cancel()); g.remove(); }
      for (const n of [...this.nodes]) n.remove();
      this.anims.clear(); this.nodes.clear(); this.ghosts.clear(); this.lease.release();
    }
  }

  /* ═════ the film ═════ */
  let cur = null;
  const RET = new Map(), LANDED = new Map();

  class Film extends Stage {
    constructor(kind, rid, source) {
      super();
      Object.assign(this, { kind, rid: String(rid == null ? "" : rid), queue: [], steps: [], closed: false, over: false, terminal: false, nudge: null, t0: performance.now(),
        cache: new Map(), labels: new Map(), spots: new Map(), chips: new Map(), stats: new Map(), focus: null, cap: null, capAt: 0, mark: null, failed: null, stay: false, stalled: false, timedOut: false,
        skipped: false, sheetNo: 0, sheetsN: 0, off: [], unsub: null, filled: 0, fillTot: 0 });
      this.plan = source && typeof source === "object" ? (source.plan || (source.pieces || source.sheets || source.label || source.customer ? source : null)) : null;
      this.api = { push: s => this.push(s), finish: () => this.finish(), skip: () => this.skip(), done: null, rid: this.rid, kind };
      this.api.done = new Promise(res => { this.resolve = res; });
      const opt = source && typeof source === "object" ? source : {};
      this.limit = +opt.safetyMs > 0 ? +opt.safetyMs : kind === "hold" ? 45000 + 25000 * (this.plan && Array.isArray(this.plan.sheets) ? this.plan.sheets.length : 2) : 40000;
      this.stallMs = +opt.stallMs > 0 ? +opt.stallMs : T.stall;
      this.guard = setTimeout(() => this.skip({ timedOut: true }), this.limit);
      this.listen(); this.precapture();
      if (source && typeof source === "object") {
        if (Array.isArray(source.steps)) for (const s of source.steps) this.push(s);
        if (typeof source.subscribe === "function") { const u = tryDo(() => source.subscribe(s => this.push(s))); if (typeof u === "function") this.unsub = u; }
        if (source.promise && typeof source.promise.then === "function") source.promise.then(res => { for (const s of (res && res.steps) || []) if (!this.seen(s)) this.push(s); this.finish(); }, () => this.finish());
      }
      this.run();
    }
    /* what the engine said, in the order it said it */
    seen(s) { return this.steps.some(x => x.type === s.type && x.at === s.at && x.sheetId === s.sheetId && x.toSheetId === s.toSheetId && JSON.stringify(x.poolIds) === JSON.stringify(s.poolIds)); }
    push(step) {
      if (this.over || this.ff || !step || typeof step !== "object" || typeof step.type !== "string") return;
      const s = Object.assign({}, step); this.steps.push(s); this.queue.push(s); ev("push", { type: s.type, sheetId: s.sheetId != null ? s.sheetId : s.toSheetId });
      tryDo(() => this.prefetch(s)); this.wake();
    }
    finish() { if (this.closed) return; this.closed = true; this.wake(); }
    wake() { if (this.nudge) { const n = this.nudge; this.nudge = null; n(); } }
    skip(o = {}) {
      if (this.ff) return; this.ff = true; this.skipped = !o.timedOut && !o.stalled && !o.stay; this.timedOut = !!o.timedOut; this.stalled = !!o.stalled; this.stay = !!o.stay;
      ev("skip", { timedOut: this.timedOut, stalled: this.stalled, stay: this.stay });
      for (const a of [...this.anims]) tryDo(() => a.cancel());
      for (const w of [...this.wakes]) w(); this.wake();
    }
    /* the pace: the longer the queue, the quicker each part */
    k() { const n = this.queue.length; return this.reduced ? 1 : n > 10 ? .45 : n > 6 ? .6 : n > 3 ? .8 : 1; }
    ms(x) { return Math.max(90, Math.round(x * this.k())); }
    /* hold until the caption has stood long enough to be read */
    hold(ms) { return this.sleep(this.capAt + (ms == null ? this.ms(this.reduced ? GENTLE.read : T.read) : ms) - performance.now()); }

    /* ── keys, a tab picked ── */
    listen() {
      const key = e => { if (e.key === "Escape") { e.preventDefault(); e.stopPropagation(); this.skip(); } };
      const down = e => { const t = e.target && e.target.closest && e.target.closest(".topbar [data-mode], #moreMenu [data-mode]"); if (t) this.skip({ stay: true }); };
      const resize = () => this.place();
      addEventListener("keydown", key, true); addEventListener("pointerdown", down, true); addEventListener("resize", resize);
      this.off.push(() => { removeEventListener("keydown", key, true); removeEventListener("pointerdown", down, true); removeEventListener("resize", resize); });
    }

    /* ── the caption, the tray, the Skip: the top of the stage, under the top bar ── */
    place() {
      const s = stageBox(), c = this.cap;
      if (c && c.isConnected) { c.style.left = s.left + s.width / 2 + "px"; c.style.top = s.top + 5 + "px"; }   // (just above the sheets' tab rows)
      const mk = this.mark && this.mark.el.isConnected ? this.mark : null; if (mk) mk.at(s);
      const k = this.skipEl && this.skipEl.isConnected ? this.skipEl : null;
      if (k) {   // Skip stands beside the On hold marker, at the right: the sheets' names, at the left, stay readable
        const m = mk ? mk.el.getBoundingClientRect() : null, w = k.offsetWidth || 80, h = k.offsetHeight || 30;
        k.style.left = Math.max(s.left + 8, m ? m.left - 10 - w : s.right - 16 - w) + "px"; k.style.top = (m ? m.top + (m.height - h) / 2 : s.top + 12) + "px";
      }
    }
    say(main, small, o = {}) {
      if (this.over) return;
      const old = this.cap, n = el("div", "hfxCap" + (o.bad ? " bad" : "")), g = this.reduced;
      n.innerHTML = "<i></i><b></b>" + (small ? "<small></small>" : ""); n.querySelector("b").textContent = main; if (small) n.querySelector("small").textContent = small;
      n.style.opacity = this.ff ? "1" : "0"; this.layer.appendChild(n); this.nodes.add(n); this.cap = n; this.capAt = performance.now() + (old ? 90 : 0); this.capMain = main; this.capSmall = small || "";
      this.place();
      const live = this.layer.querySelector(".hfxLive"); if (live) live.textContent = main + (small ? " \u00b7 " + small : "");
      ev("caption", { main, small: small || "" });
      this.anim(n, g ? [{ opacity: 0 }, { opacity: 1 }] : [{ opacity: 0, transform: "translate(-50%,7px)" }, { opacity: 1, transform: "translate(-50%,0)" }], { duration: g ? GENTLE.cap : T.cap, delay: old ? 90 : 0, easing: SOFT });
      if (old) this.anim(old, [{ opacity: 1 }, { opacity: 0 }], { duration: g ? GENTLE.cap : 220, easing: "ease-in" }).then(() => this.drop(old));
    }
    /** The small words of the caption now standing, changed in place. */
    sub(small) {
      const c = this.cap; if (!c || !c.isConnected) return; let s = c.querySelector("small");
      if (!small) { if (s) s.remove(); } else { if (!s) { s = el("small"); c.appendChild(s); } s.textContent = small; }
      this.capSmall = small || "";
    }
    /** A wait is a small labelled spinner. */
    spin(on) {
      const c = this.cap; if (!c || !c.isConnected || c.classList.contains("wait") === !!on) return;
      c.classList.toggle("wait", !!on); let s = c.querySelector("small");
      if (on) { c._prev = s ? s.textContent : null; if (!s) { s = el("small"); c.appendChild(s); } s.textContent = "working"; }
      else if (s) { if (c._prev == null) s.remove(); else s.textContent = c._prev; }
    }
    tray(label, icon, em) {
      const m = this.node("hfxMark"); m.innerHTML = `${icon || ""}<span></span><em>${em == null ? "" : em}</em>`; m.querySelector("span").textContent = label; m.style.opacity = this.ff ? "1" : "0";
      const api = { el: m, n: 0, label,
        at: st => { const w = m.offsetWidth || 120, narrow = st.width < 820; m.style.left = Math.max(st.left + 8, st.right - 16 - w) + "px"; m.style.top = st.top + (narrow ? 62 : 12) + "px"; },
        add: k => { api.n += k; m.querySelector("em").textContent = String(api.n); this.place(); if (!this.reduced && !this.ff) m.querySelector("em").animate([{ transform: "scale(1)" }, { transform: "scale(1.22)", offset: .35 }, { transform: "scale(1)" }], { duration: 380, easing: "ease-out" }); },
        set: t => { m.querySelector("em").textContent = t; this.place(); },
        done: () => { m.querySelector("em").textContent = "\u2713"; this.place(); } };
      this.mark = api; api.at(stageBox());
      this.anim(m, this.reduced ? [{ opacity: 0 }, { opacity: 1 }] : [{ opacity: 0, transform: "translateY(-8px)" }, { opacity: 1, transform: "none" }], { duration: this.reduced ? GENTLE.cap : 380, easing: IN });
      return api;
    }
    skipPill() {
      const b = el("button", "hfxSkip"); b.type = "button"; b.innerHTML = "Skip <kbd>Esc</kbd>"; b.setAttribute("aria-label", "Skip the animation (Esc)");
      b.onclick = e => { e.stopPropagation(); this.skip(); };
      this.layer.appendChild(b); this.nodes.add(b); this.skipEl = b; this.place();
      this.anim(b, [{ opacity: 0 }, { opacity: 1 }], { duration: 400, delay: 500, easing: IN });
    }

    /* ── sheets in the light ── */
    labelOf(id) {
      if (id == null) return ""; const k = String(id), known = this.labels.get(k); if (known) return known;
      const w = tryDo(() => { const pg = G.page(id), kit = root.SheetWin && root.SheetWin.holdKit; return pg && kit && kit.word ? String(kit.word(pg) || "") : ""; }); if (w) this.labels.set(k, w);
      return w || "";
    }
    /** The card of a sheet, with the sheet taken into the light first when it is not already. */
    async on(sheetId, hint) {
      if (!this.focus || this.focus.key !== String(sheetId) || !this.focus.card || !this.focus.card.isConnected) await this.focusOn(sheetId, this.labelOf(sheetId), hint);
      return this.focus && this.focus.key === String(sheetId) ? this.focus.card : null;
    }
    async focusOn(sheetId, label, hint) {
      const key = String(sheetId);
      if (this.focus && this.focus.key === key && this.focus.card && this.focus.card.isConnected) return this.focus.card;
      if (this.focus && this.focus.key !== key) this.clearSheet(this.focus.key, true);
      this.unfocus(true);
      const NF = root.NestFocus; let h = null;
      if (NF && typeof NF.open === "function" && sheetId != null) h = await this.until(tryDo(() => NF.open(sheetId, { poolIds: [], noCaption: true, ms: this.ms(620), instant: this.reduced, foot: 0, safeMs: 120000, rid: this.rid, metal: hint && hint.metal })), 2600);
      const card = (h && h.card) || G.card(sheetId, Object.assign({}, hint, { label }));
      if (!card) { ev("focus", { sheetId, found: false }); this.focus = { key, card: null, h }; return null; }
      if (!h || !h.card) {   // the page's own light is not here: the card is lit by this
        const host = card.parentElement; card.classList.add("hfxLit"); if (host) host.classList.add("hfxDim");
        const r = card.getBoundingClientRect(); if (r.top < 60 || r.bottom > innerHeight - 10) card.scrollIntoView({ block: "center", behavior: "auto" });
        this.focus = { key, card, h: null, host }; await this.sleep(this.reduced ? 0 : this.ms(380));
      } else this.focus = { key, card, h };
      ev("focus", { sheetId, found: true }); return card;
    }
    unfocus(handoff) {
      const f = this.focus; if (!f) return; this.focus = null;
      if (f.h && typeof f.h.close === "function" && !handoff) tryDo(() => f.h.close());
      if (f.card) f.card.classList.remove("hfxLit"); if (f.host) f.host.classList.remove("hfxDim");
    }
    /** Where each piece lies on a sheet, each with its picture: what was read earlier, else what the page shows now, else the step's rect. */
    items(sheetId, ids, rects, hint) {
      const size = hint && hint.sheet, rectFor = (id, i) => (Array.isArray(rects) && (rects.find(r => r && r.poolId != null && String(r.poolId) === String(id)) || (rects.some(r => r && r.poolId != null) ? null : rects[i]))) || null;
      const card = (this.focus && this.focus.key === String(sheetId) && this.focus.card) || G.card(sheetId, hint), out = [];
      const keep = this.cache.get(String(sheetId)) || new Map(); this.cache.set(String(sheetId), keep);
      const got = G.where(sheetId, ids.filter(id => !keep.has(String(id)) || !keep.get(String(id)).uv), card);
      ids.forEach((id, i) => {
        const k = String(id), prev = keep.get(k); let v = prev && prev.uv ? prev : got.get(k);
        if (v && v.uv) { if (!v.vis && prev && prev.vis) v = { uv: v.uv, vis: prev.vis }; keep.set(k, v); }   // (read from the page: kept, the first read is the truest)
        else v = { uv: G.uvOfStep(sheetId, card, rectFor(id, i), size), vis: (prev && prev.vis) || (v && v.vis) || null };   // (the engine's own rect: used, not kept)
        out.push({ poolId: k, uv: v.uv, vis: v.vis });
      });
      return { card, list: out };
    }
    /** Read before the engine changes anything: the Plan's pieces on their sheets. */
    precapture() {
      const pieces = this.plan && Array.isArray(this.plan.pieces) ? this.plan.pieces : null; if (!pieces || this.kind !== "hold") return;
      for (const s of (this.plan.sheets || [])) if (s && s.sheetId != null && s.label) this.labels.set(String(s.sheetId), s.label);
      const by = new Map();
      for (const p of pieces) if (p && p.sheetId != null && p.poolId != null && (!p.state || p.state === "onSheet")) {
        const id = String(p.sheetId); if (!by.has(id)) by.set(id, []); by.get(id).push(p.poolId); if (p.sheetLabel) this.labels.set(id, p.sheetLabel);
      }
      for (const [id, ids] of by) tryDo(() => { const keep = this.cache.get(id) || new Map(); this.cache.set(id, keep); for (const [pid, v] of G.where(id, ids, G.card(id, {}))) keep.set(pid, v); });
      ev("precapture", { sheets: by.size });
    }
    prefetch(s) {
      if (s.sheetId != null && s.label) this.labels.set(String(s.sheetId), s.label);
      if (s.type === "lift" && Array.isArray(s.poolIds)) this.items(s.sheetId, s.poolIds, s.rects, s);
    }
    /** Quiet readouts on a sheet's picture, one row each, at its top-right corner. */
    row(sheetId, html, key) {
      const card = (this.focus && this.focus.key === String(sheetId) && this.focus.card) || G.card(sheetId, {}), cr = rectOf(card && G.cv(card)); if (!cr) return null;
      let st = this.stats.get(String(sheetId));
      if (!st || !st.box.isConnected) { st = { box: this.node("hfxStat"), rows: new Map() }; this.stats.set(String(sheetId), st); }
      const w = 230; Object.assign(st.box.style, { left: Math.max(8, cr.right - 10 - w) + "px", top: cr.top + 10 + "px", width: w + "px" });
      let r = st.rows.get(key);
      if (!r) { r = el("span", "hfxRow"); st.box.appendChild(r); st.rows.set(key, r); r.innerHTML = html; this.anim(r, this.reduced ? [{ opacity: 0 }, { opacity: 1 }] : [{ opacity: 0, transform: "translateY(-5px)" }, { opacity: 1, transform: "none" }], { duration: this.reduced ? GENTLE.cap : 320, easing: IN }); }
      else r.innerHTML = html;
      return r;
    }
    /** keep: the sheet is only left for a moment, what is still to be done to it is kept. */
    clearSheet(sheetId, keep) {
      const id = String(sheetId);
      for (const s of this.spots.get(id) || []) { this.fadeOut(s.el, 420); s.el = null; }
      // (a sheet only left for a moment keeps the pieces still on their way to it: Release hold tells the film where every sheet's pieces go before the first sheet is placed)
      const wait = keep ? (this.chips.get(id) || []).filter(c => c._await && !c._done) : [];
      for (const c of this.chips.get(id) || []) if (!wait.includes(c)) this.fadeOut(c, 420);
      if (!keep) this.spots.delete(id);
      if (wait.length) this.chips.set(id, wait); else this.chips.delete(id);
      const st = this.stats.get(id); if (st) { this.stats.delete(id); this.fadeOut(st.box, 700); }
    }
    /** Pieces coming in: a chip at each spot, a copy flying to it from `from`, one after another. */
    async bringIn(sheetId, ids, from, spotAt, key, url) {
      const card = this.focus && this.focus.card, chips = this.chips.get(String(sheetId)) || []; this.chips.set(String(sheetId), chips);
      const flights = [];
      for (let j = 0; j < ids.length; j++) {
        if (this.ff || !card) break;
        const at = spotAt(j); if (!at || !at.r) continue;
        const k = this.chip(at.r, faceFor(ids[j], url), `${key}:${sheetId}:${ids[j]}`); k.c.dataset.pool = String(ids[j]); k.c._spot = at.sp || null; if (key === "place") k.c._await = true; chips.push(k.c);
        flights.push(this.flyInto(from, k.c, k.inner, this.ms(T.fly)));
        await this.sleep(this.ms(T.gap));
      }
      await Promise.all(flights);
    }
    /** The chips are set where their pieces truly lie now (read from the sheet), with a ring and a pop. */
    async settle(sheetId, ids, spots) {
      ids = (ids || []).map(String); const card = this.focus && this.focus.card, all = (this.chips.get(String(sheetId)) || []).filter(c => !c._done);
      const chips = all.filter(c => !ids.length || ids.includes(c.dataset.pool)), use = chips.length ? chips : all;   // (ids the sheet renamed settle the ones still waiting)
      const found = card && ids.length ? G.where(sheetId, ids, card) : new Map();
      for (const c of use) {
        c._done = true; const v = found.get(c.dataset.pool), r = v && card ? G.screen(card, v.uv) : null;
        if (r) { ev("settle", { pool: c.dataset.pool, x: Math.round(mid(r).x), y: Math.round(mid(r).y) }); await this.moveChip(c, r); }
        const sp = c._spot; if (sp && sp.el) { this.fadeOut(sp.el, 360); sp.el = null; }
        this.ring(rectNow(c)); this.pop(c);
      }
      return use.length;
    }

    /* ═════ the director: one step at a time, in the order they came ═════ */
    async run() {
      try {
        this.skipPill();
        await (this.kind === "hold" ? INTRO.hold : INTRO.release).call(this);
        for (;;) {
          if (this.ff) break;
          if (!this.queue.length) {
            if (this.closed) break;
            if (this.terminal) { await Promise.race([new Promise(res => { this.nudge = res; }), this.sleep(T.grace)]); if (!this.queue.length) break; continue; }
            const wait = setTimeout(() => this.spin(true), Math.min(800, this.stallMs / 2));
            await Promise.race([new Promise(res => { this.nudge = res; }), this.sleep(this.stallMs)]);
            clearTimeout(wait); this.spin(false);
            if (!this.queue.length && !this.closed && !this.ff) { this.skip({ stalled: true }); break; }
            continue;
          }
          const s = this.queue.shift(), map = this.kind === "hold" ? HOLD : REL;
          ev("beat", { type: s.type });
          try { if (own(map, s.type)) await map[s.type].call(this, s); else ev("ignored", { type: s.type }); if (this.reduced && !this.ff) await this.hold(); }
          catch (e) { tryDo(() => console.warn("hold fx", e)); }
        }
        await this.outro();
      } catch (e) { tryDo(() => console.warn("hold fx", e)); }
      finally { this.end(); }
    }
    async outro() {
      this.unfocus();
      const from = this.mark && this.mark.el.isConnected ? rectNow(this.mark.el) : null;
      for (const id of [...this.spots.keys()]) this.clearSheet(id);
      if (this.mark) this.anim(this.mark.el, [{ opacity: 1 }, { opacity: 0 }], { duration: 260, easing: "ease-in" });
      if (this.cap) { const c = this.cap; this.cap = null; this.anim(c, [{ opacity: 1 }, { opacity: 0 }], { duration: 260, easing: "ease-in" }); }
      if (this.stay) { stayPut(this.rid); return; }
      await returnToOnHold(this.rid, { from, instant: this.ff, released: this.kind === "release", text: this.doneText, via: this });
    }
    end() {
      if (this.over) return; this.over = true; clearTimeout(this.guard);
      for (const f of this.off) tryDo(f); if (this.unsub) tryDo(this.unsub);
      this.unfocus(); tryDo(() => { const NF = root.NestFocus; if (NF) { if (NF.isOpen && NF.isOpen() && NF.close) NF.close(); if (NF.restore) NF.restore(); } });
      this.close(); if (cur === this) cur = null;
      const res = { ok: !this.failed, skipped: this.skipped, timedOut: this.timedOut, stalled: this.stalled, steps: this.steps.length, ms: Math.round(performance.now() - this.t0), error: this.failed || undefined };
      ev("end", res); this.resolve(res);
    }
  }

  /* ═════ HOLD ═════ */
  const TRAY = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M3 13h5l1.5 3h5L16 13h5"/><path d="M5 13l2-7h10l2 7v6H5z"/></svg>';
  const QR = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M4 4h6v6H4zM14 4h6v6h-6zM4 14h6v6H4zM14 14h2v2h-2zM18 14h2M14 18h2M18 18h2v2"/></svg>';
  const NEXT = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M5 12h12M13 6l6 6-6 6"/></svg>';
  const dens = d => { d = +d; return Number.isFinite(d) && d > 0 ? Math.round(d <= 1 ? d * 100 : d) + "% full" : ""; };
  const INTRO = {
    async hold() {
      const n = this.plan && Array.isArray(this.plan.pieces) ? this.plan.pieces.filter(p => !p.state || p.state === "onSheet").length : 0, ns = this.plan && Array.isArray(this.plan.sheets) ? this.plan.sheets.length : 0;
      this.say(`Putting order ${this.rid} on hold`, [n ? pl(n, "piece") : "", ns ? pl(ns, "sheet") : ""].filter(Boolean).join(" \u00b7 "));
      this.tray("On hold", TRAY, 0);
      await this.hold(this.ms(700));
      if (modeNow() !== "nest") { this.ring(tabBtn("nest"), true); await this.dipTo("nest"); }
    },
    /* the order's card lifts out of On hold, its design rises and flies into the tray as the Nest tab comes in */
    async release() {
      const card = holdCard(this.rid), r = card && rectOf(card), th = card && (rectOf(card.querySelector(".comparePair, .cuDzThumbs, .rowMedia")) || r), M = motion();
      const img = card && card.querySelector(".comparePair img, .cuDzThumbs img, img"); this.pic = (img && (img.currentSrc || img.src)) || "";
      this.say(`Releasing order ${this.rid}`, "back in line");
      const mk = this.tray("Next in line", NEXT, "");
      if (r && M && !this.reduced) {
        const g = M.ghost(card, r, null, card); this.ghosts.add(g); g.dataset.hfx = "card:" + this.rid;
        this.anim(g, [{ transform: "none", opacity: 1 }, { transform: "translate(0,-6px) scale(1.012)", opacity: 1, offset: .35 }, { transform: "translate(0,-14px) scale(.985)", opacity: 0 }], { duration: 900, easing: SOFT }).then(() => { this.ghosts.delete(g); g.remove(); });
      }
      const sb = stageBox(), o = th ? mid(th) : { x: sb.left + sb.width / 2, y: sb.top + 120 }, coin = this.node("hfxCoin"), face = el("div", "hfxFace"); coin.appendChild(face);
      if (this.pic) { const im = new Image(); im.alt = ""; im.decoding = "sync"; im.src = this.pic; face.appendChild(im); }
      coin.dataset.hfx = "coin:" + this.rid; coin.style.opacity = "0";
      if (!this.reduced) {
        await this.anim(coin, [{ transform: `translate(${o.x}px,${o.y}px) scale(.5)`, opacity: 0 }, { transform: `translate(${o.x}px,${o.y - 42}px) scale(1)`, opacity: 1 }], { duration: this.ms(T.rise), easing: SOFT });
        const home = rectNow(mk.el), to = home ? mid(home) : { x: o.x, y: sb.top + 30 };
        ev("flight", { key: "coin:" + this.rid, from: { x: Math.round(o.x), y: Math.round(o.y - 42) }, to: { x: Math.round(to.x), y: Math.round(to.y) } });
        const fly = this.anim(coin, arc({ x: o.x, y: o.y - 42 }, to, { s0: 1, s1: .45, o1: 0 }), { duration: this.ms(T.fly), easing: "linear" });
        await Promise.all([fly, modeNow() !== "nest" ? this.dipTo("nest") : null]);
      } else if (modeNow() !== "nest") await this.dipTo("nest");
      this.drop(coin); mk.set("1st"); this.ring(rectNow(mk.el), true);
      await this.hold(this.ms(500));
    }
  };
  const HOLD = {
    async start(s) { if (Array.isArray(s.sheets)) { this.sheetsN = s.sheets.length; if (this.capMain && this.capMain.indexOf("Putting") === 0 && !/sheet/.test(this.capSmall)) this.sub([this.capSmall, pl(s.sheets.length, "sheet")].filter(Boolean).join(" \u00b7 ")); } },
    async sheetBegin(s) {
      this.sheetNo++; if (s.label) this.labels.set(String(s.sheetId), s.label);
      const label = s.label || this.labelOf(s.sheetId) || "the sheet";
      this.say(label, this.sheetsN > 1 ? `sheet ${this.sheetNo} of ${this.sheetsN}` : "");
      await this.focusOn(s.sheetId, label, s); await this.hold(this.ms(this.reduced ? GENTLE.read : 760));
    },
    async lift(s) {
      const label = s.label || this.labelOf(s.sheetId) || "the sheet", ids = Array.isArray(s.poolIds) ? s.poolIds : [];
      if (!this.focus || this.focus.key !== String(s.sheetId)) await this.focusOn(s.sheetId, label, s);
      this.say(`Taking the pieces off ${label}`, pl(ids.length, "piece"));
      const { card, list } = this.items(s.sheetId, ids, s.rects, s), mk = this.mark, M = motion();
      const cr = card && rectOf(G.cv(card)), fb = cr ? { left: cr.left + cr.width / 2 - 28, top: cr.top + cr.height / 2 - 24, width: 56, height: 48 } : null;
      const rects = list.map(it => (it.uv && card && G.screen(card, it.uv)) || fb), spots = list.map((it, i) => ({ uv: it.uv, r: rects[i], free: true, el: null }));
      this.spots.set(String(s.sheetId), spots);
      if (this.reduced || !M || this.ff) mk.add(ids.length);
      else {
        // the pieces glow where they lie, then lift off one by one and fly to the tray
        const glows = rects.map(r => { if (!r) return null; const g = this.node("hfxGlow"); Object.assign(g.style, { left: r.left - 5 + "px", top: r.top - 5 + "px", width: r.width + 10 + "px", height: r.height + 10 + "px" });
          this.anim(g, [{ opacity: 0, transform: "scale(.94)" }, { opacity: 1, transform: "scale(1.04)", offset: .6 }, { opacity: .85, transform: "scale(1)" }], { duration: this.ms(T.glow), easing: SOFT }); return g; });
        await this.sleep(this.ms(T.glow));
        const flights = [];
        for (let i = 0; i < list.length && !this.ff; i++) {
          const it = list[i], r = rects[i];
          if (!r) { mk.add(1); continue; }
          const vis = it.vis || el("div", "hfxBlank"); vis.classList.add("hfxPiece");
          const g = M.ghost(vis, r, 0, doc.body); g.dataset.hfx = `lift:${s.sheetId}:${it.poolId}`; this.ghosts.add(g);
          ev("flight", { key: g.dataset.hfx, from: { x: Math.round(r.left + r.width / 2), y: Math.round(r.top + r.height / 2) }, to: { x: Math.round(mid(rectNow(mk.el) || r).x), y: Math.round(mid(rectNow(mk.el) || r).y) } });
          const glow = glows[i], ms = this.ms(T.fly);
          flights.push(this.until(M.fly(g, mk.el, { plus: false, ms }), ms + 900).then(() => { this.ghosts.delete(g); g.remove(); mk.add(1); this.fadeOut(glow, 260); }));
          await this.sleep(this.ms(T.gap));
        }
        await Promise.all(flights); for (const g of glows) this.fadeOut(g, 260);
      }
      await this.hold();
    },
    async removed(s) {
      const card = await this.on(s.sheetId, s), list = this.spots.get(String(s.sheetId)) || [];
      for (const sp of list) {
        const r = (card && sp.uv && G.screen(card, sp.uv)) || sp.r; if (!r) continue; sp.r = r;
        const n = this.node("hfxSpot"); Object.assign(n.style, { left: r.left + "px", top: r.top + "px", width: r.width + "px", height: r.height + "px" }); sp.el = n;
        this.anim(n, this.reduced ? [{ opacity: 0 }, { opacity: 1 }] : [{ opacity: 0, transform: "scale(.9)" }, { opacity: 1, transform: "scale(1.07)", offset: .4 }, { opacity: .8, transform: "scale(1)", offset: .7 }, { opacity: 1, transform: "scale(1.04)", offset: .85 }, { opacity: .9, transform: "scale(1)" }], { duration: this.ms(T.spot * 1.4), easing: SOFT });
      }
      this.row(s.sheetId, `Pieces off <b>${list.length || +s.removed || 0}</b>`, "off");
      await this.sleep(this.ms(this.reduced ? 0 : T.spot));
    },
    async fillBegin(s) {
      await this.on(s.sheetId, s); this.fillTot = +s.spots || 0; this.filled = 0;
      this.say("Filling the empty spots", [this.fillTot ? pl(this.fillTot, "spot") : "", this.labelOf(s.sheetId)].filter(Boolean).join(" \u00b7 "));
      this.row(s.sheetId, `Filled <b>0</b> of <b>${this.fillTot}</b>`, "fill");
      await this.hold();
    },
    async fillFrom(s) {
      const to = s.toSheetId != null ? s.toSheetId : s.sheetId, ids = Array.isArray(s.poolIds) ? s.poolIds : [], newer = s.source === "newerSheet" || (s.source !== "waiting" && s.fromSheetId != null);
      const from = s.fromLabel || this.labelOf(s.fromSheetId), spot = ids.length > 1 ? "spots" : "spot";
      if (!this.focus || this.focus.key !== String(to)) await this.focusOn(to, this.labelOf(to), s);
      this.say(newer ? `Filling the empty ${spot} from ${from || "a newer sheet"}` : `Filling the empty ${spot} from the waiting list`, [s.rid ? `Order ${s.rid}` : "", pl(ids.length, "piece")].filter(Boolean).join(" \u00b7 "));
      const card = this.focus && this.focus.card; if (!card) { await this.hold(); return; }
      const anchor = rectOf(newer ? G.tab(card, s.fromSheetId) : G.queue(card)) || rectOf(card), spots = this.spots.get(String(to)) || [];
      this.ring(anchor, true);
      await this.bringIn(to, ids, anchor, j => {
        const sp = spots.find(x => x.free) || null; if (sp) sp.free = false;
        const cr = rectOf(G.cv(card)), r = sp && sp.r ? sp.r : cr ? { left: cr.left + cr.width * .4 + j * 24, top: cr.top + cr.height * .4, width: 52, height: 52 } : null;
        return { r, sp };
      }, "fill", s.designUrl);
      await this.hold(this.ms(500));
    },
    /** Nothing waiting fits (or a spot stays open): said once, quietly, and the hold goes on. */
    async fillSkipped(s) {
      const why = String(s.why || "").replace(/^\w/, c => c.toUpperCase()).slice(0, 90);
      this.say("Nothing fits this spot yet", why); await this.hold(this.ms(this.reduced ? GENTLE.read : 900));
    },
    async fillPlaced(s) {
      const ids = (s.poolIds || []).map(String); await this.on(s.sheetId, s); const n = await this.settle(s.sheetId, ids);
      this.filled += n || ids.length || 1;
      this.row(s.sheetId, `Filled <b>${this.fillTot ? Math.min(this.filled, this.fillTot) : this.filled}</b> of <b>${this.fillTot || this.filled}</b>`, "fill");
      this.say(`Placed on ${this.labelOf(s.sheetId) || "the sheet"}`, [s.rid ? `Order ${s.rid}` : "", this.fillTot ? `${Math.min(this.filled, this.fillTot)} of ${this.fillTot} filled` : ""].filter(Boolean).join(" \u00b7 "));
      await this.hold(this.ms(700));
    },
    async qr(s) {
      await this.on(s.sheetId, s); this.say("Remaking QR labels", this.labelOf(s.sheetId));
      const r = this.row(s.sheetId, `${QR}QR label remade <span class="tick">\u2713</span>`, "qr");
      if (r && !this.reduced && !this.ff) { const t = r.querySelector(".tick"); if (t) this.anim(t, [{ clipPath: "inset(0 100% 0 0)" }, { clipPath: "inset(0 0 0 0)" }], { duration: 520, delay: 260, easing: SOFT }); }
      await this.hold(this.ms(T.tick));
    },
    async sheetDone(s) {
      await this.on(s.sheetId, s); const label = this.labelOf(s.sheetId) || "The sheet", n = +s.charmCount, d = dens(s.density), what = [Number.isFinite(n) ? pl(n, "piece") : "", d].filter(Boolean).join(" \u00b7 ");
      this.row(s.sheetId, `<b>${what}</b>`, "done");
      this.say(`${label} is done`, what);
      await this.hold(this.ms(T.read));
      this.clearSheet(s.sheetId);
    },
    async held(s) {
      this.terminal = true; if (this.mark) this.mark.done();
      this.say(`Order ${s.rid || this.rid} is on hold`, "back to Orders");
      await this.hold();
    },
    async done() { this.terminal = true; },
    async error(s) {
      this.terminal = true; this.failed = String(s.message || "It stopped"); const label = s.sheetId != null ? this.labelOf(s.sheetId) : "";
      this.say(label ? `Stopped on ${label}` : "Stopped", this.failed.slice(0, 90), { bad: true });
      await this.hold(this.ms(1500));
    }
  };

  /* ═════ RELEASE ═════ */
  const REL = {
    async start() {},
    async queued(s) {
      this.say(s.front === false ? "Back in line" : "First in line, ahead of new orders", `Order ${this.rid}`);
      if (this.mark) this.mark.set("1st");
      await this.hold();
    },
    async target(s) {
      this.tgt = s; if (s.label) this.labels.set(String(s.sheetId), s.label); this.relLabel = s.label || this.labelOf(s.sheetId);
      const label = this.relLabel || "the sheet";
      this.say(`Finding a spot on ${label}`, s.newSheet ? "a new sheet" : "the next free spot");
      let card = await this.focusOn(s.sheetId, label, s);
      for (let i = 0; i < 8 && !card && !this.ff; i++) { await this.sleep(150); card = await this.focusOn(s.sheetId, label, s); }   // (a new sheet may take a moment to appear)
      if (s.newSheet && card && !this.reduced) {
        const tabs = rectOf(G.tab(card, s.sheetId)) || rectOf(card.querySelector(".shHead")) || rectOf(card);
        if (tabs) { const n = this.node("hfxNew"); n.textContent = "New sheet"; Object.assign(n.style, { left: tabs.left + "px", top: tabs.bottom + 4 + "px" });
          this.anim(n, [{ opacity: 0, transform: "scale(.7)" }, { opacity: 1, transform: "scale(1.06)", offset: .5 }, { opacity: 1, transform: "none" }], { duration: 560, easing: "cubic-bezier(.3,1.3,.5,1)" }); this.ring(tabs, true); }
      }
      await this.hold();
    },
    async flight(s) {
      const to = s.toSheetId != null ? s.toSheetId : s.sheetId, ids = Array.isArray(s.poolIds) ? s.poolIds : [];
      if (!this.focus || this.focus.key !== String(to)) await this.focusOn(to, this.labelOf(to), s);
      this.say(`Placing the pieces on ${this.labelOf(to) || this.relLabel || "the sheet"}`, pl(ids.length, "piece"));
      const card = this.focus && this.focus.card; if (!card) { await this.hold(); return; }
      const spot = this.tgt && this.tgt.spot ? G.uvOfStep(to, card, this.tgt.spot) : null, sr = spot && G.screen(card, spot), cr = rectOf(G.cv(card)), from = this.mark && rectNow(this.mark.el);
      await this.bringIn(to, ids, from, j => ({ r: sr ? { left: sr.left + j * 14, top: sr.top + j * 10, width: sr.width, height: sr.height } : cr ? { left: cr.left + cr.width * .42 + j * 30, top: cr.top + cr.height * .45, width: 54, height: 54 } : null }), "place", s.designUrl);
      await this.hold(this.ms(400));
    },
    async placed(s) {
      const ids = (s.poolIds || []).map(String); await this.on(s.sheetId, s); const n = await this.settle(s.sheetId, ids);   // (the light is back on the sheet that is named: another sheet's flight came after this one's)
      this.say(`Placed on ${this.labelOf(s.sheetId) || this.relLabel || "the sheet"}`, pl(ids.length || n, "piece"));
      this.row(s.sheetId, `<b>${pl(ids.length || n, "piece")}</b> placed`, "placed");
      await this.hold();
    },
    async qr(s) {
      if (s.made !== false) return HOLD.qr.call(this, s);
      // (the engine did not make the label: a sheet still filling gets it when it joins its set; the film says so and ticks nothing)
      await this.on(s.sheetId, s); this.say("QR label comes later", this.labelOf(s.sheetId));
      this.row(s.sheetId, `${QR}QR label comes later`, "qr");
      await this.hold(this.ms(T.tick));
    },
    async released(s) {
      this.terminal = true; const names = s.placed === false ? [] : [...new Set((s.sheets || []).map(x => x && (x.label || this.labelOf(x.sheetId))).filter(Boolean))];   // (an order on two sheets says both, like the timeline step does)
      const label = names.length ? names.join(", ") : this.relLabel || this.labelOf(this.tgt && this.tgt.sheetId);
      this.doneText = `Order ${s.rid || this.rid} is back in line${label ? " \u00b7 placed on " + label : ""}`;
      if (this.mark) this.mark.done();
      this.say(`Order ${s.rid || this.rid} is released`, label || "");
      await this.hold();
    },
    async done() { this.terminal = true; },
    async error(s) { return HOLD.error.call(this, s); }
  };

  /* ═════ the way home: Orders, On hold ═════ */
  /** Takes the user to Orders > On hold and lands the order's card there: it flies in from the tray (else it opens its own
   *  room), a stamp presses its HELD pill and the On hold pill answers. Safe to call twice (the film calls it and so may the
   *  glue): the second call gets the first one's result. opts: from (a rect the card flies in from), instant (the final
   *  state at once), released (the order is not in On hold any more: a note says where it went), text. */
  function returnToOnHold(rid, opts = {}) {
    rid = String(rid == null ? "" : rid); const key = "r:" + rid;
    if (RET.has(key)) return RET.get(key);
    const p = goHome(rid, opts || {}).catch(e => { tryDo(() => console.warn("hold fx", e)); return false; });
    RET.set(key, p); p.then(() => setTimeout(() => { if (RET.get(key) === p) RET.delete(key); }, 3000));
    return p;
  }
  /** The person chose a tab: the way home, called right after by the glue, is answered with "not taken" and moves nothing. */
  function stayPut(rid) {
    const key = "r:" + rid, p = Promise.resolve(false); if (RET.has(key)) return; RET.set(key, p);
    setTimeout(() => { if (RET.get(key) === p) RET.delete(key); }, 3000);
  }
  async function goHome(rid, o) {
    const via = o.via || null, S = via || new Stage();
    try {
      const instant = S.ff || S.reduced || !!o.instant, was = modeNow(); ev("home", { rid, instant });
      if (instant || was === "orders") { if (was !== "orders") { setMode("orders"); await S.settleMode("orders"); } showHold(); }
      else await S.dipTo("orders", async () => { showHold(); await frame(); });
      if (o.released) { note(o); return true; }
      // the card (drawn from the rows the engine has just written: a moment is given for it, never long)
      let card = null; const t0 = performance.now(), spin = setTimeout(() => { if (via && !card) via.spin(true); }, 500);
      while (!card && performance.now() - t0 < 2600) { card = holdCard(rid); if (!card) await new Promise(r => setTimeout(r, 70)); }
      clearTimeout(spin); if (via) via.spin(false);
      if (!card) { ev("home", { rid, card: false }); return false; }
      card.scrollIntoView({ block: "center", behavior: "auto" });
      if (instant) { ev("home", { rid, landed: "instant" }); return true; }
      if (LANDED.has(rid) && Date.now() - LANDED.get(rid) < 20000) return true;
      LANDED.set(rid, Date.now());
      await landCard(S, card, o.from); stamp(S, card);
      const b = tryDo(() => doc.querySelector('#ordChips [data-pile="hold"] b'));
      if (b && !S.reduced) b.animate([{ transform: "scale(1)" }, { transform: "scale(1.3)", offset: .3 }, { transform: "scale(1)" }], { duration: 760, easing: "cubic-bezier(.2,.7,.3,1)" });
      await new Promise(r => setTimeout(r, S.reduced ? 0 : 700));
      return true;
    } finally { if (!via) S.close(); }
  }
  /** The card, hidden, is flown in from where the pieces went (a copy of it arcs over, grows to size and lands); with no
   *  place to come from it opens its own room. */
  async function landCard(S, card, from) {
    const M = motion(), r = rectNow(card);
    ev("land", { motion: !!M, reduced: S.reduced, card: !!r, from: from ? { x: Math.round(from.left), y: Math.round(from.top), w: Math.round(from.width), h: Math.round(from.height) } : null });
    if (!M || S.reduced || !r) return;
    if (!from || !onScreen(from)) { M.grow(card, { room: false }); return; }
    const g = M.ghost(card, r, null, card); S.ghosts.add(g); g.dataset.hfx = "land:" + (card.dataset.rid || ""); card.style.visibility = "hidden";
    const fm = mid(from), to = mid(r), dx = fm.x - to.x, dy = fm.y - to.y, s = clamp(Math.min(from.height * 1.2 / r.height, from.width * 1.2 / r.width), .04, .3);
    ev("flight", { key: g.dataset.hfx, from: { x: Math.round(fm.x), y: Math.round(fm.y) }, to: { x: Math.round(to.x), y: Math.round(to.y) } });
    const ms = 1100, a = g.animate([
      { transform: `translate(${dx}px,${dy}px) scale(${s})`, opacity: 0 },
      { transform: `translate(${dx * .6}px,${dy * .6 - 40}px) scale(${(1 + s) / 2.3})`, opacity: 1, offset: .38 },
      { transform: "translate(0,-8px) scale(1.012)", opacity: 1, offset: .82 },
      { transform: "none", opacity: 1 }], { duration: ms, easing: GLIDE, fill: "forwards" });
    S.anims.add(a); await Promise.race([a.finished.catch(() => {}), new Promise(r2 => setTimeout(r2, ms + 400))]); S.anims.delete(a);
    card.style.visibility = ""; g.remove(); S.ghosts.delete(g);
  }
  /** A stamp comes down on the card's HELD pill: it lands a little large, settles, and a ring spreads. (Not a seal: nothing
   *  here is a permanent record of its own, the order's timeline keeps that.) */
  function stamp(S, card) {
    const pill = card.querySelector(".ost, .pill, .relHold"), r = rectOf(pill); if (!pill || !r || S.reduced) return;
    pill.animate([{ transform: "scale(1.7) rotate(-5deg)", opacity: 0 }, { transform: "scale(.94) rotate(0)", opacity: 1, offset: .55 }, { transform: "scale(1)", opacity: 1 }], { duration: 560, easing: "cubic-bezier(.62,0,.92,.5)" });
    setTimeout(() => S.ring(rectNow(pill)), 380);
  }
  /** A released order is not in On hold any more: a note under the pill says where it went. */
  function note(o) {
    const M = motion(), at = doc.querySelector('#ordChips [data-pile="hold"]') || doc.querySelector('#modeSeg [data-mode="orders"]');
    if (!M || !at || typeof M.note !== "function" || !o.text) return;
    tryDo(() => M.note(at, { text: o.text, ms: 7000, actions: [{ label: "Show", fn: () => setMode("nest") }] }));
  }

  /* ═════ the public face ═════ */
  function play(kind, rid, source) {
    if (cur) tryDo(() => cur.skip());
    cur = new Film(kind, rid, source);
    return cur.api;
  }
  root.OrderHoldFx = {
    playHold: (rid, source) => play("hold", rid, source),
    playRelease: (rid, source) => play("release", rid, source),
    returnToOnHold,
    skip: () => { if (cur) cur.skip(); },
    active: () => !!cur,
    events: () => LOG.slice(), clearEvents: () => { LOG.length = 0; },
    locate: {}, T, GENTLE
  };
})(typeof window !== "undefined" ? window : globalThis);
