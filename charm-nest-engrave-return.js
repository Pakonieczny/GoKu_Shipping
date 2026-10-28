/* The way back from the Engraving tab to where the journey started (ENGBACK, engrave-ui-1012 round 2b; Paul, 12 Oct 2026: "you completely removed the
 * process flow that allowed the user to come back to the order view from the engraving tab if that's where they started the journey ... They're
 * starting position every single time does it change").
 *
 * It is the page's OWN return, not a new one: the Sheet window's "to Engrave and back" (charm-nest-sheetwin.js goEngrave / returnPill / back) remembers
 * where the person came from every time they press Fix in Engraving, shows one pill in the top bar beside the tabs (class swReturn: a gold
 * "Back to ..." button, the order number, a dismiss cross), takes them back with the window growing out of the pill, and, when the piece is approved
 * in Engraving, says so and goes back by itself. This file is that component for every other start: the order window (any tab of it), the tab
 * beneath (Orders, Library, Review, Nest, ...). The Sheet window now hands its journey to the same pill (EngraveReturn.set), so there is ONE control and ONE
 * memory: starting a new journey replaces the old one.
 *
 *   window.EngraveReturn
 *     capture(t) -> origin           where the person is NOW (before EngraveLink closes anything): { kind: "order" | "page" | "sheet" | "keep", mode, ... }
 *                                    "keep": the Engraving tab itself with no window over it, so a journey going on (Back / Next in the queue) keeps its origin
 *     arm(origin, t)                 after EngraveLink.open landed: the pill for that origin (nothing moved: nothing changes; "sheet": the Sheet window sets its own)
 *     set({ kind, label, rid, key, was, back? })   a journey started elsewhere (the Sheet window); back(approved, rect) is how it goes back
 *     back(approved) / dismiss() / clear()          one press back / the cross / leaving the tab (no animation)
 *     current() -> the journey or null              (tests, other workers)
 *   EngraveLink.open and Engrave.openBack are wrapped here (feature-detected, nothing else of them changes): capture before, arm after.
 *
 * What a return restores: the tab beneath (Orders / Library / Review / ... and, for the Engraving tab itself, its list/card/search as it was),
 * the order window of THAT order on the tab it was on (Overview / Timeline / Sheet), the line and the piece chosen on its pieces list, the sheet and charm of
 * the Sheet tab, and every scroll position in it. Staged edits in the Engraving panel are never touched: they live in CharmNestEngravePanel.DRAFTS per piece, so
 * going back leaves them staged, the way they were (and the by-itself return after an approval waits while any are staged). Nothing is written or read from the network.
 * The origin is cleared when the person returns, presses the cross, or leaves the Engraving tab by any other way. */
(function () {
  "use strict";
  if (window.EngraveReturn) return;
  const POLL_MS = 350, AUTO_MS = 1300;
  const ICON_BACK = '<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M10 3 5 8l5 5" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"/></svg>';
  const ICON_X = '<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M4 4l8 8M12 4l-8 8" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round"/></svg>';
  const EASE = "cubic-bezier(.2,.8,.2,1)";
  const VIEW_OF = { orders: "ordersView", engrave: "engraveView", review: "reviewView", library: "libView", design: "designView", master: "masterView", efficiency: "efficiencyView", charms: "charmsView", nest: "sheets" };
  const WORD_OF = { orders: "Orders", engrave: "Design & Engrave", review: "Review", library: "Library", design: "Design Station", master: "Master files", efficiency: "Employee efficiency", charms: "Charms", nest: "Nest" };
  const R = { ctx: null, el: null, poll: 0, timer: 0, obs: null, going: false, seq: 0 };
  const esc = s => String(s == null ? "" : s).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const digits = v => String(v == null ? "" : v).replace(/\D/g, "");
  const tryDo = (f, d) => { try { return f(); } catch (e) { console.warn("engrave return:", e); return d; } };
  const still = () => { try { return window.Motion && Motion.reduced ? Motion.reduced() : matchMedia("(prefers-reduced-motion: reduce)").matches; } catch (_) { return false; } };
  const modeNow = () => (window.CN && CN.S && CN.S.mode) || "";
  const goMode = m => { const f = typeof window.setMode === "function" ? window.setMode : window.CN && window.CN.setMode; if (typeof f === "function") f(m); };
  const say = (msg, kind, ms) => { try { const t = typeof window.toast === "function" ? window.toast : window.CN && window.CN.toast; if (t) t(msg, kind || "", ms || 6500, "engrave-return"); } catch (_) {} };
  const wins = () => ({ order: !!(window.OrderWin && OrderWin.isOpen && OrderWin.isOpen()), sheet: !!(window.SheetWin && SheetWin.isOpen && SheetWin.isOpen()) });

  /* ── scroll positions: every scrolled box inside a root, found again by its id or its tag and classes and its place among those ── */
  const selOf = e => { if (e.id) return "#" + CSS.escape(e.id); const cl = [...e.classList].slice(0, 2); return e.tagName.toLowerCase() + cl.map(c => "." + CSS.escape(c)).join(""); };
  function scrollsOf(root) {
    const out = []; if (!root) return out;
    for (const e of [root, ...root.querySelectorAll("*")]) {
      if (!(e.scrollTop > 0 || e.scrollLeft > 0)) continue;
      if (e === root) { out.push({ self: true, top: e.scrollTop, left: e.scrollLeft }); continue; }
      const sel = selOf(e), i = [...root.querySelectorAll(sel)].indexOf(e);
      if (i >= 0) out.push({ sel, i, top: e.scrollTop, left: e.scrollLeft });
    }
    return out;
  }
  /** Puts the scrolls back as the content arrives (images and records change heights for a second), and stops at the first thing the person does themselves. */
  function putScrolls(root, scrolls, win) {
    if ((!scrolls || !scrolls.length) && !win) return;
    let stop = false; const ev = ["wheel", "touchstart", "keydown", "mousedown"], off = () => { stop = true; ev.forEach(n => document.removeEventListener(n, off, true)); };
    ev.forEach(n => document.addEventListener(n, off, { capture: true, passive: true }));
    const apply = () => {
      if (stop || (root && !root.isConnected)) return;
      for (const s of scrolls || []) {
        const e = s.self ? root : root && [...root.querySelectorAll(s.sel)][s.i];
        if (e && (Math.abs(e.scrollTop - s.top) > 1 || Math.abs(e.scrollLeft - s.left) > 1)) { e.scrollTop = s.top; e.scrollLeft = s.left; }
      }
      if (win && (Math.abs(window.scrollY - win[1]) > 1 || Math.abs(window.scrollX - win[0]) > 1)) window.scrollTo(win[0], win[1]);
    };
    [0, 120, 350, 800, 1500].forEach(ms => setTimeout(ms ? apply : () => requestAnimationFrame(apply), ms));
    setTimeout(off, 1700);
  }
  const viewRoot = m => document.getElementById(VIEW_OF[m] || "");
  /** The box that holds the tabs' views and their scrollers (the Library scrolls in the stage around it): every scroll beneath is kept, not only the view's own. */
  const mainRoot = () => document.querySelector("main.main") || document.getElementById("app") || document.body;

  /* ── where the person is now ── */
  function pageOf(m) {
    const E = window.Engrave;
    let engrave = null;
    if (m === "engrave" && E && E.view) {
      // (the words drafts of the Engraving tab are not in the snapshot: coming back never replaces what has been typed meanwhile)
      engrave = Object.assign({}, tryDo(() => E.view(), {})); delete engrave.drafts;
    }
    return { mode: m, scrolls: scrollsOf(mainRoot()), win: [window.scrollX, window.scrollY], engrave };
  }
  function orderOf(OW) {
    const w = tryDo(() => OW.where && OW.where(), null) || {};
    const key = w.key || tryDo(() => OW.key(), null);
    return {
      rid: w.rid || tryDo(() => OW.rid(), ""), key, view: w.view || tryDo(() => OW.view(), "info"),
      piece: w.piece !== undefined ? w.piece : tryDo(() => OW.selectedPiece(), null), side: w.side !== undefined ? w.side : tryDo(() => OW.selectedSide(), null),
      sheetId: w.sheetId || "", sheetAt: w.sheetAt == null ? null : w.sheetAt, focus: w.focus || "",
      scrolls: scrollsOf(document.getElementById("orderWin"))
    };
  }
  /** Where the person is, before anything is closed. "keep": on the Engraving tab with no window over it (a journey going on). */
  function capture() {
    const w = wins(), m = modeNow();
    if (w.order) return Object.assign({ kind: "order" }, pageOf(m), { order: orderOf(window.OrderWin) });
    if (w.sheet) return { kind: "sheet", mode: m };
    if (m && m !== "engrave") return Object.assign({ kind: "page" }, pageOf(m));
    return { kind: "keep", mode: m };
  }

  /* ── the pill: the Sheet window's, in the top bar beside the tabs, where it covers nothing ── */
  function ensureEl() {
    if (R.el && R.el.isConnected) return R.el;
    if (!document.getElementById("egRetStyle")) {
      // (the tab strip gives way, never the pill: it scrolls, and keeps the tab in front in view)
      const st = document.createElement("style"); st.id = "egRetStyle";
      st.textContent = ".swReturn[data-eng-return]{flex:0 0 auto}.swReturn[data-eng-return] .go{white-space:nowrap}";
      document.head.appendChild(st);
    }
    R.el = document.createElement("div"); R.el.className = "swReturn"; R.el.setAttribute("role", "status"); R.el.dataset.engReturn = "";
    (document.getElementById("modeNav") || document.body).appendChild(R.el);
    return R.el;
  }
  function paint(state) {   // state: "wait" | "going" | "done"
    const c = R.ctx, el = ensureEl(); if (!c) return;
    const ok = state !== "wait";
    el.classList.toggle("ok", ok);
    // (waiting: the Sheet window's pill names the order beside the sheet it goes back to; for the order window and the tabs the order is on the card in front of the person, so the pill keeps to what it does)
    const word = state === "going" ? "Approved · going back" : state === "done" ? "Approved" : c.kind === "sheet" ? esc(c.rid || "") : "";
    const title = state === "going" ? "Approved: going back" : state === "done" ? "Approved: press Back when you are done here" : `Order ${esc(c.rid || "")}: settle it here, then go back`;
    el.innerHTML = `<button class="go" data-a="back" title="${c.kind === "sheet" ? "" : title}">${ICON_BACK}Back to ${esc(c.label)}</button>${word ? `<span class="st" title="${title}"><i></i>${word}</span>` : ""}<button class="x" data-a="x" title="Stay here" aria-label="Dismiss">${ICON_X}</button>`;
    el.querySelector("[data-a=back]").onclick = () => back();
    el.querySelector("[data-a=x]").onclick = () => dismiss();
  }
  const stagedFor = c => {
    const P = window.CharmNestEngravePanel, rid = digits(c.rid); if (!P || !P.DRAFTS || !rid) return false;
    for (const [k, d] of P.DRAFTS) if (String(k).startsWith(rid) && d && (d.dirty || d.posTouched)) return true;
    return false;
  };
  function stopTimers() { clearInterval(R.poll); clearTimeout(R.timer); R.poll = 0; R.timer = 0; }
  function watch() {
    if (R.obs) return;
    const v = document.getElementById("engraveView"); if (!v || !window.MutationObserver) return;
    // leaving the Engraving tab by any other way ends the journey
    R.obs = new MutationObserver(() => { if (R.ctx && !R.going && v.classList.contains("hidden")) clear(); });
    R.obs.observe(v, { attributes: true, attributeFilter: ["class"] });
  }
  /** A journey starts (or starts again): the starting position is replaced, the pill drawn afresh. */
  function set(ctx) {
    if (!ctx) return clear();
    stopTimers();
    const c = R.ctx = Object.assign({}, ctx);
    if (!c.label) c.label = WORD_OF[c.mode] || "where you were";
    const el = ensureEl(); paint("wait");
    el.hidden = false; el.style.animation = "none"; void el.offsetWidth; el.style.animation = "";
    watch();
    // approved in Engraving: the pill says so and takes the person back to where they came from (unless something is staged there)
    R.poll = setInterval(() => {
      if (R.ctx !== c) return clearInterval(R.poll);
      const E = window.Engrave, job = c.key && E && E.items ? tryDo(() => E.items().get(c.key), null) : null;
      if (job && !job.stamping && !(window.Seal && Seal.busy && Seal.busy()) && ["approved", "written"].includes(job.state) && !["approved", "written"].includes(c.was)) {
        clearInterval(R.poll); R.poll = 0;
        if (stagedFor(c)) { paint("done"); return; }
        paint("going");
        R.timer = setTimeout(() => { if (modeNow() === "engrave" && R.ctx === c) back(true); }, AUTO_MS);
      }
    }, POLL_MS);
  }
  /** Leaving: the memory and the pill go at once. */
  function clear() { stopTimers(); R.ctx = null; if (R.el) R.el.hidden = true; }
  /** The cross: the memory goes, the pill folds away. */
  function dismiss() {
    stopTimers(); const was = R.ctx; R.ctx = null; const el = R.el; if (!el) return;
    if (was && was.gone) tryDo(() => was.gone());
    const done = () => { if (!R.ctx) el.hidden = true; };
    if (still() || !el.animate) return done();
    el.animate([{ opacity: 1, transform: "none" }, { opacity: 0, transform: "translateY(-6px) scale(.96)" }], { duration: 180, easing: EASE }).finished.then(done, done);
  }

  /* ── going back ── */
  /** The tab beneath, as it was (and for the Engraving tab itself: its list, card and search). */
  function setPage(c) {
    if (c.mode && modeNow() !== c.mode) goMode(c.mode);
    const E = window.Engrave;
    if (c.mode === "engrave" && c.engrave && E && E.restoreView && E.render) { E.restoreView(c.engrave); E.render(); }
    putScrolls(mainRoot(), c.scrolls, c.win);
  }
  function toOrder(c) {
    const OW = window.OrderWin, o = c.order; if (!OW || !OW.openOrder) throw new Error("the order view is not loaded");
    setPage(c);
    const sheet = o.view === "sheet", opts = { view: o.view || "info", row: o.key ? { key: o.key } : undefined, from: R.el && !R.el.hidden ? R.el : undefined };
    if (sheet) Object.assign(opts, { sheetId: o.sheetId || undefined, poolId: o.focus || undefined, sheetAt: o.sheetAt == null ? undefined : o.sheetAt });
    const p = OW.openOrder(o.rid, opts);
    Promise.resolve(p).then(() => settleOrder(o)).catch(e => { console.warn("engrave return: order view", e); say(`Order ${o.rid} did not open: ${e.message || e}`, "bad", 6000); });
  }
  /** The window is open: the piece chosen on its pieces list, then every scroll in it. */
  async function settleOrder(o) {
    const OW = window.OrderWin, end = Date.now() + 4000;
    while (Date.now() < end && !(OW.isOpen() && (!o.key || OW.key() === o.key))) await new Promise(r => setTimeout(r, 80));
    if (!OW.isOpen()) return;
    const want = o.side && (o.piece || o.key) ? (o.piece || o.key) + "#" + o.side : o.piece || null;
    if ((tryDo(() => OW.selectedPiece(), null) || null) !== (o.piece || null) || (o.side && tryDo(() => OW.selectedSide(), null) !== o.side)) tryDo(() => OW.selectPiece(want));
    putScrolls(document.getElementById("orderWin"), o.scrolls);
  }
  function toPage(c) {
    setPage(c);
    const root = viewRoot(c.mode);
    if (root && !still() && root.animate) root.animate([{ opacity: 0, transform: "translateY(6px)" }, { opacity: 1, transform: "none" }], { duration: 260, easing: EASE });
  }
  /** One press back. approved: the piece was approved in Engraving (the Sheet window flashes the charm it came from). */
  function back(approved) {
    if (window.Seal && Seal.defer && Seal.defer("engrave-return", () => back(approved))) return;
    const c = R.ctx; if (!c || R.going) return;
    R.going = true; stopTimers();
    const rect = R.el && R.el.isConnected ? R.el.getBoundingClientRect() : null;
    try {
      if (typeof c.back === "function") c.back(!!approved, rect && { left: rect.left, top: rect.top, width: rect.width, height: rect.height, right: rect.right, bottom: rect.bottom, x: rect.x, y: rect.y });
      else if (c.kind === "order") toOrder(c);
      else toPage(c);
    } catch (e) {
      console.warn("engrave return:", e); say(`Could not go back to ${c.label}: ${e.message || e}`, "bad", 6000);
      R.going = false; set(c); return;      // the way back stays, as it was
    }
    R.ctx = null;                           // returned: this journey is over
    if (c.gone) tryDo(() => c.gone());
    const el = R.el; if (el) { if (still() || !el.animate) el.hidden = true; else el.animate([{ opacity: 1, transform: "none" }, { opacity: 0, transform: "translateY(-6px) scale(.96)" }], { duration: 180, easing: EASE }).finished.then(() => { if (!R.ctx) el.hidden = true; }, () => { if (!R.ctx) el.hidden = true; }); }
    setTimeout(() => { R.going = false; }, 400);
  }

  /** After EngraveLink / openBack landed on the Engraving tab with the windows gone: the pill for the origin captured before. */
  function arm(o, t) {
    if (!o || o.kind === "keep") return;
    const w = wins(); if (modeNow() !== "engrave" || w.order || w.sheet) return;    // nothing moved: the journey (if any) is as it was
    if (o.kind === "sheet") { clear(); return; }                                    // the Sheet window sets its own (EngraveReturn.set) as the link ends
    const E = window.Engrave, L = window.EngraveLink;
    const job = tryDo(() => L && L.resolve && t ? L.resolve(t).job : null, null) || tryDo(() => {
      const v = E.view(); return v.focus ? E.items().get(v.focus) : null;
    }, null);
    const rid = digits(t && t.rid) || digits(job && job.row && job.row.order && job.row.order.receiptId) || digits(o.order && o.order.rid);
    set({ kind: o.kind, mode: o.mode, label: o.kind === "order" ? "order" : (WORD_OF[o.mode] || "where you were"), rid, key: job ? job.key : null, was: job ? job.state : null, order: o.order, scrolls: o.scrolls, win: o.win, engrave: o.engrave });
  }

  /* ── the doors into the Engraving tab: capture before, arm after ── */
  function wrap(obj, name) {
    if (!obj || typeof obj[name] !== "function" || obj[name].__engBack) return;
    const orig = obj[name];
    const wrapped = async function (...args) {
      const my = ++R.seq; let o = null;
      try { o = capture(); } catch (e) { console.warn("engrave return: capture", e); }
      const res = await orig.apply(this, args);
      if (my === R.seq) tryDo(() => arm(o, name === "open" ? args[0] : null));
      return res;
    };
    wrapped.__engBack = true; obj[name] = wrapped;
  }
  wrap(window.EngraveLink, "open");
  wrap(window.Engrave, "openBack");

  window.EngraveReturn = { capture, arm, set, back, dismiss, clear, current: () => (R.ctx ? Object.assign({}, R.ctx) : null), _R: R };
})();
