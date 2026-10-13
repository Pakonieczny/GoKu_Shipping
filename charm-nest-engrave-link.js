/* The one way into the engraving of one piece of one order (Paul, 5 Oct 2026: "When I click the 'Open it in Engraving'
 * button it does not open the actual back engraving modal with this specific order, it only open the Engraving tab list
 * with no specific order selected").
 *
 *   window.EngraveLink.open({ rid, key, poolId, piece, face, sandbox, closeFirst }) -> Promise<boolean>
 *     rid        the order number (else read from poolId, piece or key)
 *     key        the order line's key (row.key): the line the Overview shows
 *     poolId     the pool id of one copy of the piece (the Sheet tab and the Sheet window know it)
 *     piece      the piece the order window has chosen (its line key); it wins over `key` when both name a job
 *     face       "front" | "back": which face of a piece engraved on both (else the face a key names: a front's key ends "~front"; else the one still to settle)
 *     sandbox    optional true/false: what the caller knows about the order. A Sandbox order is never opened from the
 *                production page, nor a real one from the Sandbox page (the two never cross)
 *     closeFirst optional function (may return a promise): how the caller's own window closes, so nothing is ever
 *                opened on top of a window (the order window and the Sheet window are closed here without being asked)
 *   true: the Engraving tab is on that order's own piece, its details open (the placement card for a piece still to
 *   settle; its Decided row, opened, for an approved or skipped one), the search box on the order number so nothing else
 *   is in front. false: it was not: NOTHING moved (no window closed, the Engraving tab keeps its view, list and search box)
 *   and one plain line says why (inside the open window, else a toast): a piece on hold or on no sheet yet has no job.
 *
 *   window.EngraveLink.resolve({ rid, key, poolId, piece }) -> { rid, job, tab: "place" | "done" | null, why }   (why: the plain line for a piece with no job that none is coming for, else "")
 *     what open() would land on, without moving anything (tests, and the callers that want to know first)
 *
 * The Engrave module (charm-nest-bridge.js) owns the screen; this only names where it should be (Engrave.restoreView,
 * the very same call the Sheet tab's "View in Engrave" made) and checks that it got there. Nothing is written, nothing
 * is read from the network, no job is created or read by a model: a piece whose job is still to come is waited for
 * (a small labelled spinner, at most WAIT_MS), then the line says so and the person stays where they were.
 * Callers feature-detect: without this file they keep what they did before. */
(function () {
  "use strict";
  if (window.EngraveLink) return;
  const AWAY = { hold: "This piece is on hold and not on a sheet yet, so there is no engraving to open.", sheet: "This piece is not on a sheet yet, so there is no engraving to open." };
  const DONE = ["approved", "written", "skipped"];                                  // the Decided tab's
  const PLACE = ["review", "words", "blocked", "classify", "ready", "fitting"];     // the Placements tab's
  const WAIT_MS = 7000, STEP_MS = 120, SHUT_MS = 2500, LAND_MS = 4000;
  const digits = v => String(v == null ? "" : v).replace(/\D/g, "");
  const lead = v => { const m = /^(\d{4,20})(?:[_:\-]|$)/.exec(String(v == null ? "" : v)); return m ? m[1] : ""; };
  const sleep = ms => new Promise(r => setTimeout(r, ms));
  const bounded = (p, ms) => Promise.race([Promise.resolve(p).catch(() => {}), sleep(ms)]);
  const tab = job => (job && DONE.includes(job.state) ? "done" : job && PLACE.includes(job.state) ? "place" : null);
  const live = j => !!(j && j.row && j.row.state !== "gone");
  const ridOfJob = j => digits(j && j.row && j.row.order && j.row.order.receiptId);
  const jobs = () => { try { const E = window.Engrave; return E && E.items ? [...E.items().values()] : []; } catch (_) { return []; } };
  const goMode = m => { const f = typeof window.setMode === "function" ? window.setMode : window.CN && window.CN.setMode; if (typeof f === "function") f(m); else throw new Error("no way to switch tab"); };
  const say = (msg, kind, ms) => {
    try {
      // (a modal window sits over the toasts: the line is said inside it, once, and goes by itself)
      const dlg = document.querySelectorAll("dialog[open]");
      if (dlg.length && kind !== "bad") { document.querySelectorAll(".elWait[data-note]").forEach(n => n.remove()); const n = spinner(msg, true); n.node.dataset.note = "1"; n.node.addEventListener("click", n.remove); setTimeout(n.remove, ms || 6500); return; }
      const t = typeof window.toast === "function" ? window.toast : window.CN && window.CN.toast; if (t) t(msg, kind || "", ms || 6500, "engrave-link");
    } catch (_) {}
  };
  const sandboxNow = () => { try { return typeof WORKSPACE_SANDBOX !== "undefined" ? !!WORKSPACE_SANDBOX : !!(window.Sandbox && window.Sandbox.on && window.Sandbox.on()); } catch (_) { return false; } };

  /* the face of an engraving (charm-nest-engrave-faces.js): a job, a key or a request that does not say "front" is the back */
  const faceOf = x => { const F = window.CharmNestEngraveFaces; return F && F.faceOf ? F.faceOf(x) : x && (x.face === "front" || /~front$/.test(String(x.key || ""))) ? "front" : "back"; };
  /** The face a caller asked for: `face`, else the face a key names ("...~front"), else none (no preference). */
  const face = t => (t.face === "front" || t.face === "back" ? t.face : /~front$/.test(String(t.piece || t.key || "")) ? "front" : "");
  /** The job of the piece named, and nothing else: another piece of the same order is never put in its place. */
  function resolve(t) {
    t = t || {};
    const E = window.Engrave;
    const rid = digits(t.rid) || lead(t.poolId) || lead(t.piece) || lead(t.key);
    if (!E || !E.items) return { rid, job: null, tab: null };
    const items = E.items(), mine = j => live(j) && (!rid || ridOfJob(j) === rid);
    let job = null;
    // a copy's own job first (two copies of one line share one; a copy edited on its own has its own), then the piece, then the line
    // (a piece engraved on both faces has TWO jobs that hold the same copy: the one asked for is the one named by `piece` / `key` (a front's key ends "~front"), else the face asked for, else the one still to settle, else the back)
    if (t.poolId) {
      const held = jobs().filter(j => mine(j) && !j.editingBack && (j.copies || []).includes(t.poolId));
      job = held.length > 1 ? (held.find(j => j.key === String(t.piece)) || held.find(j => j.key === String(t.key)) || (face(t) && held.find(j => faceOf(j) === face(t))) || held.find(j => PLACE.includes(j.state)) || held.find(j => faceOf(j) === "back") || held[0]) : held[0] || null;
    }
    for (const k of [t.piece, t.key]) { if (job || !k) continue; const j = items.get(String(k)); if (j && mine(j)) job = j; }
    // the face asked for, when the job found is the piece's other face (a line key names the back; the front of the same piece is its sibling)
    const want = face(t), F = window.CharmNestEngraveFaces;
    if (job && want && F && F.parseKey && faceOf(job) !== want) { const pk = F.parseKey(job.key), sib = items.get(F.jobKey(pk.lineKey, pk.slot, want)); if (sib && mine(sib) && !sib.editingBack) job = sib; }
    // nothing but the order named: its one job; an order of several pieces is shown as its own list instead (no guess)
    if (!job && rid && !t.poolId && !t.piece && !t.key) { const all = jobs().filter(j => mine(j) && !j.editingBack); if (all.length === 1) job = all[0]; }
    return { rid, job, tab: tab(job), why: job ? "" : AWAY[awayWhy(t, rid)] || "" };   // (why: the one plain line for a piece with no job that none is coming for: on hold, or on no sheet yet; else "")
  }

  /** A job that is still to come: the order's line is in the pull and its engraving is not settled (not read, or to settle),
   *  or the page has no engraving job at all yet (it is starting). A record of an old order is never waited for. */
  function mayArrive(t, rid) {
    const E = window.Engrave; if (!E || !E.items) return false;
    let rows = [];
    try { rows = (window.Orders && window.Orders.rows ? window.Orders.rows() : []).filter(r => r && r.state !== "gone" && digits(r.order && r.order.receiptId) === rid); } catch (_) {}
    const named = [t.piece, t.key].filter(Boolean).map(String);
    const mineRows = rows.filter(r => named.includes(r.key) || (t.poolId && (r.poolIds || []).includes(t.poolId)));
    const pool = mineRows.length ? mineRows : named.length || t.poolId ? [] : rows;
    // (ENGCRASH: a piece that is on hold or on no sheet yet is not waited for: its job is made only once it is on a sheet, Engrave.classifyAll, so none is coming in the next seconds)
    return pool.some(r => { if (awayOf(r)) return false; const e = r.engrave; return !e ? !!(r.spec && r.spec.engraveCandidate) : ["classify", "reclassify"].includes(e.state) || (!!e.needed && !e.approved && e.state !== "skipped"); });
  }

  /** Why a piece of the order has no engraving job and none is coming: "hold" (its order is on hold, off its sheet) or "sheet" (it is on no sheet yet: a job is made once the piece is on a sheet), else "". */
  function awayOf(r) {
    try {
      // (a job is made once the piece is on a sheet, in a run released by the new policy: Gate.modern; an older run makes it when the piece is pooled, so a pooled piece is waited for there)
      const PP = window.PiecePlacement, p = PP && PP.ofRow ? PP.ofRow(r) : null, G = window.Gate, sheetFirst = !(G && G.modern) || !!G.modern();
      if (p && p.state && p.state !== "loading") return p.state === "hold" && !p.onSheet ? "hold" : p.state === "waiting" && !p.onSheet && sheetFirst ? "sheet" : "";
      if (r.hold || r.state === "held") return "hold";
      return sheetFirst && ["pulled", "waiting", "pooled"].includes(r.state) ? "sheet" : "";
    } catch (_) { return ""; }
  }
  /** What the order named (by its pieces, or the order alone) is: the first "hold" among its rows, else "sheet" when every row is away, else "". */
  function awayWhy(t, rid) {
    let rows = [];
    try { rows = (window.Orders && window.Orders.rows ? window.Orders.rows() : []).filter(r => r && r.state !== "gone" && digits(r.order && r.order.receiptId) === rid); } catch (_) {}
    const named = [t.piece, t.key].filter(Boolean).map(String), mine = rows.filter(r => named.includes(r.key) || (t.poolId && (r.poolIds || []).includes(t.poolId))), use = mine.length ? mine : rows;
    const a = use.map(awayOf); if (!a.length || a.some(x => !x)) return "";
    return a.includes("hold") ? "hold" : "sheet";
  }

  /* ── the small labelled spinner of a wait (in the open window, when there is one: a modal dialog covers the page) ── */
  function spinner(text, plain) {
    if (!document.getElementById("elStyle")) {
      const s = document.createElement("style"); s.id = "elStyle";
      s.textContent = ".elWait{position:fixed;left:50%;bottom:26px;transform:translateX(-50%);z-index:2147483000;display:inline-flex;align-items:center;gap:8px;padding:8px 14px;border:1px solid var(--line,#e4ddd0);border-radius:999px;background:var(--card,#fffefb);color:var(--ink70,#5b554c);font:600 12px var(--sans,system-ui,sans-serif);box-shadow:var(--sh,0 9px 28px rgba(30,26,20,.12));max-width:calc(100vw - 32px)}" +
        ".elWait i{width:11px;height:11px;flex:0 0 11px;border:2px solid var(--line,#e4ddd0);border-top-color:var(--gold,#a9823f);border-radius:50%;animation:elSpin .8s linear infinite}" +
        ".elWait span{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}@keyframes elSpin{to{transform:rotate(360deg)}}@media (prefers-reduced-motion:reduce){.elWait i{animation-duration:2.4s}}";
      document.head.appendChild(s);
    }
    const open = document.querySelectorAll("dialog[open]"), host = open.length ? open[open.length - 1] : document.body;
    const n = document.createElement("div"); n.className = "elWait"; n.setAttribute("role", "status"); n.setAttribute("aria-live", "polite");
    n.innerHTML = plain ? "<span></span>" : '<i aria-hidden="true"></i><span></span>'; n.lastChild.textContent = text;
    host.appendChild(n);
    return { remove: () => n.remove(), node: n };
  }

  /** The caller's window, the order window, and the Sheet window (an order view opened from the sheet window gives it
   *  back as it closes: it goes too). Waits for a seal being pressed first, as the windows themselves do. */
  async function shut(extra) {
    try { await window.Seal?.whenIdle?.(); } catch (_) {}
    if (typeof extra === "function") { try { await bounded(extra(), SHUT_MS); } catch (_) {} }
    const OW = window.OrderWin;
    if (OW && OW.isOpen && OW.isOpen()) {
      await bounded(OW.close && OW.close(), SHUT_MS);
      const d = document.getElementById("orderWin"); if (OW.isOpen()) { try { d.close(); } catch (_) { d && d.removeAttribute("open"); } }
    }
    const SW = window.SheetWin;
    if (SW && SW.isOpen && SW.isOpen()) {
      await bounded(SW.close && SW.close(), SHUT_MS);
      for (let i = 0; i < 20 && SW.isOpen(); i++) await sleep(50);
    }
  }

  /** A date filter on the Engraving lists would hide the very piece asked for: it is set to All for that list, only then. */
  function widen(scope, js) {
    const A = window.CNListActivity; if (!A || !A.state || !A.matches) return;
    try { const s = A.state(scope); if (s.range !== "all" && js.some(j => !A.matches(j, s.range))) A.set(scope, { range: "all" }); } catch (_) {}
  }

  /** Is the piece in front? null: the tab is not drawn yet (a seal is being pressed, the page is settling). */
  function shown(job, done) {
    const root = document.getElementById("engraveView"); if (!root || root.classList.contains("hidden")) return null;
    if (done) { const row = root.querySelector(".doneRow.open"); return !!row && row.dataset.key === job.key; }
    const card = root.querySelector("#egQueue .rvItem"); return !!card && card.dataset.key === job.key;
  }

  async function land(job, rid) {
    const E = window.Engrave, done = tab(job) === "done", v = E.view();
    widen(done ? "engrave-done" : "engrave-place", [job]);
    E.restoreView(Object.assign({}, v, done
      ? { tab: "done", focus: null, list: false, chosen: true, q: rid, openDone: job.key }
      : { tab: "place", focus: job.key, list: false, chosen: true, q: rid, openDone: null }));
    goMode("engrave"); E.render();
    const end = Date.now() + LAND_MS; let s = shown(job, done);
    while (s === null && Date.now() < end) { await sleep(60); s = shown(job, done); }
    // (a decided piece's row is brought into view when the list is longer than the screen)
    if (s === true && done) { try { const row = document.querySelector("#engraveView .doneRow.open"); if (row) row.scrollIntoView({ block: "nearest" }); } catch (_) {} }
    const w = E.view();
    return s === true || (s === null && w.tab === (done ? "done" : "place") && (done || w.focus === job.key));
  }

  /** A piece with no engraving job to open (ENGCRASH, Paul 12 Oct: the back card of a held order waited, then closed the order window and left the Engraving tab filtered to an order that has no job: an empty list).
   *  Nothing moves: no window is closed, the Engraving tab keeps its view, its list and its search box. The reason is said in one plain line, inside the window that is open (a modal window covers the toasts), else as a toast. */
  function fallback(rid, why, t) {
    const away = why === "none" ? "" : why === "late" ? "" : awayWhy(t || {}, rid);
    say(why === "none" ? "This piece has nothing to engrave."
      : AWAY[away] ? AWAY[away]
      : why === "late" ? "The engraving of this piece did not finish loading. Try again in a moment."
      : "This piece's engraving is not loaded here.", "", 7000);
    return false;
  }
  let seq = 0;
  async function open(t) {
    t = t || {};
    const my = ++seq, E = window.Engrave;
    try { document.querySelectorAll(".elWait[data-note]").forEach(n => n.remove()); } catch (_) {}   // (a line said by an earlier press is gone before the next one)
    if (!E || !E.items || !E.restoreView || !E.render) return false;   // nothing moved: the caller's own way is all there is
    let r = resolve(t);
    if (typeof t.sandbox === "boolean" && t.sandbox !== sandboxNow()) {
      say(`${r.rid ? "Order " + r.rid : "This order"} belongs to ${t.sandbox ? "the Sandbox" : "production"}, and this page is in ${sandboxNow() ? "Sandbox" : "production"} mode, so its engraving is not opened here.`, "bad", 8000);
      return false;
    }
    if (!r.rid && !r.job) { say("There is no order to open in Engraving.", "bad"); return false; }
    // (ENGCRASH: no job and none coming, or a job with nothing to engrave: nothing is closed and nothing is filtered, the reason is said where the person is)
    if (r.job ? !r.tab : !mayArrive(t, r.rid)) return fallback(r.rid, r.job ? "none" : "missing", t);
    let waited = false;
    if (!r.job && mayArrive(t, r.rid)) {
      waited = true;
      const sp = spinner(`Finding the engraving for order ${r.rid}…`);
      try {
        for (const end = Date.now() + WAIT_MS; Date.now() < end && my === seq;) {
          await sleep(STEP_MS); r = resolve(t);
          if (r.job || !mayArrive(t, r.rid)) break;
        }
      } finally { sp.remove(); }
      if (my !== seq) return false;
      if (!r.job) return fallback(r.rid, "late", t);   // (ENGCRASH: it did not come: the window stays, the Engraving tab is not touched)
    }
    await shut(t.closeFirst);
    if (my !== seq) return false;
    try {
      if (r.job && r.tab) return await land(r.job, r.rid || ridOfJob(r.job));
      fallback(r.rid, r.job ? "none" : waited ? "late" : "missing", t);
    } catch (e) { console.warn("engrave link:", e); throw e; }
    return false;
  }

  window.EngraveLink = { open, resolve };
})();
