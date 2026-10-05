/* The one way into the engraving of one piece of one order (Paul, 5 Oct 2026: "When I click the 'Open it in Engraving'
 * button it does not open the actual back engraving modal with this specific order, it only open the Engraving tab list
 * with no specific order selected").
 *
 *   window.EngraveLink.open({ rid, key, poolId, piece, sandbox, closeFirst }) -> Promise<boolean>
 *     rid        the order number (else read from poolId, piece or key)
 *     key        the order line's key (row.key): the line the Overview shows
 *     poolId     the pool id of one copy of the piece (the Sheet tab and the Sheet window know it)
 *     piece      the piece the order window has chosen (its line key); it wins over `key` when both name a job
 *     sandbox    optional true/false: what the caller knows about the order. A Sandbox order is never opened from the
 *                production page, nor a real one from the Sandbox page (the two never cross)
 *     closeFirst optional function (may return a promise): how the caller's own window closes, so nothing is ever
 *                opened on top of a window (the order window and the Sheet window are closed here without being asked)
 *   true: the Engraving tab is on that order's own piece, its details open (the placement card for a piece still to
 *   settle; its Decided row, opened, for an approved or skipped one), the search box on the order number so nothing else
 *   is in front. false: it was not (a toast says why, and the Engraving tab is filtered to the order instead).
 *
 *   window.EngraveLink.resolve({ rid, key, poolId, piece }) -> { rid, job, tab: "place" | "done" | null }
 *     what open() would land on, without moving anything (tests, and the callers that want to know first)
 *
 * The Engrave module (charm-nest-bridge.js) owns the screen; this only names where it should be (Engrave.restoreView,
 * the very same call the Sheet tab's "View in Engrave" made) and checks that it got there. Nothing is written, nothing
 * is read from the network, no job is created or read by a model: a piece whose job is not there yet is waited for
 * (a small labelled spinner, at most WAIT_MS), then the Engraving tab is shown for the order and the toast says so.
 * Callers feature-detect: without this file they keep what they did before. */
(function () {
  "use strict";
  if (window.EngraveLink) return;
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
  const say = (msg, kind, ms) => { try { const t = typeof window.toast === "function" ? window.toast : window.CN && window.CN.toast; if (t) t(msg, kind || "", ms || 6500, "engrave-link"); } catch (_) {} };
  const sandboxNow = () => { try { return typeof WORKSPACE_SANDBOX !== "undefined" ? !!WORKSPACE_SANDBOX : !!(window.Sandbox && window.Sandbox.on && window.Sandbox.on()); } catch (_) { return false; } };

  /** The job of the piece named, and nothing else: another piece of the same order is never put in its place. */
  function resolve(t) {
    t = t || {};
    const E = window.Engrave;
    const rid = digits(t.rid) || lead(t.poolId) || lead(t.piece) || lead(t.key);
    if (!E || !E.items) return { rid, job: null, tab: null };
    const items = E.items(), mine = j => live(j) && (!rid || ridOfJob(j) === rid);
    let job = null;
    // a copy's own job first (two copies of one line share one; a copy edited on its own has its own), then the piece, then the line
    if (t.poolId) job = jobs().find(j => mine(j) && !j.editingBack && (j.copies || []).includes(t.poolId)) || null;
    for (const k of [t.piece, t.key]) { if (job || !k) continue; const j = items.get(String(k)); if (j && mine(j)) job = j; }
    // nothing but the order named: its one job; an order of several pieces is shown as its own list instead (no guess)
    if (!job && rid && !t.poolId && !t.piece && !t.key) { const all = jobs().filter(j => mine(j) && !j.editingBack); if (all.length === 1) job = all[0]; }
    return { rid, job, tab: tab(job) };
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
    return pool.some(r => { const e = r.engrave; return !e ? !!(r.spec && r.spec.engraveCandidate) : ["classify", "reclassify"].includes(e.state) || (!!e.needed && !e.approved && e.state !== "skipped"); });
  }

  /* ── the small labelled spinner of a wait (in the open window, when there is one: a modal dialog covers the page) ── */
  function spinner(text) {
    if (!document.getElementById("elStyle")) {
      const s = document.createElement("style"); s.id = "elStyle";
      s.textContent = ".elWait{position:fixed;left:50%;bottom:26px;transform:translateX(-50%);z-index:2147483000;display:inline-flex;align-items:center;gap:8px;padding:8px 14px;border:1px solid var(--line,#e4ddd0);border-radius:999px;background:var(--card,#fffefb);color:var(--ink70,#5b554c);font:600 12px var(--sans,system-ui,sans-serif);box-shadow:var(--sh,0 9px 28px rgba(30,26,20,.12));max-width:calc(100vw - 32px)}" +
        ".elWait i{width:11px;height:11px;flex:0 0 11px;border:2px solid var(--line,#e4ddd0);border-top-color:var(--gold,#a9823f);border-radius:50%;animation:elSpin .8s linear infinite}" +
        ".elWait span{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}@keyframes elSpin{to{transform:rotate(360deg)}}@media (prefers-reduced-motion:reduce){.elWait i{animation-duration:2.4s}}";
      document.head.appendChild(s);
    }
    const open = document.querySelectorAll("dialog[open]"), host = open.length ? open[open.length - 1] : document.body;
    const n = document.createElement("div"); n.className = "elWait"; n.setAttribute("role", "status"); n.setAttribute("aria-live", "polite");
    n.innerHTML = '<i aria-hidden="true"></i><span></span>'; n.lastChild.textContent = text;
    host.appendChild(n);
    return { remove: () => n.remove() };
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

  /** The Engraving tab filtered to the order (a list, nothing chosen), with the reason said. */
  function fallback(rid, why) {
    const E = window.Engrave, v = E.view(), mine = jobs().filter(j => live(j) && !j.editingBack && ridOfJob(j) === rid);
    const t = mine.some(j => PLACE.includes(j.state)) || !mine.some(j => DONE.includes(j.state)) ? "place" : "done";
    widen("engrave-" + t, mine);
    E.restoreView(Object.assign({}, v, { tab: t, focus: null, list: true, chosen: true, q: rid, openDone: null }));
    goMode("engrave"); E.render();
    const of = rid ? "order " + rid : "this order";
    say(why === "none" ? `This piece has nothing to engrave. This is the Engraving tab for ${of}.`
      : why === "late" ? `The engraving of this piece did not finish loading. This is the Engraving tab for ${of}.`
      : `This piece's engraving is not loaded here. This is the Engraving tab for ${of}.`, "", 7000);
  }

  let seq = 0;
  async function open(t) {
    t = t || {};
    const my = ++seq, E = window.Engrave;
    if (!E || !E.items || !E.restoreView || !E.render) return false;   // nothing moved: the caller's own way is all there is
    let r = resolve(t);
    if (typeof t.sandbox === "boolean" && t.sandbox !== sandboxNow()) {
      say(`${r.rid ? "Order " + r.rid : "This order"} belongs to ${t.sandbox ? "the Sandbox" : "production"}, and this page is in ${sandboxNow() ? "Sandbox" : "production"} mode, so its engraving is not opened here.`, "bad", 8000);
      return false;
    }
    if (!r.rid && !r.job) { say("There is no order to open in Engraving.", "bad"); return false; }
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
    }
    await shut(t.closeFirst);
    if (my !== seq) return false;
    try {
      if (r.job && r.tab) return await land(r.job, r.rid || ridOfJob(r.job));
      fallback(r.rid, r.job ? "none" : waited ? "late" : "missing");
    } catch (e) { console.warn("engrave link:", e); throw e; }
    return false;
  }

  window.EngraveLink = { open, resolve };
})();
