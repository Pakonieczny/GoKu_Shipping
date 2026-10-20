/* Charm Nest · Cancel Order on an On hold card (Paul, 5 Oct 2026: "Image #3 - Add an extra button here 'Cancel Order' that
   will cancel the order and move it to cancelled orders tab. Also add the same style beautiful animation showing the user
   where the order is moving to.").

   The button sits beside "Release hold" on the cards of Orders > On hold, and nowhere else. It cancels the order through
   the one cancel path the sorter already has (SheetWin.takeOffOrder with mode "cancel": the record is kept first under
   Orders > Cancelled by cancelPut, the order's timeline gets the cancel with who and when and keeps every earlier step,
   then the order leaves every list; an order that somehow still has pieces on a sheet comes off them through the same
   steps a cancel from the sheet window takes). Nothing is deleted anywhere; the record can be restored.

   No question is asked: Undo is the safety. The person's name is asked the way Review's Complete Order and Hold do it, in
   the small inline name bar (CNEmployee.edit), never a browser pop-up, and only when none is saved on this computer.

   What is seen, by the app's own Motion pieces (charm-nest-motion.js), so it reads like every other move in the sorter:
   the card lifts while the cancel is kept (the button shows a small labelled spinner), the Cancelled chip answers with a
   soft ring, the card flies to it and shrinks into it, the count on the chip ticks up, and a note says
   "Order N cancelled" with Undo and Show. Undo is the existing cancelRestore: the order comes back on hold and its card
   flies back out of the chip into On hold. Only transform and opacity animate; prefers-reduced-motion has no flight and the
   note still shows; a safety sweep never leaves an overlay behind; the work itself never waits for the animation.

   A save that fails (offline, the server) leaves the card exactly where it was with one plain line under the buttons;
   nothing is lost and the button can be pressed again.

   Public: CancelUI.mount(node, rid | row) (a Review card of an order on hold, CANCELREVIEW; was Open Orders' held card), CancelUI.cancel(rid), CancelUI.undo(rid), CancelUI.busy(rid), CancelUI.canUndo(rid).

   (NOHOLDPILE, Paul 20 Oct 2026: "Remove the 'On hold' tab and UI from the Orders tab ... it competes with the existing Review
   tab": the On hold pile is gone; the Cancel Order button (CancelUI.mount) moves to the held card in Open Orders. The cancel itself, its
   flight to the Cancelled chip, the note and Undo stay, called as CancelUI.cancel(rid); a held order is shown in Review.)

   (CANCELREVIEW, Paul 20 Oct 2026: "Relocate the 'Cancel Order' button to be only visible as part of the orders that are on Hold inside the Review
   list order view. Every order on Hold in the Review needs a 'Cancel Order' button. When pressed that particular order needs to animate and fly
   towards the Orders tab and then go into the Cancelled orders sub tab, and this needs to be easily visible by the user to understand where the
   order ended up." The button is on the Review card of an order on hold (a held line, a lifted hold, the "Held order" item), one per card, beside
   that card's Release Hold (the same .btn sm height and corners, plain); the Orders tab has none. Review draws the card and calls
   CancelUI.mount(node, rid). Pressed in Review, the cancel is the same one path; the Review list's own update (Motion.expect on the card's name)
   flies the card to the Orders tab in the top bar's tabs (the Cancelled chip when it is on screen), the tab answers with a ring and a small
   "Cancelled N" tag whose number rolls up as the card lands, and the note says where it went: "Order N cancelled — it's in Orders › Cancelled",
   with Undo and Show (Orders, on Cancelled, at that order). Undo puts the order back on hold and its card flies back out of the Orders tab into
   Review. Only transform and opacity animate; prefers-reduced-motion has no flight, ring or tag, and the note still shows.) */
(function (root) {
  "use strict";
  const doc = root.document;
  const M = () => root.Motion || null;
  const reduced = () => { try { return M() ? M().reduced() : !!root.matchMedia("(prefers-reduced-motion: reduce)").matches; } catch (_) { return false; } };
  const esc = s => String(s == null ? "" : s).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const FLY_MS = 1300, UNDO_MS = 12000, MSG_MS = 12000, SWEEP_MS = 9000;

  const CSS = [
    ".cnHoldBtns{display:flex;flex-wrap:wrap;align-items:stretch;gap:8px;min-width:0;max-width:100%}",
    ".cnHoldBtns>.btn{margin:0;min-width:0}",
    // (RHSAME, Paul 20 Oct 2026: the Release Hold beside it is the Review card's .btn now, so Cancel keeps the .btn corners (9 px, was 7 px to match the old small "Release hold"): the same height and radius, still plain)
    ".btn.ghost.cnCancelBtn{font:600 12px var(--sans);color:var(--ink70);cursor:pointer}",
    ".btn.ghost.cnCancelBtn:hover:not(:disabled){color:var(--ink)}",
    ".btn.ghost.cnCancelBtn:disabled{cursor:default}",
    ".cnCancelBtn .spin{margin-right:6px}",
    ".ocard.cnBusy,.orderListRow.cnBusy,.reviewListRow.cnBusy{transform:translateY(-2px);transition:transform .24s cubic-bezier(.2,.8,.2,1)}",
    "@media (prefers-reduced-motion:reduce){.ocard.cnBusy,.orderListRow.cnBusy,.reviewListRow.cnBusy{transform:none;transition:none}}",
    ".rowActions>.cnCancelMsg{flex-basis:100%}",   // (CANCELREVIEW: the plain line under a Review card's buttons, on a row of its own)
    // (CANCELREVIEW: the "Cancelled N" tag on the Orders tab while a card flies to it from Review; it lives on the motion layer, so nothing in the top bar moves)
    ".cnTabTag{position:fixed;z-index:142;display:inline-flex;align-items:center;gap:5px;padding:2px 8px;border-radius:999px;background:var(--velvet,#3b2f2a);color:#f3eee4;font:700 10.5px/1.5 var(--sans);white-space:nowrap;pointer-events:none;box-shadow:0 4px 12px rgba(30,24,16,.18);will-change:transform,opacity}",
    ".cnTabTag>b{display:inline-grid;min-width:1ch;font:800 11px var(--mono);overflow:hidden}",
    ".cnCancelMsg{display:block;max-width:36ch;margin:0;font:12px/1.4 var(--sans);color:var(--ink70)}",
    ".cnTick{display:inline-grid;position:relative}.cnTick>i{grid-area:1/1;font-style:normal;text-align:center}",
    ".cnBeacon{position:fixed;box-sizing:border-box;border:2px solid rgba(176,86,63,.6);border-radius:999px;pointer-events:none;will-change:transform,opacity}",
    ".cnNoteSpin{display:inline-block;width:11px;height:11px;margin-right:8px;vertical-align:-2px;border:2px solid rgba(243,238,228,.3);border-top-color:#f3eee4;border-radius:50%;animation:spin .7s linear infinite}"
  ].join("");
  function css() {
    try { if (doc.getElementById("cnCancelCss")) return; const s = doc.createElement("style"); s.id = "cnCancelCss"; s.textContent = CSS; doc.head.appendChild(s); } catch (_) {}
  }

  /* ── what the page knows ── */
  const nameNow = () => { try { return String((root.CNEmployee && CNEmployee.name && CNEmployee.name()) || (root.B && B.employee) || localStorage.getItem("cn.employee") || "").trim(); } catch (_) { return ""; } };
  const rowsOf = rid => (root.Orders ? Orders.rows() : []).filter(r => String(r.order.receiptId) === String(rid) && r.state !== "gone");
  /** The pile's chip; the Orders tab button when the chip is not drawn or not showing (the tab is always in the top bar). */
  const chipEl = p => { const c = doc.querySelector(`#ordChips [data-pile="${p}"]`); return c && c.getClientRects().length ? c : doc.querySelector('#modeSeg [data-mode="orders"]'); };
  const chipB = p => { const b = doc.querySelector(`#ordChips [data-pile="${p}"] b`); return b && b.getClientRects().length ? b : null; };
  const cardsOf = rid => [...doc.querySelectorAll("#ordItems [data-rid], #rvList [data-rid]")].filter(n => n.dataset.rid === String(rid));
  // (CANCELREVIEW) the Orders tab in the top bar's tabs; the Review cards of an order that carry its Cancel Order, while Review is on screen
  const tabEl = () => doc.querySelector('#modeSeg [data-mode="orders"]');
  const reviewCards = rid => { const v = doc.getElementById("reviewView"); if (!v || v.classList.contains("hidden") || !v.getClientRects().length) return []; return [...doc.querySelectorAll("#rvList > [data-rid]")].filter(n => n.dataset.rid === String(rid) && n.querySelector(".cnCancelBtn")); };
  const RELSEL = '[data-rv-release], .relHold:not([data-gate])';   // (a Review card's Release Hold, or an Orders card's)
  const keyOf = r => "ord:" + r.key;
  const toastIf = (m, kind, ms) => { try { if (typeof root.toast === "function") root.toast(m, kind || "", ms || 5000); } catch (_) {} };

  /** A cancel in flight, by order number: { rid, who, rows, snap, state, started, landed }. */
  const flights = new Map();
  /** What Undo needs, by order number: kept while the page is open (the record itself is kept for good on the server). */
  const undoable = new Map();
  const messages = new Map();   // rid → { text, at }: the plain line under the buttons, put back when a card is drawn again
  const asking = new Set();
  const msgT = new Map();       // rid → the timer that takes its line away

  /* ── the button ── */
  const LABEL = "Cancel Order";
  function paintBusy(rid) {
    const on = flights.has(String(rid));
    for (const n of cardsOf(rid)) {
      n.classList.toggle("cnBusy", on);
      const b = n.querySelector(".cnCancelBtn"); if (!b) continue;
      b.disabled = on;
      b.innerHTML = on ? `<span class="spin"></span>Cancelling` : LABEL;
      // (Release hold waits while the cancel is kept: one change to an order at a time)
      const rel = n.querySelector(RELSEL);
      if (rel) { if (on && !rel.disabled) { rel.disabled = true; rel.dataset.cnOff = "1"; } else if (!on && rel.dataset.cnOff) { rel.disabled = false; delete rel.dataset.cnOff; } }
    }
  }
  function say(rid, text) {
    rid = String(rid);
    if (text) messages.set(rid, { text, at: Date.now() }); else { messages.delete(rid); clearTimeout(msgT.get(rid)); msgT.delete(rid); }
    for (const n of cardsOf(rid)) drawMsg(n, rid);
  }
  function drawMsg(n, rid) {
    const wrap = n.querySelector(".cnHoldBtns") || n.querySelector(".cnCancelBtn"); if (!wrap) return;   // (CANCELREVIEW: a Review card has no wrap: the line goes after its Cancel Order)
    let line = n.querySelector(".cnCancelMsg");
    const m = messages.get(String(rid)), live = m && Date.now() - m.at < MSG_MS;
    if (!live) { if (m) messages.delete(String(rid)); if (line) line.remove(); return; }
    if (!line) { line = doc.createElement("span"); line.className = "cnCancelMsg"; line.setAttribute("role", "alert"); wrap.after(line); }
    if (line.textContent !== m.text) line.textContent = m.text;
    clearTimeout(msgT.get(rid)); msgT.set(rid, setTimeout(() => { msgT.delete(rid); say(rid, ""); }, Math.max(500, MSG_MS - (Date.now() - m.at))));
  }


  /* ── the small motions: a ring on the chip, a count that ticks ── */
  function beacon(el) {
    if (reduced() || !M() || !el || !el.isConnected) return;
    const r = el.getBoundingClientRect(); if (!(r.width > 0)) return;
    const layer = M().layer(el);
    for (let i = 0; i < 2; i++) {
      const ring = doc.createElement("i"); ring.className = "cnBeacon"; ring.setAttribute("aria-hidden", "true");
      Object.assign(ring.style, { left: r.left + "px", top: r.top + "px", width: r.width + "px", height: r.height + "px" });
      layer.appendChild(ring);
      const kill = () => ring.remove();
      try { ring.animate([{ transform: "scale(.96)", opacity: 0 }, { transform: "scale(1)", opacity: .75, offset: .25 }, { transform: "scale(1.4,1.85)", opacity: 0 }], { duration: 900, delay: i * 380, easing: "cubic-bezier(.2,.7,.3,1)", fill: "both" }).finished.then(kill, kill); } catch (_) { kill(); }
      setTimeout(kill, 900 + i * 380 + 700);
    }
  }
  const countOf = () => { try { return root.Cancelled ? Cancelled.count() : null; } catch (_) { return null; } };
  /** The chip's number shows the old count until the card lands (the list had already drawn the new one). */
  function holdCount(ctx) {
    const b = chipB("cancelled"), n = countOf();
    if (!b || n == null || Number(b.textContent) !== n || n < 1) return;
    b.textContent = String(n - 1); ctx.held = b;
    ctx.holdT = setTimeout(() => letGoCount(ctx), 3500);
  }
  function letGoCount(ctx) {
    clearTimeout(ctx.holdT);
    const b = ctx.held; ctx.held = null;
    const n = countOf(); if (b && b.isConnected && n != null && !b.querySelector(".cnTick")) b.textContent = String(n);
  }
  /** A number that rolls to the next: the old one rises out, the new one comes up under it (transform and opacity only). */
  function roll(b, from, to) {
    if (!b || !b.isConnected) return;
    const settle = () => { const n = countOf(); b.classList.remove("cnTick"); b.textContent = String(n == null ? to : n); };
    if (reduced() || from === to || !b.animate) { settle(); return; }
    const up = to > from, o = doc.createElement("i"), n = doc.createElement("i");
    o.textContent = String(from); n.textContent = String(to);
    b.textContent = ""; b.classList.add("cnTick"); b.append(o, n);
    const opts = { duration: 520, easing: "cubic-bezier(.3,1.2,.4,1)", fill: "forwards" };
    try {
      o.animate([{ transform: "none", opacity: 1 }, { transform: `translateY(${up ? "-80%" : "80%"})`, opacity: 0 }], opts);
      n.animate([{ transform: `translateY(${up ? "80%" : "-80%"})`, opacity: 0 }, { transform: "none", opacity: 1 }], opts).finished.then(settle, settle);
    } catch (_) { settle(); return; }
    setTimeout(() => { if (b.classList.contains("cnTick")) settle(); }, 1400);
  }
  function tickChip(delta) {
    const b = chipB("cancelled"), n = countOf(); if (!b || n == null) return;
    roll(b, n - delta, n);
  }

  /* ── the move to Cancelled, by the list's own update (Motion.expect → reconcile → fly): Review's card, or the Orders card ── */
  const toChip = ctx => Object.assign(() => (ctx.state === "failed" ? null : (ctx.started || start(ctx), chipEl("cancelled"))), { cancelRid: ctx.rid });
  // (CANCELREVIEW, Paul 20 Oct 2026: "this needs to be easily visible by the user to understand where the order ended up": the note names the tab and its sub tab)
  const WHERE = rid => `Order ${rid} cancelled \u2014 it's in Orders \u203a Cancelled`;
  function noteSpec(ctx) {
    return { text: WHERE(ctx.rid), ms: UNDO_MS, actions: [
      { label: "Undo", title: "put it back on hold", fn: () => { undo(ctx.rid).catch(e => console.warn("cancel undo", e)); } },
      { label: "Show", title: "open Orders \u203a Cancelled at this order", fn: () => showCancelled(ctx.rid) }] };
  }
  /** What leaves for Cancelled: the order's Review cards when it was pressed in Review (CANCELREVIEW), else its Orders card(s); one note, on the first. */
  function expectLeave(ctx) {
    const m = M(); if (!m || !m.expect) return;
    if (ctx.mks.length) { ctx.mks.forEach((k, i) => m.expect(k, { to: toChip(ctx), plus: false, ms: FLY_MS, ttl: 120000, note: i === 0 ? noteSpec(ctx) : null })); return; }
    ctx.rows.forEach((r, i) => m.expect(keyOf(r), { to: toChip(ctx), plus: i === 0 ? "+1" : false, ms: FLY_MS, ttl: 120000, note: i === 0 ? noteSpec(ctx) : null }));
  }
  function forgetLeave(ctx) { const m = M(); if (!m || !m.expect || (flights.has(ctx.rid) && flights.get(ctx.rid) !== ctx)) return; for (const r of ctx.rows) m.expect(keyOf(r), { ttl: 1 }); for (const k of ctx.mks) m.expect(k, { ttl: 1 }); }
  /** The flight begins (the list took the card out): the chip answers with a ring, and keeps its old count until it lands; with the
   *  Orders view hidden (pressed in Review) the Orders tab answers instead: the ring, and a "Cancelled N" tag that rolls up on landing. */
  function start(ctx) {
    if (ctx.started) return; ctx.started = true;
    const c = chipEl("cancelled"); beacon(c); holdCount(ctx);
    if (c && c === tabEl()) tabTag(ctx);
    ctx.landT = setTimeout(() => land(ctx), Math.round(FLY_MS * .86) + 40);
  }
  function land(ctx) {
    if (ctx.landed) return; ctx.landed = true;
    const b = ctx.held; ctx.held = null; clearTimeout(ctx.holdT);
    const n = countOf();
    if (b && b.isConnected && n != null) roll(b, n - 1, n); else tickChip(1);
    if (ctx.tag && ctx.tag.isConnected && n != null) roll(ctx.tag.querySelector("b"), n - 1, n);
  }
  /** (CANCELREVIEW) The Orders tab's "Cancelled N" tag, just over its top right corner, on the motion layer: it shows the old count
   *  while the card flies, rolls to the new one as it lands, then fades. Transform and opacity only; none under reduced motion. */
  function tabTag(ctx) {
    const t = tabEl(); if (reduced() || !M() || !t || !t.getClientRects().length || ctx.tag) return;
    const r = t.getBoundingClientRect(), n = countOf();
    const tag = doc.createElement("span"); tag.className = "cnTabTag"; tag.setAttribute("aria-hidden", "true");
    tag.innerHTML = `Cancelled <b>${n == null ? "" : Math.max(0, n - 1)}</b>`;
    Object.assign(tag.style, { left: Math.round(r.right - 6) + "px", top: Math.max(2, Math.round(r.top - 9)) + "px" });
    M().layer(t).appendChild(tag); ctx.tag = tag;
    try { tag.animate([{ transform: "translate(-50%,6px) scale(.7)", opacity: 0 }, { transform: "translate(-50%,0) scale(1)", opacity: 1 }], { duration: 360, easing: "cubic-bezier(.3,1.3,.5,1)", fill: "both" }); } catch (_) {}
    ctx.tagT = setTimeout(() => dropTag(ctx), FLY_MS + 2400);
  }
  function dropTag(ctx) {
    clearTimeout(ctx.tagT); const tag = ctx.tag; ctx.tag = null; if (!tag) return;
    const kill = () => tag.remove();
    try { tag.animate([{ transform: "translate(-50%,0) scale(1)", opacity: 1 }, { transform: "translate(-50%,-6px) scale(.9)", opacity: 0 }], { duration: 320, easing: "ease-in", fill: "forwards" }).finished.then(kill, kill); } catch (_) { kill(); }
    setTimeout(kill, 900);
  }
  /** No flight was seen (reduced motion, the list was not on screen): the same answer, without the card in the air. */
  function plain(ctx) {
    if (ctx.started || ctx.plainDone) return; ctx.plainDone = true;
    const m = M();
    if (!m) { toastIf(WHERE(ctx.rid), "ok", 6000); return; }
    if (!reduced()) {
      m.pulse(() => chipEl("cancelled")); tickChip(1);
      if (chipEl("cancelled") === tabEl()) { beacon(tabEl()); tabTag(ctx); const n = countOf(); if (ctx.tag && n != null) setTimeout(() => { if (ctx.tag) roll(ctx.tag.querySelector("b"), n - 1, n); }, 380); }
    }
    m.note(Object.assign(() => chipEl("cancelled"), { cancelRid: ctx.rid }), noteSpec(ctx));
  }
  /** Whatever of this move is still on screen is taken away, whatever happened to its animation. */
  function sweep(ctx) {
    clearTimeout(ctx.landT); letGoCount(ctx); forgetLeave(ctx);
    const keys = new Set(ctx.rows.map(r => r.key)), mks = new Set(ctx.mks);
    for (const g of doc.querySelectorAll(".mGhost")) { const c = g.querySelector(".mCopy"); if (c && (keys.has(c.dataset.key) || mks.has(c.dataset.mkey))) g.remove(); }
    for (const x of doc.querySelectorAll(".cnBeacon")) x.remove();
    clearTimeout(ctx.tagT); if (ctx.tag) { ctx.tag.remove(); ctx.tag = null; }
    const b = doc.querySelector('#ordChips [data-pile="cancelled"] b'); if (b && b.classList.contains("cnTick")) { b.classList.remove("cnTick"); const n = countOf(); b.textContent = String(n == null ? "" : n); }
  }

  /* ── the cancel ── */
  const plainError = e => {
    const msg = String((e && (e.error || e.message)) || e || "").replace(/^Error:\s*/, "").trim();
    if ((root.navigator && navigator.onLine === false) || /failed to fetch|network|load failed|timed out|timeout|offline/i.test(msg)) return "Could not reach the server, so nothing was cancelled. The order is still on hold; try again when you are back online.";
    return `Not cancelled: ${msg || "something went wrong"}. The order is still on hold; try again.`;
  };
  async function cancel(rid) {
    rid = String(rid);
    css();   // (the chip's ring and ticking count; NOHOLDPILE: was put in by the On hold card's button)
    if (flights.has(rid)) return { ok: false, error: "busy" };
    const rows = rowsOf(rid); if (!rows.length) return { ok: false, error: "That order is not in the list any more." };
    if (!root.SheetWin || typeof SheetWin.takeOffOrder !== "function") { say(rid, "Cancelling is not ready on this page yet. Try again in a moment."); return { ok: false, error: "not ready" }; }
    say(rid, "");
    // (an order that Hold or Release hold is moving right now is left alone until that has finished)
    try { const o = root.OrderHold && OrderHold.status && OrderHold.status(rid); if (o && o.running === true) { say(rid, "This order is being moved right now. Try again when it has finished."); return { ok: false, error: "moving" }; } } catch (_) {}
    // the person's name: the small inline bar, once, when this computer has none (put away: nothing happens)
    let who = nameNow();
    if (!who) {
      if (asking.has(rid)) { try { CNEmployee.edit({ why: "Kept with the cancel on the order's timeline." }); } catch (_) {} return { ok: false, error: "name" }; }
      asking.add(rid);
      try { who = root.CNEmployee && CNEmployee.edit ? String(await CNEmployee.edit({ why: "Kept with the cancel on the order's timeline." }) || "").trim() : ""; } catch (_) { who = ""; }
      asking.delete(rid);
      if (!who) return { ok: false, error: "name" };
    }
    const rows2 = rowsOf(rid); if (!rows2.length) return { ok: false, error: "gone" };
    // (CANCELREVIEW: pressed in Review, the order's Review cards are what flies, by their names in that list; mks is empty from anywhere else)
    const mks = [...new Set(reviewCards(rid).map(n => n.dataset.mkey).filter(Boolean))];
    const ctx = { rid, who, rows: rows2.slice(), mks, snap: snapshot(rid, rows2, who), state: "working", started: false, landed: false, plainDone: false, held: null };
    flights.set(rid, ctx); paintBusy(rid); expectLeave(ctx);
    let res;
    try { res = await root.SheetWin.takeOffOrder({ orderId: rid, mode: "cancel", scope: "order", by: who, note: "" }); }
    catch (e) { res = { ok: false, error: (e && e.message) || String(e) }; }
    if (!res || !res.ok) {
      ctx.state = "failed"; forgetLeave(ctx); flights.delete(rid); paintBusy(rid); sweep(ctx);
      say(rid, plainError(res)); return res || { ok: false, error: "failed" };
    }
    ctx.state = "done"; flights.delete(rid); undoable.set(rid, ctx.snap); paintBusy(rid);
    // (the list has taken the card out by now and flown it; where it could not, the same answer is given plainly)
    setTimeout(() => plain(ctx), reduced() ? 120 : 1800);
    setTimeout(() => sweep(ctx), SWEEP_MS);
    return { ok: true };
  }

  /* ── Undo ── */
  /** What the cancel takes off the page, kept so Undo can put the same order back as it stood. */
  function snapshot(rid, rows, who) {
    const B = root.B || {}, run = B.run || null, pool = new Map();
    for (const r of rows) for (const id of r.poolIds || []) { const p = B.pool && B.pool.rows && B.pool.rows.get(id); if (p && !(root.Pool && Pool.sheetOf(id))) pool.set(id, p); }
    // (CANCELREVIEW: the order's "Held order" item in Review, put back by Undo; the names of the Review cards it had, which fly back out of the Orders tab)
    let rv = [], mks = []; try { rv = (root.Review && Review.items ? Review.items() : []).filter(x => /^held:/.test(String(x.key || "")) && String(x.rid) === String(rid)).map(x => Object.assign({}, x)); mks = reviewCards(rid).map(n => n.dataset.mkey).filter(Boolean); } catch (_) {}
    return { rid, who, rows: rows.slice(), pool, rv, mks, at: Date.now(), inRun: !!(run && Array.isArray(run.orders) && run.orders.some(x => String(x) === rid)), hold: run && run.holds && typeof run.holds === "object" && run.holds[rid] !== undefined ? run.holds[rid] : undefined };
  }
  /** The order's lines back in the lists, held (a line the cancel took off a sheet waits on hold: it is not put back on a sheet). */
  function revive(snap) {
    const B = root.B, rid = snap.rid; let n = 0;
    for (const r of snap.rows) {
      if (B.orders.byKey.has(r.key)) continue;
      if (!(r.poolIds || []).length && r.state !== "gone") { r.state = "held"; if (!r.hold) { r.hold = r.reason = `Taken off its sheet by ${snap.who}: cancelled, then put back on hold`; r.holdSeen = false; } r.heldAt = r.heldAt || Date.now(); }
      B.orders.rows.push(r); B.orders.byKey.set(r.key, r); n++;
    }
    for (const [id, p] of snap.pool) if (!B.pool.rows.has(id)) B.pool.rows.set(id, p);
    const run = B.run;
    if (run) {
      if (snap.inRun && Array.isArray(run.orders) && !run.orders.some(x => String(x) === rid)) run.orders.push(rid);
      if (snap.hold !== undefined && run.holds && typeof run.holds === "object") run.holds[rid] = snap.hold;
      run.lines = Object.fromEntries(Orders.rows().map(Orders.lineRecord));
    }
    try { Orders.interpretAll(); } catch (e) { console.warn("cancel undo: interpret", e); }
    try { for (const x of snap.rv || []) if (!Review.items().some(y => y.key === x.key)) Review.items().push(x); } catch (e) { console.warn("cancel undo: held item", e); }   // (CANCELREVIEW: its "Held order" item, as it was)
    try { Review.syncOrderItems(); } catch (e) { console.warn("cancel undo: review", e); }
    if (root.Session && Session.schedule) Session.schedule();
    return n;
  }
  function waitNote(text) {
    const m = M(); if (!m) return null;
    const n = m.note(() => chipEl("cancelled"), { text, ms: 45000 });
    if (n) { const s = doc.createElement("span"); s.className = "cnNoteSpin"; n.querySelector(".mNoteT").prepend(s); }
    return n;
  }
  function showCancelled(rid) { try { if (root.Orders && Orders.showCancelled) Orders.showCancelled(rid); } catch (e) { console.warn("cancel show", e); } }
  /** Adds Cancel Order beside the card's Release Hold on the Review card of an order on hold (CANCELREVIEW, Paul 20 Oct 2026: "only visible as part of the orders that are on Hold inside the Review
   *  list order view": Review draws the card, decides the order is on hold, then calls this with the order number, or one of its lines). One per card; the same .btn sm as Release Hold, plain, right after
   *  it (after a Custom Orders card's Release Hold too, RELORDER), else last among the card's buttons. (NOHOLDPILE put it on the held card in Open Orders; the Orders tab has none now.) */
  function mount(node, x) {
    try {
      const rid = String((x && typeof x === "object" ? x.order && x.order.receiptId : x) || "");
      if (!node || !rid || !rowsOf(rid).length) return;
      const acts = node.querySelector(".rowActions") || node.querySelector(".fixes"); if (!acts || acts.querySelector(".cnCancelBtn")) return;
      css();
      const rel = acts.querySelector(RELSEL);
      const b = doc.createElement("button"); b.type = "button"; b.className = "btn ghost sm cnCancelBtn"; b.textContent = LABEL;
      b.setAttribute("aria-label", `Cancel order ${rid}`);
      b.title = "Cancel this order. It moves to Orders \u203a Cancelled, where its record is kept and can be restored.";
      b.onclick = e => { e.stopPropagation(); e.preventDefault(); cancel(rid).catch(err => console.warn("cancel order", err)); };
      if (rel) rel.after(b); else acts.appendChild(b);
      node.dataset.rid = node.dataset.rid || rid;
      if (flights.has(rid)) paintBusy(rid);
      drawMsg(node, rid);
    } catch (e) { console.warn("cancel button", e); }
  }  // (NOHOLDPILE, Paul 20 Oct 2026: Orders › On hold is gone, "it competes with the existing Review tab": a held order is shown in Review)
  function showHeld(rid) {
    try { if (root.Review && Review.showHeld) Review.showHeld(rid); else if (typeof root.setMode === "function") root.setMode("review"); } catch (e) { console.warn("cancel show held", e); }
  }
  async function undo(rid) {
    rid = String(rid);
    css();
    const snap = undoable.get(rid); if (!snap) { toastIf(`Order ${rid} can be restored from Orders > Cancelled.`, "", 6000); return { ok: false, error: "nothing to undo" }; }
    if (snap.busy) return { ok: false, error: "busy" }; snap.busy = true;
    for (const k of snap.rows.map(keyOf).concat(snap.mks || [])) if (M() && M().expect) M().expect(k, { ttl: 1 });   // (a cancel's flight that was never played must not meet the order's next move)
    const m = M(), wait = waitNote(`Putting order ${rid} back on hold…`);
    try {
      if (!root.Cancelled || typeof Cancelled.restore !== "function") throw new Error("Restoring is not ready on this page yet");
      await Cancelled.restore(rid, snap.who || nameNow());
    } catch (e) {
      snap.busy = false; if (wait && wait.close) wait.close();
      const msg = plainError(e).replace(/^Not cancelled:/, "Not put back:").replace(/The order is still on hold; try again\.?/, "The order is still under Cancelled; try again.").replace(/so nothing was cancelled\. The order is still on hold;/, "so nothing was restored. The order is still under Cancelled;");
      if (m) m.note(() => chipEl("cancelled"), { text: msg, ms: 15000, actions: [{ label: "Try again", fn: () => { undo(rid).catch(e => console.warn("cancel undo", e)); } }, { label: "Show", fn: () => showCancelled(rid) }] }); else toastIf(msg, "bad", 8000);
      return { ok: false, error: e && e.message };
    }
    // the record is gone from Cancelled (kept in its history, and the timeline says it was restored): the lines are back
    if (m && m.expectIn) for (const r of snap.rows) m.expectIn(keyOf(r), { from: () => chipEl("cancelled"), ttl: 30000, ms: FLY_MS });
    if (m && m.expectIn) for (const k of snap.mks || []) m.expectIn(k, { from: () => chipEl("cancelled"), ttl: 30000, ms: FLY_MS });   // (CANCELREVIEW: its Review card flies back out of the Orders tab)
    undoable.delete(rid);
    // (REVIEWHELD, Paul 20 Oct 2026: undone from Review, its card is found where it comes back in Open: brought into view and marked, as a reopened card is (Review's RV.back))
    try { if (reviewCards(rid).length || (snap.mks || []).length) { const v = root.Review && Review.view && Review.view(); if (v) v.back = { keys: snap.rows.map(r => r.key), until: Date.now() + 8000 }; } } catch (_) {}
    let n = 0;
    try { n = revive(snap); } catch (e) { console.warn("cancel undo", e); }
    if (wait && wait.close) wait.close();
    try { Orders.renderNow(); } catch (_) { try { Orders.render(); } catch (__) {} }
    try { if (root.Review && Review.render) Review.render(); } catch (_) {}   // (CANCELREVIEW: its card is back in Review)
    tickChip(-1);
    const held = snap.rows.some(r => r.hold && r.state !== "gone");
    const card = () => reviewCards(rid)[0] || null, inRv = !!card();
    const text = n ? (held ? `Order ${rid} is back on hold${inRv ? " \u2014 its card is back in Review" : ""}` : `Order ${rid} is back in the lists`) : `Order ${rid} is no longer under Cancelled`;
    // (NOHOLDPILE: a held order is back in Open Orders, with its HELD chip; CANCELREVIEW: undone from Review, the note stands under its card there)
    const target = () => card() || chipEl("");
    if (m) {
      if (!reduced()) m.pulse(target);
      m.note(target, { text, ms: 7000, actions: held ? [{ label: "Show", title: "open Review at this order", fn: () => showHeld(rid) }] : [] });
    } else toastIf(text, "ok", 5000);
    if (root.B && B.run && root.RunCtl) RunCtl.save(B.run).catch(e => { console.warn("cancel undo: run", e); toastIf(`Order ${rid} is back; the run will save it with its next change (${e.message})`, "", 7000); });
    return { ok: true };
  }

  root.CancelUI = { mount, cancel, undo, busy: rid => flights.has(String(rid)), canUndo: rid => undoable.has(String(rid)), LABEL };
})(typeof window !== "undefined" ? window : globalThis);
