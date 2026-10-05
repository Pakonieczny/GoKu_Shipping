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

   Public: CancelUI.mount(node, row) (called by the Orders list for a held row on the On hold pile), CancelUI.cancel(rid),
   CancelUI.undo(rid), CancelUI.busy(rid). */
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
    ".btn.ghost.cnCancelBtn{border-radius:7px;font:600 12px var(--sans);color:var(--ink70);cursor:pointer}",
    ".btn.ghost.cnCancelBtn:hover:not(:disabled){color:var(--ink)}",
    ".btn.ghost.cnCancelBtn:disabled{cursor:default}",
    ".cnCancelBtn .spin{margin-right:6px}",
    ".ocard.cnBusy,.orderListRow.cnBusy{transform:translateY(-2px);transition:transform .24s cubic-bezier(.2,.8,.2,1)}",
    "@media (prefers-reduced-motion:reduce){.ocard.cnBusy,.orderListRow.cnBusy{transform:none;transition:none}}",
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
  const onHoldPile = () => { try { return !!(root.Orders && Orders.view && Orders.view().pile === "hold"); } catch (_) { return false; } };
  const rowsOf = rid => (root.Orders ? Orders.rows() : []).filter(r => String(r.order.receiptId) === String(rid) && r.state !== "gone");
  /** The pile's chip; the Orders tab button when the chip is not drawn or not showing (the tab is always in the top bar). */
  const chipEl = p => { const c = doc.querySelector(`#ordChips [data-pile="${p}"]`); return c && c.getClientRects().length ? c : doc.querySelector('#modeSeg [data-mode="orders"]'); };
  const chipB = p => { const b = doc.querySelector(`#ordChips [data-pile="${p}"] b`); return b && b.getClientRects().length ? b : null; };
  const cardsOf = rid => [...doc.querySelectorAll("#ordItems [data-rid]")].filter(n => n.dataset.rid === String(rid));
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
      const rel = n.querySelector(".relHold:not([data-gate])");
      if (rel) { if (on && !rel.disabled) { rel.disabled = true; rel.dataset.cnOff = "1"; } else if (!on && rel.dataset.cnOff) { rel.disabled = false; delete rel.dataset.cnOff; } }
    }
  }
  function say(rid, text) {
    rid = String(rid);
    if (text) messages.set(rid, { text, at: Date.now() }); else { messages.delete(rid); clearTimeout(msgT.get(rid)); msgT.delete(rid); }
    for (const n of cardsOf(rid)) drawMsg(n, rid);
  }
  function drawMsg(n, rid) {
    const wrap = n.querySelector(".cnHoldBtns"); if (!wrap) return;
    let line = n.querySelector(".cnCancelMsg");
    const m = messages.get(String(rid)), live = m && Date.now() - m.at < MSG_MS;
    if (!live) { if (m) messages.delete(String(rid)); if (line) line.remove(); return; }
    if (!line) { line = doc.createElement("span"); line.className = "cnCancelMsg"; line.setAttribute("role", "alert"); wrap.after(line); }
    if (line.textContent !== m.text) line.textContent = m.text;
    clearTimeout(msgT.get(rid)); msgT.set(rid, setTimeout(() => { msgT.delete(rid); say(rid, ""); }, Math.max(500, MSG_MS - (Date.now() - m.at))));
  }

  /** Adds the button beside Release hold on a card of the On hold pile (Orders draws the card, then calls this). */
  function mount(node, row) {
    try {
      if (!node || !row || !row.hold || row.state === "gone" || !onHoldPile()) return;
      const acts = node.querySelector(".rowActions"); if (!acts || acts.querySelector(".cnCancelBtn")) return;
      css();
      const rid = String(row.order.receiptId), rel = acts.querySelector(".relHold:not([data-gate])");
      let wrap = rel && rel.parentElement && rel.parentElement.classList.contains("cnHoldBtns") ? rel.parentElement : null;
      if (!wrap) { wrap = doc.createElement("div"); wrap.className = "cnHoldBtns"; if (rel) { rel.replaceWith(wrap); wrap.appendChild(rel); } else acts.appendChild(wrap); }
      const b = doc.createElement("button"); b.type = "button"; b.className = "btn ghost sm cnCancelBtn"; b.textContent = LABEL;
      b.setAttribute("aria-label", `Cancel order ${rid}`);
      b.title = "Cancel this order. Its record is kept under Cancelled, where it can be restored.";
      b.onclick = e => { e.stopPropagation(); e.preventDefault(); cancel(rid).catch(err => console.warn("cancel order", err)); };
      wrap.appendChild(b);
      node.dataset.rid = node.dataset.rid || rid;
      if (flights.has(rid)) paintBusy(rid);
      drawMsg(node, rid);
    } catch (e) { console.warn("cancel button", e); }
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

  /* ── the move to Cancelled, by the Orders list's own update (Motion.expect → reconcile → fly) ── */
  const toChip = ctx => Object.assign(() => (ctx.state === "failed" ? null : (ctx.started || start(ctx), chipEl("cancelled"))), { cancelRid: ctx.rid });
  function noteSpec(ctx) {
    return { text: `Order ${ctx.rid} cancelled · kept under Cancelled`, ms: UNDO_MS, actions: [
      { label: "Undo", title: "put it back on hold", fn: () => { undo(ctx.rid).catch(e => console.warn("cancel undo", e)); } },
      { label: "Show", title: "open Cancelled at this order", fn: () => showCancelled(ctx.rid) }] };
  }
  function expectLeave(ctx) {
    const m = M(); if (!m || !m.expect) return;
    ctx.rows.forEach((r, i) => m.expect(keyOf(r), { to: toChip(ctx), plus: i === 0 ? "+1" : false, ms: FLY_MS, ttl: 120000, note: i === 0 ? noteSpec(ctx) : null }));
  }
  function forgetLeave(ctx) { const m = M(); if (!m || !m.expect || (flights.has(ctx.rid) && flights.get(ctx.rid) !== ctx)) return; for (const r of ctx.rows) m.expect(keyOf(r), { ttl: 1 }); }
  /** The flight begins (the list took the card out): the chip answers with a ring, and keeps its old count until it lands. */
  function start(ctx) {
    if (ctx.started) return; ctx.started = true;
    const c = chipEl("cancelled"); beacon(c); holdCount(ctx);
    ctx.landT = setTimeout(() => land(ctx), Math.round(FLY_MS * .86) + 40);
  }
  function land(ctx) {
    if (ctx.landed) return; ctx.landed = true;
    const b = ctx.held; ctx.held = null; clearTimeout(ctx.holdT);
    const n = countOf();
    if (b && b.isConnected && n != null) roll(b, n - 1, n); else tickChip(1);
  }
  /** No flight was seen (reduced motion, the list was not on screen): the same answer, without the card in the air. */
  function plain(ctx) {
    if (ctx.started || ctx.plainDone) return; ctx.plainDone = true;
    const m = M();
    if (!m) { toastIf(`Order ${ctx.rid} cancelled · kept under Cancelled`, "ok", 6000); return; }
    if (!reduced()) { m.pulse(() => chipEl("cancelled")); tickChip(1); }
    m.note(Object.assign(() => chipEl("cancelled"), { cancelRid: ctx.rid }), noteSpec(ctx));
  }
  /** Whatever of this move is still on screen is taken away, whatever happened to its animation. */
  function sweep(ctx) {
    clearTimeout(ctx.landT); letGoCount(ctx); forgetLeave(ctx);
    const keys = new Set(ctx.rows.map(r => r.key));
    for (const g of doc.querySelectorAll(".mGhost")) { const c = g.querySelector(".mCopy"); if (c && keys.has(c.dataset.key)) g.remove(); }
    for (const x of doc.querySelectorAll(".cnBeacon")) x.remove();
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
    const ctx = { rid, who, rows: rows2.slice(), snap: snapshot(rid, rows2, who), state: "working", started: false, landed: false, plainDone: false, held: null };
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
    return { rid, who, rows: rows.slice(), pool, at: Date.now(), inRun: !!(run && Array.isArray(run.orders) && run.orders.some(x => String(x) === rid)), hold: run && run.holds && typeof run.holds === "object" && run.holds[rid] !== undefined ? run.holds[rid] : undefined };
  }
  /** The order's lines back in the lists, held (a line the cancel took off a sheet waits on hold: it is not put back on a sheet). */
  function revive(snap) {
    const B = root.B, rid = snap.rid; let n = 0;
    for (const r of snap.rows) {
      if (B.orders.byKey.has(r.key)) continue;
      if (!(r.poolIds || []).length && r.state !== "gone") { r.state = "held"; if (!r.hold) { r.hold = r.reason = `Taken off its sheet by ${snap.who}: cancelled, then put back on hold`; } r.heldAt = r.heldAt || Date.now(); }
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
  function showHeld(rid) {
    try { if (typeof root.setMode === "function" && doc.getElementById("ordersView") && doc.getElementById("ordersView").classList.contains("hidden")) root.setMode("orders"); Orders.showPile("hold", rid); } catch (e) { console.warn("cancel show held", e); }
  }
  async function undo(rid) {
    rid = String(rid);
    const snap = undoable.get(rid); if (!snap) { toastIf(`Order ${rid} can be restored from Orders > Cancelled.`, "", 6000); return { ok: false, error: "nothing to undo" }; }
    if (snap.busy) return { ok: false, error: "busy" }; snap.busy = true;
    for (const r of snap.rows) if (M() && M().expect) M().expect(keyOf(r), { ttl: 1 });   // (a cancel's flight that was never played must not meet the order's next move)
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
    undoable.delete(rid);
    let n = 0;
    try { n = revive(snap); } catch (e) { console.warn("cancel undo", e); }
    if (wait && wait.close) wait.close();
    try { Orders.renderNow(); } catch (_) { try { Orders.render(); } catch (__) {} }
    tickChip(-1);
    const held = snap.rows.some(r => r.hold && r.state !== "gone");
    const text = n ? (held ? `Order ${rid} is back on hold` : `Order ${rid} is back in the lists`) : `Order ${rid} is no longer under Cancelled`;
    const target = () => chipEl(held ? "hold" : "");
    if (m) {
      if (!reduced()) m.pulse(target);
      m.note(target, { text, ms: 7000, actions: held ? [{ label: "Show", title: "open On hold at this order", fn: () => showHeld(rid) }] : [] });
    } else toastIf(text, "ok", 5000);
    if (root.B && B.run && root.RunCtl) RunCtl.save(B.run).catch(e => { console.warn("cancel undo: run", e); toastIf(`Order ${rid} is back; the run will save it with its next change (${e.message})`, "", 7000); });
    return { ok: true };
  }

  root.CancelUI = { mount, cancel, undo, busy: rid => flights.has(String(rid)), canUndo: rid => undoable.has(String(rid)), LABEL };
})(typeof window !== "undefined" ? window : globalThis);
