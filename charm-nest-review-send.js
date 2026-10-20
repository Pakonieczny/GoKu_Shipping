/* Charm Nest · Send to Review, the button on each row of the Orders tab (Paul, 12 Oct 2026: "In the order tab, I need you to add
   one button per each order on the right hand side that allows me to mark in order for review and added it to the review
   tab").

   One quiet button under "Qty · Ship by" on every order row (the list and the cards view alike). It does nothing by itself but
   call the two small APIs the Review tab keeps (CharmNestReviewMark: the model and the save; CharmNestReviewHold: the sheet
   a marked order sits on is held), through the thin wrapper below. Nothing is built here that those two own, and the page
   works unchanged until they are present: with no CharmNestReviewMark the button is not drawn at all.

   States, in the app's own button look (.btn.ghost.sm: hover, focus ring, pressed, disabled):
     "Send to Review"   the order is not in Review.
     spinner + the same words, disabled, aria-busy   the save is running (a small labelled spinner, no new words).
     "In Review"        the order is in the Review tab already (marked by a person here or on another computer, or one the
                        system put there): a press opens it there. It never adds a second time: the check is made before
                        the press and again inside the API.
   An order that is on a sheet stays on it (it is never lifted off or nested again); one plain line under the button says
   which sheet and that the sheet is held until the order is resolved in Review. A piece already released (its QR label
   printed, Complete Order) or on a sheet already cut cannot be held: one line says so and nothing is marked.

   The signed-in name is the one the app already holds (CNEmployee); when nobody is signed in the existing inline name bar
   opens, as for every other step that records a person. Nothing here reads or writes Firebase or Etsy, and nothing polls:
   the buttons are redrawn by the Orders list's own redraw and by the one change callback the mark API offers.

   Public: ReviewSend.mount(node, row) (the Orders list calls it for every row it draws, new or kept), ReviewSend.press(rid),
   ReviewSend.sync(), ReviewSend.refresh() (SENDREVIEW: once a frame with the Review tab's count, only a changed button is drawn), ReviewSend.busy(rid), ReviewSend.LABEL / ReviewSend.IN. */
(function (root) {
  "use strict";
  const doc = root.document;
  const LABEL = "Send to Review", IN = "In Review", MSG_MS = 15000;

  const CSS = [
    ".btn.ghost.revSendBtn{border-radius:7px;font:600 12px var(--sans);color:var(--ink70);cursor:pointer;align-self:flex-start}",
    ".btn.ghost.revSendBtn:hover:not(:disabled){color:var(--ink)}",
    ".btn.ghost.revSendBtn:disabled{cursor:default}",
    ".btn.ghost.revSendBtn.revIn{color:var(--ink);background:var(--paper2)}",
    ".btn.ghost.revSendBtn.revIn:hover:not(:disabled){background:var(--card2)}",
    ".revSendBtn .spin{margin-right:6px}",
    ".revSendMsg{display:block;max-width:36ch;margin:0;font:12px/1.4 var(--sans);color:var(--ink70)}"
  ].join("");
  function css() {
    try { if (doc.getElementById("cnReviewSendCss")) return; const s = doc.createElement("style"); s.id = "cnReviewSendCss"; s.textContent = CSS; doc.head.appendChild(s); } catch (_) {}
  }

  /* ── the thin wrapper: everything the button needs from the other pieces, each feature-detected ── */
  const MARK = () => root.CharmNestReviewMark || null;
  const HOLD = () => root.CharmNestReviewHold || null;
  const api = {
    ready() { const m = MARK(); return !!(m && typeof m.mark === "function" && typeof m.isMarked === "function"); },
    marked(rid) { try { return !!MARK().isMarked(rid); } catch (_) { return false; } },
    mark(rid, by) { return Promise.resolve().then(() => MARK().mark(rid, { by })); },
    // an order a person did not mark can be in Review all the same (the system put it there): the Review tab's own list says so (a person's mark is isMarked's: the mark API owns when it is open or resolved)
    // (SENDREVIEW, Paul 20 Oct 2026: "This button should be directly linked and communicate with the orders that are already in the Review tab": asked of Review's Open itself, Review.openOrders, the
    //  lists its count is made of; it asked Review.items() alone, the decisions, and a custom order waiting on its own designs or Complete Order is a card of Open that is no decision, so its
    //  card said "Send to Review" for an order already in Review. Read once for a drawing: kept to the end of the turn, as the Orders list's own action memo)
    natural(rid, row) {
      try {
        const R = root.Review; if (!R) return null;
        if (typeof R.openOrders === "function") return openNow().get(String(rid)) || null;
        if (typeof R.items !== "function") return null;
        const own = new Set(rowKeys(rid)); if (row && row.key) own.add(row.key);
        return R.items().find(it => it && it.kind !== "reviewMark" && !it.done && !it.decided && !/^(eng|held):/.test(String(it.key || "")) && (it.rows && it.rows.length ? it.rows : it.row ? [it.row] : []).some(r => r && own.has(r.key) && r.state !== "gone")) || null;
      } catch (_) { return null; }
    },
    inReview(rid, row) { return api.marked(rid) || !!api.natural(rid, row); },
    open(rid, row) {
      // a person's mark: the mark API's own card; one the system put there: the Review tab's own card for it
      try { const m = MARK(); if (m && typeof m.open === "function" && api.marked(rid) && m.open(rid) !== false) return true; } catch (e) { console.warn("review send: open", e); }
      try {
        const it = api.natural(rid, row);
        if (it && root.Review && Review.showCard) { Review.showCard(typeof Review.mkeyOf === "function" ? Review.mkeyOf(it) : it.kind === "customOrder" ? "cu:" + String(it.key).replace(/^[a-z]+:/, "") : String(it.key), "open"); return true; }   // (SENDREVIEW: the card's own key in the list, Review.mkeyOf)
      } catch (e) { console.warn("review send: show", e); }
      try { if (typeof root.setMode === "function") { root.setMode("review"); return true; } } catch (_) {}
      return false;
    },
    onChange(fn) { try { const m = MARK(); if (m && typeof m.onChange === "function") return { api: m, off: m.onChange(fn) }; } catch (_) {} return null; },
    // REVHOLD: why this order cannot be held (a piece already released, a sheet already cut or Completed), "" when it can
    cannotHold(rid) {
      try { const h = HOLD(); if (!h || typeof h.canHold !== "function") return ""; const c = h.canHold(rid); return c && c.ok === false ? String(c.why || "This order can no longer be held.") : ""; } catch (_) { return ""; }
    },
    // the sheets the order's open Review holds, as REVHOLD names them: [{ sheetId, name, setId }]
    sheets(rid) {
      try {
        const h = HOLD(); if (!h) return [];
        let l = typeof h.sheetsHeldBy === "function" ? h.sheetsHeldBy(rid) : [];
        if ((!l || !l.length) && typeof h.canHold === "function") { const c = h.canHold(rid); l = c && c.ok ? c.sheets : []; }
        return Array.isArray(l) ? l.filter(x => x && x.name) : [];
      } catch (_) { return []; }
    }
  };

  /* ── what the page knows ── */
  let openMemo = null;   // (SENDREVIEW: Review's Open by order number, read once for a drawing and let go at the end of the turn)
  const openNow = () => {
    if (openMemo) return openMemo;
    const m = openMemo = root.Review.openOrders();
    Promise.resolve().then(() => { if (openMemo === m) openMemo = null; });
    return m;
  };
  const rowsOf = rid => { try { return (root.Orders ? Orders.rows() : []).filter(r => String(r.order.receiptId) === String(rid) && r.state !== "gone"); } catch (_) { return []; } };
  const rowKeys = rid => rowsOf(rid).map(r => r.key);
  const cardsOf = rid => [...doc.querySelectorAll("#ordItems [data-rid]")].filter(n => n.dataset.rid === String(rid));
  const nameNow = () => { try { return String((root.CNEmployee && CNEmployee.name && CNEmployee.name()) || "").trim(); } catch (_) { return ""; } };
  /** "set-… · SS Draft 2": the same words the row itself shows for where the piece is; "" when it is on no sheet. */
  function whereOf(rid, row) {
    try {
      const l = api.sheets(rid);
      if (l.length) return l.map(x => x.setId ? `${x.setId} · ${x.name}` : String(x.name)).join(" + ");
      for (const r of [row].concat(rowsOf(rid))) {
        const w = r && root.Orders && Orders.placeOf ? Orders.placeOf(r) : null;
        if (w && w.sheet) return w.set ? `${w.set} · ${w.sheet}` : String(w.sheet);
      }
    } catch (_) {}
    return "";
  }

  /* ── the plain line under the button ── */
  const busyRids = new Set();
  const messages = new Map(), msgT = new Map();   // rid → { text, at } · the timer that takes it away
  function say(rid, text) {
    rid = String(rid);
    clearTimeout(msgT.get(rid)); msgT.delete(rid);
    if (text) { messages.set(rid, { text, at: Date.now() }); msgT.set(rid, setTimeout(() => say(rid, ""), MSG_MS)); } else messages.delete(rid);
    for (const n of cardsOf(rid)) drawMsg(n, rid);
  }
  function drawMsg(n, rid) {
    const b = n.querySelector(".revSendBtn"); if (!b) return;
    let line = n.querySelector(".revSendMsg");
    const m = messages.get(String(rid));
    if (!m) { if (line) line.remove(); return; }
    if (!line) { line = doc.createElement("span"); line.className = "revSendMsg"; line.setAttribute("role", "status"); b.after(line); }
    if (line.textContent !== m.text) line.textContent = m.text;
  }

  /* ── the button ── */
  function paint(b, rid, row) {
    const busy = busyRids.has(String(rid)), inRev = !busy && api.inReview(rid, row), state = busy ? "busy" : inRev ? "in" : "idle";
    if (b.dataset.revState === state) return;
    b.dataset.revState = state;
    b.disabled = busy;
    b.classList.toggle("revIn", inRev);
    if (busy) {
      const sp = doc.createElement("span"); sp.className = "spin"; sp.setAttribute("aria-hidden", "true");
      b.setAttribute("aria-busy", "true"); b.replaceChildren(sp, doc.createTextNode(LABEL));
      b.title = "";
    } else {
      b.removeAttribute("aria-busy"); b.replaceChildren(doc.createTextNode(inRev ? IN : LABEL));
      b.title = inRev ? "Open it in Review" : "Put this order in the Review tab";
    }
    b.setAttribute("aria-label", inRev ? `Order ${rid} is in Review. Open it there.` : `Send order ${rid} to Review`);
  }
  /** Every drawn button says what is true now (after a press, a change from another screen, a redraw). */
  function sync() {
    try {
      for (const b of doc.querySelectorAll("#ordItems .revSendBtn")) {
        const n = b.closest("[data-rid]"); if (!n) continue;
        b.dataset.revState = ""; paint(b, n.dataset.rid, null);
      }
    } catch (_) {}
  }

  /** (SENDREVIEW, Paul 20 Oct 2026: "If the status of a given order updates from anywhere in the application, then these buttons and UI must react to the change/update in near real-time":
   *  called once a frame with the Review tab's count (LiveStrip.now, which every Review drawing and every Orders drawing asks for): Open is read once and only a button whose state
   *  changed is drawn again (paint keeps one that reads the same). Nothing is read from the cloud and nothing polls; the Orders tab out of sight is drawn when it is shown.) */
  function refresh() {
    try {
      const v = doc.getElementById("ordersView"); if (!v || v.classList.contains("hidden")) return;
      const bs = doc.querySelectorAll("#ordItems .revSendBtn"); if (!bs.length) return;
      openMemo = null;
      for (const b of bs) { const n = b.closest("[data-rid]"); if (n) paint(b, n.dataset.rid, null); }
    } catch (_) {}
  }

  /** The Orders list draws the row, then calls this (for kept rows too: it only adds what is missing and repaints the state). */
  function mount(node, row) {
    try {
      if (!node || !row || row.state === "gone" || !api.ready()) { const old = node && node.querySelector && node.querySelector(".revSendBtn"); if (old && !api.ready()) old.remove(); return; }
      const acts = node.querySelector(".rowActions"); if (!acts) return;
      subscribe();
      const rid = String(row.order.receiptId);
      let b = acts.querySelector(".revSendBtn");
      if (!b) {
        css();
        b = doc.createElement("button"); b.type = "button"; b.className = "btn ghost sm revSendBtn";
        b.onclick = e => { e.stopPropagation(); e.preventDefault(); press(rid, row).catch(err => console.warn("send to review", err)); };
        const facts = acts.querySelector(".rowFacts");
        if (facts) facts.after(b); else acts.appendChild(b);
        node.dataset.rid = node.dataset.rid || rid;
      }
      b.dataset.rid = rid;
      paint(b, rid, row);
      drawMsg(node, rid);
    } catch (e) { console.warn("send to review button", e); }
  }

  /* ── the press ── */
  const plainError = e => {
    const msg = String((e && (e.error || e.message)) || e || "").replace(/^Error:\s*/, "").trim();
    if ((root.navigator && navigator.onLine === false) || /failed to fetch|network|load failed|timed out|timeout|offline/i.test(msg)) return "Could not reach the server, so nothing was sent. Try again when you are back online.";
    return `Not sent: ${msg || "something went wrong"}. Try again.`;
  };
  async function press(rid, row) {
    rid = String(rid);
    if (busyRids.has(rid)) return { ok: false, error: "busy" };
    if (!api.ready()) { say(rid, "Review is not ready on this page yet. Try again in a moment."); return { ok: false, error: "not ready" }; }
    // already in Review: open it there, never add it a second time
    if (api.inReview(rid, row)) { api.open(rid, row); return { ok: true, opened: true }; }
    say(rid, "");
    // a piece already released, or a sheet already cut or Completed, cannot be held: one line, nothing is marked
    const why = api.cannotHold(rid); if (why) { say(rid, why); return { ok: false, error: "cannot hold", why }; }
    const who = nameNow() || (root.CNEmployee && CNEmployee.need ? String(await CNEmployee.need() || "").trim() : "");
    if (!who) return { ok: false, error: "name" };
    if (busyRids.has(rid)) return { ok: false, error: "busy" };
    busyRids.add(rid); sync();
    let res;
    try { res = await api.mark(rid, who); }
    catch (e) { res = { ok: false, error: (e && e.message) || String(e) }; }
    busyRids.delete(rid);
    if (res === false || (res && res.ok === false)) { sync(); say(rid, plainError(res)); return res || { ok: false, error: "failed" }; }
    sync();
    const sheet = whereOf(rid, row), many = sheet.includes(" + ");
    if (sheet) say(rid, `Stays on ${sheet}. ${many ? "Those sheets are" : "That sheet is"} held until this is resolved in Review.`);
    return { ok: true, sheet };
  }

  // (the mark API's one change callback: registered the first time a button is drawn, when the script that offers it is surely loaded; again if the API object is ever replaced)
  let sub = null;
  const subscribe = () => {
    const m = MARK(); if (!m || (sub && sub.api === m)) return;
    try { if (sub && typeof sub.off === "function") sub.off(); } catch (_) {}
    sub = api.onChange(sync);
  };
  root.ReviewSend = { mount, press, sync, refresh, busy: rid => busyRids.has(String(rid)), LABEL, IN };
})(typeof window !== "undefined" ? window : globalThis);
