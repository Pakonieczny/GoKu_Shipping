/* Charm Nest · Hold Order, the button on each row of the Orders tab (Paul, 12 Oct 2026: "In the order tab, I need you to add
   one button per each order on the right hand side that allows me to mark in order for review and added it to the review
   tab").
   (HOLDORDER, Paul 20 Oct 2026: "Rename the 'Send to Review' button everywhere in the application to 'Hold Order'. Also update the Seal text to better reflect
   the new button name and the function it performs. Ensure the text inside the button is white for good contrast.": the button says "Hold Order", in white on
   its own solid colour (--holdOrder, charm-nest-1.html); the order still waits in the Review tab ("In Review" is unchanged) and its timeline seal reads
   ORDER HELD. The stored record (kind reviewMark, key rid + "_review", the note ids rvmark.*) is unchanged, so an older mark reads and draws the same.)

   One quiet button under "Qty · Ship by" on every order row (the list and the cards view alike). It does nothing by itself but
   call the two small APIs the Review tab keeps (CharmNestReviewMark: the model and the save; CharmNestReviewHold: the sheet
   a marked order sits on is held), through the thin wrapper below. Nothing is built here that those two own, and the page
   works unchanged until they are present: with no CharmNestReviewMark the button is not drawn at all.

   States, in the app's own button look (.btn.ghost.sm: hover, focus ring, pressed, disabled):
     "Hold Order"       the order is not in Review (HOLDORDER: it was "Send to Review").
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
   ReviewSend.button(rid, row) / ReviewSend.buttonHtml(rid) (HOLDORDER: the ONE way the button is made, for any other place that shows Hold Order: an element ready
   to put in, or its markup, whose press is taken by one listener on the page),
   ReviewSend.sync(), ReviewSend.refresh() (SENDREVIEW: once a frame with the Review tab's count, only a changed button is drawn), ReviewSend.busy(rid), ReviewSend.LABEL / ReviewSend.IN.

   (HOLDBTN, Paul 20 Oct 2026: "Remove the 'Hold' button everywhere in the app and replace it with the 'Send to Review' and ensure it has the same look, feel, and functionality":
    the orange Hold stood on the order window's piece rows (HoldUI.slot / HoldUI.fill). Those rows now ask this file for its own button: ReviewSend.slot(ctx) -> one span in the
    row's markup, ReviewSend.fill(host) -> each span becomes the element button() makes for the Orders rows too (same element, paint, press, seal and line under it).
    The one addition, opts.offSheets (every button that stands where Hold stood): once the order is marked, its pieces on a sheet not yet laser-cut come off it, through the
    old Hold's own engine (HoldUI.takeOff -> OrderHold.run), so a held order never stays on a sheet that gets cut. Marking alone does not do it: REVHOLD keeps the pieces where
    they are and holds the sheet. A piece on a cut sheet is refused before anything is marked, as before.) */
(function (root) {
  "use strict";
  const doc = root.document;
  const LABEL = "Hold Order", IN = "In Review", MSG_MS = 15000;   // (HOLDORDER, Paul 20 Oct 2026: the button's words; "In Review" stays)
  const TIP = "Hold this order: it waits in the Review tab, and its sheet with it, until it is released there";

  const CSS = [
    ".btn.ghost.revSendBtn{border-radius:7px;font:600 12px var(--sans);color:var(--ink70);cursor:pointer;align-self:flex-start}",
    // (HOLDORDER, Paul 20 Oct 2026: "Ensure the text inside the button is white for good contrast": Hold Order is white on its own solid colour, --holdOrder (a deep plum beside Release Hold's
    //  orange --relHold, charm-nest-1.html; white on it is far past AA). "In Review" keeps the quiet look below: it is a state that opens the card, not the action)
    ".btn.ghost.revSendBtn:not(.revIn){background:var(--holdOrder,#ea580c);border-color:var(--holdOrder,#ea580c);color:#fff}",
    ".btn.ghost.revSendBtn:not(.revIn):hover:not(:disabled){background:var(--holdOrderHover,#c2410c);border-color:var(--holdOrderHover,#c2410c);color:#fff}",
    ".btn.ghost.revSendBtn:not(.revIn):disabled{opacity:.75;color:#fff}",
    ".btn.ghost.revSendBtn:disabled{cursor:default}",
    ".btn.ghost.revSendBtn.revIn{color:var(--ink);background:var(--paper2)}",
    ".btn.ghost.revSendBtn.revIn:hover:not(:disabled){background:var(--card2)}",
    ".revSendBtn .spin{margin-right:6px}",
    ".revSendMsg{display:block;max-width:36ch;margin:0;font:12px/1.4 var(--sans);color:var(--ink70)}",
    // (HOLDPROG, Paul 27 Oct 2026: "no progress bar, nothing to let us know what was happening and when it would've finish and actually did it complete successfully": a thin bar and the step
    //  it is on, with the seconds, under the button; it ends in one plain line that says it is done (a tick) or what went wrong)
    ".revSendProg{display:grid;gap:4px;min-width:180px;max-width:40ch;margin:4px 0 0;font:12px/1.35 var(--sans);color:var(--ink70)}",
    ".revSendProg .rpBar{position:relative;height:5px;border-radius:3px;background:var(--paper2,#ece7dc);overflow:hidden}",
    ".revSendProg .rpBar>i{position:absolute;inset:0;transform-origin:0 50%;transform:scaleX(var(--p,0));background:var(--holdOrder,#ea580c);border-radius:3px;transition:transform .35s cubic-bezier(.2,.8,.2,1)}",
    ".revSendProg:not([data-end]) .rpBar>i::after{content:'';position:absolute;inset:0;background:linear-gradient(90deg,transparent,rgba(255,255,255,.55),transparent);transform:translateX(-100%);animation:rpShine 1.2s linear infinite}",
    "@keyframes rpShine{to{transform:translateX(100%)}}",
    ".revSendProg[data-end=ok] .rpBar>i{background:var(--sage,#5f7a5b)}.revSendProg[data-end=bad] .rpBar>i{background:var(--clay,#b5523b)}",
    ".revSendProg .rpTx{display:flex;gap:6px;align-items:baseline;min-width:0}.revSendProg .rpTx b{font-weight:600;color:var(--ink)}.revSendProg .rpTx small{margin-left:auto;font:11px var(--mono);color:var(--ink45);white-space:nowrap}",
    ".revSendProg[data-end=ok] .rpTx b::before{content:'\\2713  ';color:var(--sage,#5f7a5b)}",
    "@media (prefers-reduced-motion:reduce){.revSendProg .rpBar>i{transition:none}.revSendProg .rpBar>i::after{animation:none}}"
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
    // (HOLDBTN, Paul 20 Oct 2026: the old Hold's path, HoldUI.takeOff: its pieces on sheets not yet cut come off them; null when that path is not on the page)
    takeOff(rid, by, onStep) { const H = root.HoldUI; return H && typeof H.takeOff === "function" ? Promise.resolve().then(() => H.takeOff(rid, { name: by, onStep })).catch(e => ({ ok: false, off: 0, error: (e && e.message) || String(e) })) : Promise.resolve(null); },
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
  // (HOLDBTN, Paul 20 Oct 2026: a button drawn where Hold stood (data-rev-host) has its line under it in its own cell)
  const cardsOf = rid => [...doc.querySelectorAll("#ordItems [data-rid]")].filter(n => n.dataset.rid === String(rid))
    .concat([...doc.querySelectorAll(".revSendBtn[data-rev-host]")].filter(b => b.dataset.rid === String(rid) && b.parentElement).map(b => b.parentElement));
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

  /* ── HOLDPROG: the progress of a hold under its button (the bar, the step it is on, the seconds) ── */
  const progs = new Map();   // rid → { p (0..1), text, t0, end: "" | "ok" | "bad", endAt }
  let progTick = 0;
  const secsOf = g => Math.max(0, Math.round(((g.endAt || Date.now()) - g.t0) / 1000));
  function prog(rid, patch) {
    rid = String(rid);
    const g = Object.assign(progs.get(rid) || { p: 0, text: "", t0: Date.now(), end: "" }, patch || {});
    if (patch && patch.end) g.endAt = Date.now();
    progs.set(rid, g);
    for (const n of cardsOf(rid)) drawProg(n, rid);
    if ([...progs.values()].some(x => !x.end)) { if (!progTick) progTick = setInterval(() => { for (const [r] of progs) for (const n of cardsOf(r)) drawProg(n, r); if (![...progs.values()].some(x => !x.end)) { clearInterval(progTick); progTick = 0; } }, 1000); }
    if (g.end) setTimeout(() => { if (progs.get(rid) === g) { progs.delete(rid); for (const n of cardsOf(rid)) drawProg(n, rid); } }, MSG_MS);
  }
  function drawProg(n, rid) {
    const b = n.querySelector(".revSendBtn"); if (!b) return;
    let el = n.querySelector(".revSendProg"); const g = progs.get(String(rid));
    if (!g) { if (el) el.remove(); return; }
    if (!el) { el = doc.createElement("span"); el.className = "revSendProg"; el.setAttribute("role", "progressbar"); el.setAttribute("aria-valuemin", "0"); el.setAttribute("aria-valuemax", "100"); el.innerHTML = '<span class="rpBar"><i></i></span><span class="rpTx"><b></b><small></small></span>'; (n.querySelector(".revSendMsg") || b).after(el); }
    const pct = Math.round(Math.max(0, Math.min(1, g.end === "ok" ? 1 : g.p)) * 100);
    el.style.setProperty("--p", String(pct / 100)); el.setAttribute("aria-valuenow", String(pct)); el.setAttribute("aria-valuetext", g.text);
    if (g.end) el.dataset.end = g.end; else delete el.dataset.end;
    const tb = el.querySelector(".rpTx b"), ts = el.querySelector(".rpTx small"), secs = `${secsOf(g)} s`;
    if (tb.textContent !== g.text) tb.textContent = g.text; if (ts.textContent !== secs) ts.textContent = secs;
  }
  /** The engine's steps (OrderHold.run) as the bar and its words: each sheet is its share; on a sheet, taking the pieces off is the most of it, then the free space is filled, then its label. */
  function progFeed(rid) {
    const sheets = [], names = new Map(), part = new Map(); let fills = 0;
    const word = id => names.get(id) || "the sheet";
    const at = () => sheets.length ? sheets.reduce((a, id) => a + (part.get(id) || 0), 0) / sheets.length : 0;
    const go = (id, p, text) => { if (id != null) part.set(id, Math.max(part.get(id) || 0, p)); prog(rid, { p: 0.15 + 0.85 * at(), text }); };
    return s => {
      if (!s || typeof s !== "object") return;
      const id = s.sheetId != null ? s.sheetId : s.toSheetId;
      if (s.type === "start") { for (const x of s.sheets || []) if (!sheets.includes(x)) sheets.push(x); go(null, 0, sheets.length ? `Taking its pieces off ${sheets.length > 1 ? sheets.length + " sheets" : "the sheet"}…` : "Holding it…"); }
      else if (s.type === "wait") go(null, 0, `Waiting: ${s.why}…`);
      else if (s.type === "sheetBegin") { if (s.label) names.set(id, s.label); if (!sheets.includes(id)) sheets.push(id); go(id, 0.05, `Taking it off ${word(id)}…`); }
      else if (s.type === "removed") go(id, 0.45, `Taken off ${word(id)}`);
      else if (s.type === "fillBegin") go(id, 0.5, `Filling the free space on ${word(id)}…`);
      else if (s.type === "fillPlaced") { fills++; go(id, Math.min(0.85, (part.get(id) || 0.5) + 0.12), `Filling ${word(id)}: order ${s.rid} moved in`); }
      else if (s.type === "fillSkipped") go(id, 0.85, `${word(id)}: the free space stays free`);
      else if (s.type === "qr") go(id, 0.9, `New QR label for ${word(id)}…`);
      else if (s.type === "sheetDone") go(id, 1, `${word(id)} done`);
      else if (s.type === "held") go(null, 1, "Saving…");
      progFeed.fills = fills;
    };
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
      b.title = inRev ? "Open it in Review" : TIP;
    }
    b.setAttribute("aria-label", inRev ? `Order ${rid} is in Review. Open it there.` : `Hold order ${rid}`);   // (HOLDORDER: was "Send order … to Review")
  }
  /** Every drawn button says what is true now (after a press, a change from another screen, a redraw). */
  function sync() {
    try {
      for (const b of doc.querySelectorAll(".revSendBtn[data-rid]")) {   // (HOLDORDER: every Hold Order on the page, wherever it was put)
        const n = b.closest("[data-rid]"); if (!n) continue;
        b.dataset.revState = ""; paint(b, n.dataset.rid, null);
      }
    } catch (_) {}
  }

  /** (SENDREVIEW, Paul 20 Oct 2026: "If the status of a given order updates from anywhere in the application, then these buttons and UI must react to the change/update in near real-time":
   *  called once a frame with the Review tab's count (LiveStrip.now, which every Review drawing and every Orders drawing asks for): Open is read once and only a button whose state
   *  changed is drawn again (paint keeps one that reads the same). Nothing is read from the cloud and nothing polls; the Orders tab out of sight is drawn when it is shown.) */
  function refresh() {
    try { const hs = doc.querySelectorAll(".revSendBtn[data-rev-host]"); if (hs.length) { openMemo = null; for (const b of hs) paint(b, b.dataset.rid, null); } } catch (_) {}   // (HOLDBTN: the order window's buttons, wherever it is open)
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
        b = button(rid, row);
        const facts = acts.querySelector(".rowFacts");
        if (facts) facts.after(b); else acts.appendChild(b);
        node.dataset.rid = node.dataset.rid || rid;
      }
      b.dataset.rid = rid;
      paint(b, rid, row);
      drawMsg(node, rid); drawProg(node, rid);
    } catch (e) { console.warn("hold order button", e); }
  }
  /** (HOLDORDER, Paul 20 Oct 2026) The ONE maker of the Hold Order button: the Orders rows use it, and so can any other place that shows Hold Order. Painted for what is true now. */
  /** (HOLDBTN, Paul 20 Oct 2026) opts.offSheets: once marked, the order's pieces on sheets not yet cut come off them (HoldUI.takeOff), for every button where Hold stood; opts.host names that place. */
  function button(rid, row, opts) {
    css(); subscribe();
    rid = String(rid); opts = opts || {};
    const b = doc.createElement("button"); b.type = "button"; b.className = "btn ghost sm revSendBtn"; b.dataset.rid = rid;
    if (opts.host) b.dataset.revHost = String(opts.host);
    const how = opts.offSheets ? { offSheets: true } : undefined;
    b.onclick = e => {
      e.stopPropagation(); e.preventDefault();
      // (HOLDSTAMP, Paul 27 Oct 2026: pressed in the order window, its ORDER HELD seal is stamped there, in sight, once the order is held)
      try { if (b.closest("#orderWin") && !busyRids.has(rid) && !api.inReview(rid, row || null) && root.OrderWin && OrderWin.expectStamp) OrderWin.expectStamp(rid, "hold", Date.now()); } catch (_) {}
      press(rid, row || null, how).catch(err => console.warn("hold order", err));
    };
    paint(b, rid, row || null);
    return b;
  }
  /** The same button as markup (for a place drawn from a string); its press is taken by the one listener below. */
  const escA = v => String(v).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  function buttonHtml(rid) {
    css(); subscribe();
    const s = escA(rid), inRev = api.ready() && api.inReview(String(rid), null);
    return `<button type="button" class="btn ghost sm revSendBtn${inRev ? " revIn" : ""}" data-rid="${s}" data-rev-state="${inRev ? "in" : "idle"}" title="${escA(inRev ? "Open it in Review" : TIP)}" aria-label="${escA(inRev ? `Order ${rid} is in Review. Open it there.` : `Hold order ${rid}`)}">${inRev ? IN : LABEL}</button>`;
  }
  const cancelled = rid => { try { return !!(root.Cancelled && typeof Cancelled.has === "function" && Cancelled.has(rid)); } catch (_) { return false; } };
  /** (HOLDBTN, Paul 20 Oct 2026) One span in a row drawn as text where the button goes, where Hold stood ("" for an order not in the list, cancelled, or a page without Review); fill() makes it. */
  function slot(ctx) {
    ctx = ctx || {};
    const rid = String(ctx.rid || "").trim();
    if (!rid || !api.ready() || !rowsOf(rid).length || cancelled(rid)) return "";
    return `<span class="revSendSlot" data-rev-slot data-rid="${escA(rid)}"${ctx.offSheets === false ? "" : ' data-off="1"'}></span>`;
  }
  /** (HOLDBTN) Each slot under `host` becomes button() (with offSheets), with its line under it when it has one. */
  function fill(host) {
    const where = host && host.querySelectorAll ? host : doc;
    for (const s of [...where.querySelectorAll("[data-rev-slot]")]) {
      if (!api.ready()) { s.remove(); continue; }
      const b = button(s.dataset.rid, null, { offSheets: s.dataset.off === "1", host: "orderWindow" });
      s.replaceWith(b);
      if (b.parentElement) { drawMsg(b.parentElement, b.dataset.rid); drawProg(b.parentElement, b.dataset.rid); }   // (HOLDPROG: a window painted again mid-hold keeps its bar)
    }
  }
  try {
    doc.addEventListener("click", e => {
      const b = e.target && e.target.closest ? e.target.closest(".revSendBtn[data-rid]") : null;
      if (!b || b.onclick || b.disabled) return;   // (a button made by button() takes its own press)
      e.stopPropagation(); e.preventDefault(); press(b.dataset.rid, null).catch(err => console.warn("hold order", err));
    });
  } catch (_) {}

  /* ── the press ── */
  const plainError = e => {
    const msg = String((e && (e.error || e.message)) || e || "").replace(/^Error:\s*/, "").trim();
    if ((root.navigator && navigator.onLine === false) || /failed to fetch|network|load failed|timed out|timeout|offline/i.test(msg)) return "Could not reach the server, so the order was not held. Try again when you are back online.";
    return `Not held: ${msg || "something went wrong"}. Try again.`;   // (HOLDORDER: the words of the button)
  };
  async function press(rid, row, opts) {
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
    const offing = !!(opts && opts.offSheets);
    if (offing) prog(rid, { p: 0.05, text: "Holding the order…", t0: Date.now(), end: "" });
    let res;
    try { res = await api.mark(rid, who); }
    catch (e) { res = { ok: false, error: (e && e.message) || String(e) }; }
    if (res === false || (res && res.ok === false)) { busyRids.delete(rid); sync(); if (offing) prog(rid, { end: "bad", text: "Not held" }); say(rid, plainError(res)); return res || { ok: false, error: "failed" }; }
    // (HOLDBTN, Paul 20 Oct 2026: a button where Hold stood also takes the order's pieces off sheets not yet cut, through the old Hold's engine.)
    // (HOLDPROG, Paul 27 Oct 2026: "it took over a minute to complete ... and there was no feedback to the user": the order IS held once it is marked, so the button says so at once ("In Review",
    //  its seal stamped); the sheets are put right under it with the bar and the step it is on, and the last line says it is done, or what went wrong, and how long it took)
    let off = null;
    // (HOLDSTAMP: the order window is painted at once, so its ORDER HELD seal is stamped at the press, not when the sheets are done)
    try { if (root.OrderWin && typeof OrderWin.isOpen === "function" && OrderWin.isOpen()) OrderWin.paint(); } catch (_) {}
    if (offing) {
      prog(rid, { p: 0.15, text: "Order held. Taking its pieces off the sheet…" });
      busyRids.delete(rid); sync();
      off = await api.takeOff(rid, who, progFeed(rid));
    }
    busyRids.delete(rid);
    sync();
    if (off && off.off > 0) {
      const names = (off.names || []).filter(Boolean), list = names.length < 2 ? names.join("") : names.slice(0, -1).join(", ") + " and " + names[names.length - 1];
      prog(rid, { end: "ok", text: "Done" });
      say(rid, `Order held. Taken off ${list || "its sheet"}; it waits in Review.`);
      return { ok: true, sheet: "", off: off.off, names };
    }
    const sheet = whereOf(rid, row), many = sheet.includes(" + ");
    const offWhy = off && off.ok === false && (off.why || off.error) ? ` (${String(off.why || off.error).replace(/[.\s]+$/, "")})` : "";
    if (offing) prog(rid, offWhy ? { end: "bad", text: "Held, but its pieces stay on the sheet" } : { end: "ok", text: "Done" });
    if (sheet) say(rid, `Order held. Stays on ${sheet}${offWhy}. ${many ? "Those sheets are" : "That sheet is"} held until this is resolved in Review.`);
    else if (offing) say(rid, "Order held; it waits in Review.");
    return { ok: true, sheet, off: off ? off.off || 0 : undefined };
  }

  // (the mark API's one change callback: registered the first time a button is drawn, when the script that offers it is surely loaded; again if the API object is ever replaced)
  let sub = null;
  const subscribe = () => {
    const m = MARK(); if (!m || (sub && sub.api === m)) return;
    try { if (sub && typeof sub.off === "function") sub.off(); } catch (_) {}
    sub = api.onChange(sync);
  };
  root.ReviewSend = { mount, press, sync, refresh, button, buttonHtml, slot, fill, busy: rid => busyRids.has(String(rid)), LABEL, IN };
})(typeof window !== "undefined" ? window : globalThis);
