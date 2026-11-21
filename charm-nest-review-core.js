/*  charm-nest-review-core.js — the Review tab, built ONCE from the cloud's data (CharmNestReviewCore).
 *  ═══════════════════════════════════════════════════════════════════════
 *  Every sorter computer built its own Review tab from its own memory (B.orders.rows, B.review.items, Review.settled, B.customDesigns, the
 *  placement answers it had read...), so two computers showed different cards and different counts. This file is the Review's rules with
 *  nothing of a page in them: one pure function, build(snapshot), that a Netlify function runs on what Firestore holds, and whose answer every
 *  computer only draws. No DOM, no window, no Date.now(), no randomness: the same snapshot gives the same answer, in the same order.
 *  Node: module.exports. Browser: window.CharmNestReviewCore (charm-nest-orders.js's pattern).
 *
 *  ── THE SNAPSHOT (input) ───────────────────────────────────────────────
 *  {
 *    now:        number ms            the build's time, from the caller (echoed as out.at; no rule reads a clock)
 *    lines:      [Line]               every line of the run's pull, gone ones included   (Charm_Nest_Runs/{run}.lines, Orders.lineRecord, see Line)
 *    customDone: { lineKey: Rec }     completed custom records                          (Charm_Custom_Orders/{lineKey}, state != "open", kind != "reviewMark": B.maps.customDone)
 *    customKept: { lineKey: Rec }     reopened custom records (state "open")            (Charm_Custom_Orders, B.maps.customKept; read for a send's seals only)
 *    reviewMarks:{ rid_review: Mark } Hold Order (Send to Review) records               (Charm_Custom_Orders/{rid}_review, B.maps.reviewMarks: { receiptId, review: { state "open"|"done", markedAt, markedBy, doneAt, doneBy, how, sku } })
 *    customSheet:[Send]               Send to Sheet receipts                            (Charm_Custom_Sheet card-<hash>: { ck, rid, phase "pending"|"sent", sent: { at, by, id, lines: { lineKey: [...] } } })
 *    customSent: { lineKey: Dec }     a line's send decision kept on its custom record  (Charm_Custom_Orders/{lineKey} with how "sheet"; NEW as a server read, see CLOUD 7)
 *    cancelled:  [rid]                orders cancelled                                   (Charm_Nest_Cancelled doc ids: Cancelled.ids())
 *    settled:    [Settled]            the decisions a person answered in Review          (NEW collection, e.g. Charm_Nest_Review_Settled/{key}~{t}, see CLOUD 5)
 *    shipped:    [rid]  (optional)    orders a shipping station shipped                  (the Completed pile, CNCompleted: only the tab badge reads it)
 *  }
 *  Line (one per order line; the fields the rules read, all already in Orders.lineRecord unless marked NEW):
 *    key                       "<receiptId>_<transactionId>"                             lineRecord [0]
 *    state                     pulled | noDesign | held | unmatched | pooled | ... | committed | skipped | gone
 *    hold, reason              the hold's words (a line on hold), its state's reason     lineRecord hold / reason
 *    heldAt          NEW       when the hold was set (ms)                                (the bridge reads row.heldAt; lineRecord does not keep it yet)
 *    placement       NEW       the cloud's placement answer for the line: { state: "hold"|"sheet"|"hand"|"cancelled"|..., onSheet, partial } or null
 *                              (PiecePlacement.ofRow, worked out from Charm_Pool / Charm_Nest_Sheets; the server has these itself)
 *    holdSeen        NEW       a hold the cloud already confirmed (row.holdSeen; see CLOUD 8)
 *    poolIds                   the line's pieces in the nesting queue                    lineRecord poolIds
 *    frontAt, releasing        a hold lifted (Release Hold) / a release under way        lineRecord frontAt / releasing { at, by }
 *    arrivedAt, activityAt     when the line came in / last touched (ms)                 lineRecord
 *    changePending             Etsy changed the order after the pull, not yet accepted  lineRecord changePending (see CLOUD 1)
 *    changedAt, changedWhy NEW when, and what changed (the card's time and words)
 *    order  { receiptId, orderNumber, createTs (s), updateTs (s), shipBy (s), buyer }    lineRecord orderId / createTs / updateTs / snap
 *    line   { transactionId, listingId, sku, title }                                     lineRecord transactionId / snap
 *    spec   { designSku, special: { label, monogram, notCut } | null, material }  NEW    the line's reading (interpretLine) — lineRecord keeps sku only
 *    problems  [Problem]       NEW as full objects: the questions the line asks NOW, as interpretAll leaves row.problems (interpretLine's
 *                              spec.problems less what materialOverride / sizeOverride / releaseAnyway / a sent custom design set aside).
 *                              lineRecord keeps their kinds only; decisionKey and the card's words need { kind, sku, zodiac, generic,
 *                              lookalike, listingId, huggie, optionName, optionValue, metalLabel, reason, formHold, size, available, why,
 *                              note, count, letter, font, pairSecond, setLayout, earWords, material }.
 *  Settled (one decision answered, Review.remove): { key (the decision's key), kind, why, by, t (ms, when answered), orders: [rid], lines, rowKey }
 *
 *  ── THE ANSWER (output) ────────────────────────────────────────────────
 *  { at: now,
 *    cards: [Card]   Open's cards (newest activity first), then Resolved's (newest first)
 *    counts: { open, resolved, openCount (= the bridge's Review.openCount(): the Open segment's number), reviewCount (= Review.count(): decisions only,
 *              read by the top banner, the engraving gate and the run), customOrders (= the Custom Orders chip: customAsk + customWait), customAsk,
 *              customWait, needResolution, held (= heldOrder), one key per chip of the bridge's ORDER list (customOrder, needResolution, blockedSku,
 *              missingSize, oversize, fontMissing, engraveWords, notRepresentable, flipFailed, placement, heldOrder: Open's numbers), noChip { orderChanged,
 *              reviewMark } (Open cards with no chip), resolvedChips { the same ORDER keys: Resolved's numbers }, reviewOrders (= the top nav's Review badge
 *              with the production flow: open orders "In review / On hold", CNFlow) },
 *    openOrders: [rid]   the orders Open lists (Review.openOrders(): what the Orders tab's In Review and the flow read), ascending
 *    openKeys:   [key]   Open's card keys (the bridge's item keys: ord:…, chg:…, held:…, cinfo:…), ascending
 *    resolvedKeys:[key]  Resolved's card keys, ascending
 *    itemKeys:   [key]   the decision queue (B.review.items less engraving "eng:" and held "held:" notices), ascending }
 *  Card: { key, mkey (the drawn card's key, the bridge's mkeyOf / settled key), kind, chip (its chip, or "" for none), state "Open"|"Resolved",
 *          t (its own time, from data), at (its latest activity, from data: the sort key), orders [rid], lineKeys, sku, listingId, title, why,
 *          problems [kind], flags: info / done / decided / waits / folded }
 *
 *  ── RULES PORTED (charm-nest-bridge.js unless said) ────────────────────
 *    Review.mine / isNotice / count / openCount ..... 20853-20859      tabOf, kindOfWords, isCustomRow, customKey .... 20864-20869
 *    problemText, decisionKey, syncOrderItems ....... 20944-21000      rowsOf, mkeyOf ................................ 21002-21004
 *    KIND_WORDS, NO_CHIP ............................ 23019-23026      sentToSheet, HOW_WORDS ........................ 23072-23077
 *    sentRecord (time and by only) .................. 23118-23128      openParts / openPartsRaw ...................... 23163-23179
 *    heldCards ...................................... 23183-23197      openOrders .................................... 23202-23209
 *    customLists .................................... 23210-23250      releasedCards ................................. 23261-23285
 *    render's Resolved list, chips and counts ....... 23485-23563 (CHIPS, chipOf, ORDER, clLaid, openCustom, answeredCards, releasedNow, finished, byKind, doneKind, customN)
 *    Orders.heldNow ................................. 2495             CustomSheet.decisionOf / sending ............... 18612-18614 (index: 18608)
 *    charm-nest-review-mark.js isMarked / markedRids / itemsFor / doneSettled ........................ 31-32, 234-262
 *    charm-nest-flow-stages.js lineRank / orderRank / count (review only) 73-111; Orders openRow / flowCtx / flowNow, bridge 2394-2440
 *
 *  ── CLOUD: what replaces the browser-only state the bridge reads ───────
 *    1  "chg:" Order changed cards. The bridge's card is an item Review.add made on the ONE computer that saw Etsy's change (2283, 5030); no other
 *       computer has it. Here: the line's own changePending (already kept by lineRecord; set with the card, cleared when the change is accepted or the
 *       line is made up again, 21116-21188) is the card, with changedAt / changedWhy (NEW) for its time and words. A writer must store changePending
 *       only once a change is offered (the bridge also raises it for the few milliseconds of a repool in flight).
 *    2  A card's time. The bridge stamps a card with Date.now() when this page first made it (put, heldCards, customLists itemFor), so each
 *       computer dated (and sorted) the same card differently. Here every time is the data's: a decision is as old as its oldest line
 *       (arrivedAt, else the order's createTs); a mark is its markedAt; a held order its heldAt (else createTs: the bridge's own RVSAME fallback);
 *       a custom card that asks nothing its oldest line; done / sent / settled / released cards keep the record's time, as in the bridge.
 *    3  Held order cards. The bridge lends a card the time and words of the run's "held:" notice (B.review.items, this page's). Here: line.heldAt
 *       and line.hold. heldMade / infoItems / relCache / doneCache (identity caches) are not needed: nothing is kept between two builds.
 *    4  Order. Open is sorted by the card's activity (at, desc; key asc). The bridge sorts by CNListActivity.at over the card (its Date.now() t
 *       included) and keeps a card's place through a Complete / Reopen (RV.place, per computer); the date range and direction are each computer's
 *       localStorage. The core returns every card; a drawer may filter by date and flip the direction.
 *    5  Settled decisions. Review.settled is the list THIS computer answered this shift (lost on reload); Resolved listed different decisions on
 *       each computer. Here: `settled`, a cloud list written where Review.remove writes settled.unshift (one record per answer, its shape above).
 *       (Today only Review.remove writes it, and of the Review's own buttons only "Accept the new order" (acceptChange 21810) calls remove with a
 *       decision's key: most answers re-read the line, and syncOrderItems drops the card with no settled record.)
 *       (S2, Paul 21 Nov 2026: built. In drive mode "on" a viewer's Review.remove sends each answer to reviewSettledPut (Charm_Nest_Review_Settled/list, the newest 500 of 14 days,
 *       netlify/functions/_charmNestReviewSettled.js); the cloud world reads it into this snapshot, and every viewer draws that same list (charm-nest-viewer.js settled*).)
 *    6  A send under way. CustomSheet.sending reads B.customDesigns (this computer's send intent). Here: a Charm_Custom_Sheet record with phase
 *       "pending" (customSheetPut writes it before the send is through; hydrate reads it back the same way, 19180).
 *    7  A send kept on a custom record. B.maps.customSent / row._customSentDecision are filled only by the order window of the computer that opened
 *       the order (customGet, 27963-27985). Here: `customSent`, the server reading Charm_Custom_Orders how:"sheet" for the pulled lines.
 *    8  On hold now. Orders.heldNow trusts the placement answer once this page has SEEN the cloud confirm the hold (row.holdSeen), else its own flag.
 *       Here: the same rule over line.placement (the server's own placement answer) and line.holdSeen; with no placement, line.hold.
 *    9  Cancelled. Cancelled.has reads this computer's cached id list (localStorage, refreshed each minute). Here: `cancelled`, the collection's ids.
 *   10  The tab badge. With the production flow (CNFlow) the top nav's Review badge is NOT Review.openCount(): it is the open orders standing at
 *       "In review / On hold" (Ladder navBadges, Orders.flowNow). It reads placement (onSheet) and the shipped pile: `placement`, `shipped`.
 *  ═══════════════════════════════════════════════════════════════════════ */
(function (root, factory) {
  if (typeof module === "object" && module.exports) module.exports = factory();
  else root.CharmNestReviewCore = factory();
})(typeof self !== "undefined" ? self : this, function () {
  "use strict";

  /* ═══ 0 · words and kinds (bridge 23019-23026, 23072, 23497) ═══ */
  const KIND_WORDS = { customOrder: "Custom Orders", needsMaterial: "Options · metal", needsMapping: "Options", unmatchedSku: "Unknown SKU", blockedSku: "Blocked SKU", missingSize: "Size", oversize: "Too big", fontMissing: "Font", engraveWords: "Words", notRepresentable: "Characters", flipFailed: "Flip", placement: "Placement", orderChanged: "Changed", heldOrder: "Held", reviewMark: "Marked", released: "Released" };
  const NO_CHIP = new Set(["orderChanged", "reviewMark"]);
  const ORDER = ["customOrder", "needResolution", "blockedSku", "missingSize", "oversize", "fontMissing", "engraveWords", "notRepresentable", "flipFailed", "placement", "heldOrder"];
  const CHIPS = { needResolution: { label: "Need Resolution", kinds: ["needsMapping", "unmatchedSku"] } };
  const HOW_SHEET = "sent to the sheets with the order's own designs";
  const METAL_LABEL = { gold: "GF 14/20", silver: "SS", rose: "RG 14/20", gold10k: "10K Gold", gold14k: "14K Gold" };   // (charm-nest-1.html METALS)
  const labelOf = m => METAL_LABEL[m] || m;
  const SEP = "\u0000";

  /* ═══ 1 · small pure helpers ═══ */
  const str = v => String(v == null ? "" : v);
  const num = v => { const n = +v; return Number.isFinite(n) ? n : 0; };
  const ridOf = v => str(v).replace(/\D/g, "").slice(0, 30);
  const isObj = v => !!v && typeof v === "object" && !Array.isArray(v);
  const byStr = (a, b) => (a < b ? -1 : a > b ? 1 : 0);
  const uniq = xs => { const out = [], seen = new Set(); for (const x of xs) if (!seen.has(x)) { seen.add(x); out.push(x); } return out; };

  /** A line as the rules read it (lenient about what the store gives: numbers as strings, missing objects). */
  function lineOf(raw) {
    const r = isObj(raw) ? raw : {}, o = isObj(r.order) ? r.order : {}, l = isObj(r.line) ? r.line : {};
    const sp = isObj(r.spec) ? r.spec : null;
    const special = sp && sp.special ? (isObj(sp.special) ? sp.special : { label: str(sp.special) }) : null;
    return {
      key: str(r.key), state: str(r.state || "pulled"), reason: r.reason == null ? null : str(r.reason), hold: r.hold ? str(r.hold) : null, heldAt: num(r.heldAt),
      placement: isObj(r.placement) && r.placement.state && r.placement.state !== "loading" ? r.placement : null, holdSeen: !!r.holdSeen,
      poolIds: Array.isArray(r.poolIds) ? r.poolIds.map(str) : [], frontAt: num(r.frontAt), releasing: isObj(r.releasing) && num(r.releasing.at) > 0 ? { at: num(r.releasing.at), by: str(r.releasing.by) } : (r.releasing ? { at: 0, by: str(r.releasing.by) } : null),
      arrivedAt: num(r.arrivedAt), activityAt: num(r.activityAt),
      changePending: !!r.changePending, changedAt: num(r.changedAt), changedWhy: str(r.changedWhy),
      order: { receiptId: str(o.receiptId), orderNumber: str(o.orderNumber || o.receiptId), createTs: num(o.createTs), updateTs: num(o.updateTs), shipBy: num(o.shipBy), buyer: str(isObj(o.buyer) ? o.buyer.name : o.buyer) },
      line: { transactionId: str(l.transactionId), listingId: str(l.listingId), sku: str(l.sku), title: str(l.title) },
      spec: sp ? { designSku: sp.designSku == null ? null : str(sp.designSku), special, material: sp.material || null } : null,
      problems: Array.isArray(r.problems) ? r.problems.filter(isObj) : []
    };
  }
  const rid = l => l.order.receiptId;
  const firstSeen = l => l.arrivedAt || (l.order.createTs > 0 ? l.order.createTs * 1000 : 0);
  const activity = l => Math.max(l.activityAt, l.heldAt, l.order.updateTs > 0 ? l.order.updateTs * 1000 : 0, firstSeen(l));

  /* ═══ 2 · a question's words and its decision's key (bridge 20944-20955) ═══ */
  function problemText(p) {
    return p.kind === "needsMaterial" ? `needs material (${p.metalLabel || "none"})`
      : p.kind === "needsMapping" ? (p.letter ? `option "${p.optionName}: ${p.optionValue}" — ${p.letter.why}` : p.pairSecond ? `${p.pairSecond.why}` : p.font ? p.font.why : p.setLayout ? `${p.setLayout.why}` : p.earWords ? `${p.earWords.why}` : p.note && !p.count ? `option "${p.optionName}: ${p.optionValue}" not mapped (${p.note})${p.why ? " — " + p.why : ""}` : p.count ? `option "${p.optionName}: ${p.optionValue}" may name how many pieces — a person says (${p.count.why})` : `option "${p.optionName}: ${p.optionValue}" not mapped${p.why ? " — " + p.why : ""}`)
      : p.kind === "unmatchedSku" ? (p.formHold ? p.reason : `SKU ${p.sku || "?"}: ${p.reason}`)
      : p.kind === "blockedSku" ? `SKU ${p.sku} blocked: ${p.reason}`
      : p.kind === "missingSize" ? (p.why ? `size ${p.size}: ${p.why}` : `no design for size ${p.size || "(none)"} (have ${(p.available || []).join(", ")})`)
      : p.kind === "oversize" ? `oversize for the ${labelOf(p.material)} plate` : p.kind;
  }
  /** An unknown SKU is ONE decision however many orders bought it; an unmapped option one however many lines carry it. */
  function decisionKey(l, p) {
    if (p.kind === "unmatchedSku") return p.sku ? `ord:sku:${p.sku}${p.zodiac ? SEP + p.zodiac : ""}${p.generic ? SEP + "L" + p.generic : ""}${p.lookalike ? SEP + "K" + p.lookalike : ""}` : `ord:listing:${p.listingId || l.key}${p.huggie ? SEP + "huggie" : ""}`;
    if (p.kind === "blockedSku") return `ord:blocked:${p.sku}`;
    if (p.kind === "needsMapping") return `ord:opt:${p.optionName}${SEP}${p.optionValue}`;
    if (p.kind === "needsMaterial") return `ord:mat:${p.listingId || l.key}${SEP}${p.metalLabel || ""}`;
    return `ord:${l.key}:${p.kind}`;
  }
  const isCustomRow = l => !!(l.spec && l.spec.special);
  const customKey = l => `ord:custom:${rid(l)}:${(l.spec && l.spec.designSku) || l.line.sku || l.line.listingId || l.key}`;
  const sentToSheet = d => !!d && (d.how === "sheet" || d.why === HOW_SHEET);
  const mkeyOf = it => it.kind === "customOrder" ? "cu:" + str(it.key).replace(/^[a-z]+:/, "") : str(it.key);
  const kindOfWords = w => { for (const [k, v] of Object.entries(KIND_WORDS)) if (v === w && k !== "customOrder") return k === "needsMaterial" ? "needsMapping" : k; return null; };
  const doneTime = rec => (rec && Math.max(num(rec.lastPrintedAt), num(rec.completedAt), num(rec.printedAt))) || 0;

  /* ═══ 3 · the world the rules read ═══ */
  function world(snap) {
    snap = isObj(snap) ? snap : {};
    // (one order for every computer, whatever order the store returned them in: by ship-by, then order, then line; the bridge's pull is ship-by first too)
    const lines = (Array.isArray(snap.lines) ? snap.lines : []).map(lineOf).filter(l => l.key)
      .sort((a, b) => (a.order.shipBy - b.order.shipBy) || byStr(rid(a), rid(b)) || byStr(a.key, b.key));
    const byKey = new Map(); for (const l of lines) if (!byKey.has(l.key)) byKey.set(l.key, l);
    const customDone = isObj(snap.customDone) ? snap.customDone : {}, customKept = isObj(snap.customKept) ? snap.customKept : {};
    const marks = isObj(snap.reviewMarks) ? snap.reviewMarks : {}, customSent = isObj(snap.customSent) ? snap.customSent : {};
    // CustomSheet.index (18608): line key -> the card record that sent (or is sending) it; the last record naming a line wins, as the bridge's map does
    const sheetIx = new Map();
    for (const rec of Array.isArray(snap.customSheet) ? snap.customSheet : []) {
      if (!isObj(rec) || !isObj(rec.sent) || !isObj(rec.sent.lines)) continue;
      const e = rec.phase === "sent" ? { sent: rec.sent, pending: false } : { sent: null, pending: true };
      for (const k of Object.keys(rec.sent.lines)) sheetIx.set(k, e);
    }
    const cancelled = new Set((Array.isArray(snap.cancelled) ? snap.cancelled : []).map(str));
    const shipped = new Set((Array.isArray(snap.shipped) ? snap.shipped : []).map(str));
    const settled = (Array.isArray(snap.settled) ? snap.settled : []).filter(isObj).map(d => ({ key: str(d.key), kind: str(d.kind), why: str(d.why), by: str(d.by), t: num(d.t), orders: (Array.isArray(d.orders) ? d.orders : []).map(str), lines: num(d.lines), rowKey: str(d.rowKey), how: d.how }))
      .sort((a, b) => (b.t - a.t) || byStr(a.key, b.key)).slice(0, 200);   // (newest first, 200 at most: Review.remove)
    const W = { now: num(snap.now), lines, byKey, customDone, customKept, marks, customSent, sheetIx, cancelled, shipped, settled };
    W.engraving = Array.isArray(snap.engraving) ? new Set(snap.engraving.map(str)) : null;   // (CLOUDWORLD: the orders with engraving still to settle, when the caller gives them: the Design & Engrave badge)
    // CustomSheet.decisionOf / sending (18612-18614)
    W.decisionOf = l => { const e = sheetIx.get(l.key); return (e && e.sent) || (isObj(customSent[l.key]) ? customSent[l.key] : null) || null; };
    W.sending = l => { const e = sheetIx.get(l.key); return !!(e && e.pending && !e.sent); };
    // spec.customDone is the custom record read for the line (interpretLine: ctx.customDone[lineKey])
    W.doneOf = l => (isObj(customDone[l.key]) ? customDone[l.key] : null);
    // Orders.heldNow (2495)
    W.heldNow = l => { const pl = l.placement; if (pl && l.holdSeen) return pl.state === "hold"; return !!l.hold || (pl || {}).state === "hold"; };
    // charm-nest-review-mark.js isMarked (31)
    W.markOf = r => { const m = marks[ridOf(r) + "_review"]; return m && isObj(m.review) ? m.review : null; };
    W.isMarked = r => { const v = W.markOf(r); return !!v && v.state === "open"; };
    return W;
  }
  const live = l => l.state !== "gone";
  const rowsOf = it => (it.rows && it.rows.length ? it.rows : it.row ? [it.row] : []).filter(live);

  /* ═══ 4 · the decision queue: syncOrderItems (20957-20996) + the review marks (review-mark itemsFor) + Order changed (CLOUD 1) ═══ */
  function decisionItems(W) {
    const keep = new Map();
    for (const l of W.lines) {
      if (l.state === "gone" || l.hold) continue;   // (a line a person parked is parked)
      if (isCustomRow(l) && l.problems.length) {
        const key = customKey(l);
        if (!keep.has(key)) keep.set(key, { kind: "customOrder", key, row: l, problem: l.problems[0], problems: [], rows: [], why: "" });
        const it = keep.get(key); if (!it.rows.includes(l)) it.rows.push(l);
        for (const p of l.problems) if (!it.problems.some(q => q.kind === p.kind && problemText(q) === problemText(p))) it.problems.push(p);
        it.problem = it.problems[0]; it.why = it.problems.map(problemText).join(" · ");
        continue;
      }
      for (const p of l.problems) {
        const key = decisionKey(l, p);
        if (!keep.has(key)) keep.set(key, { kind: p.kind, key, row: l, problem: p, rows: [], why: problemText(p) });
        const it = keep.get(key); if (!it.rows.includes(l)) it.rows.push(l);
      }
    }
    // an order a person held (Hold Order / Send to Review): one item for each open mark whose order has a line in the pull (itemsFor)
    const byRid = new Map();
    for (const l of W.lines) { if (!live(l)) continue; const r = rid(l); if (!W.isMarked(r)) continue; if (!byRid.has(r)) byRid.set(r, []); byRid.get(r).push(l); }
    for (const [r, rs] of byRid) { const v = W.markOf(r); keep.set("ord:rvm:" + r, { kind: "reviewMark", key: "ord:rvm:" + r, rid: r, row: rs[0], rows: rs, problem: { kind: "reviewMark", rid: r }, problems: [], why: "Order held" + (v.markedBy ? " by " + v.markedBy : ""), t: num(v.markedAt) }); }
    // Order changed (CLOUD 1): kept while its line is in the pull, not cut and committed, not gone
    for (const l of W.lines) if (l.changePending && !["gone", "committed"].includes(l.state)) keep.set("chg:" + l.key, { kind: "orderChanged", key: "chg:" + l.key, row: l, rows: [l], why: l.changedWhy || `Etsy updated order ${rid(l)} after the pull`, t: l.changedAt || activity(l) });
    // (CLOUD 2: a decision is as old as the oldest line that asks it)
    for (const it of keep.values()) if (!it.t) { const ts = rowsOf(it).map(firstSeen).filter(x => x > 0); it.t = ts.length ? Math.min(...ts) : 0; }
    return [...keep.values()];
  }

  /* ═══ 5 · Custom Orders that ask nothing, completed and sent (customLists 23210-23250) ═══ */
  function customLists(W, decided) {
    const open = [], done = [], sent = [], engraving = [], groups = new Map();
    for (const l of W.lines) {
      const sp = l.spec, rec = W.doneOf(l), decision = W.decisionOf(l), pending = W.sending(l);
      if (!sp || !(sp.special || rec || decision || pending) || l.state === "gone") continue;
      const isDone = !!rec && !sentToSheet(rec), isSent = !isDone && !!decision;
      const mono = !!(sp.special && sp.special.monogram) && !isDone && !isSent && !pending;   // (MONOROUTE: the Engraving tab's)
      if (!isSent && decided.has(l.key)) continue;
      if (!isDone && !isSent && !pending && (l.problems.length || l.hold || ["committed", "skipped"].includes(l.state))) continue;
      const key = (isDone ? "cdone:" : isSent ? "csent:" : "cinfo:") + customKey(l).slice(4);
      if (!groups.has(key)) groups.set(key, { key, done: isDone, decided: isSent, mono, rows: [] });
      groups.get(key).rows.push(l);
    }
    for (const g of groups.values()) {
      const first = g.rows[0];
      const record = g.done ? g.rows.map(W.doneOf).reduce((a, b) => doneTime(b) > doneTime(a) ? b : a) : g.decided ? sentRecord(W, first, W.decisionOf(first)) : null;
      const own = g.done ? g.rows.map(r => ({ r, d: W.decisionOf(r) })).find(x => x.d) : null;
      const t = g.decided ? record.at : g.done && doneTime(record) ? doneTime(record) : (g.done ? num(record && record.updatedAtMs) : Math.min(...g.rows.map(firstSeen)));   // (CLOUD 2)
      const it = { kind: "customOrder", key: g.key, info: true, rows: g.rows, row: first, done: g.done, decided: g.decided, record, alsoSent: own ? sentRecord(W, own.r, own.d) : null, foldedInto: null, t, why: customWhy(first, g.done, record) };
      (g.done ? done : g.decided ? sent : g.mono ? engraving : open).push(it);
    }
    // a completed record whose line has left the pull (or is gone) is still listed, so its label can be printed again
    for (const lineKey of Object.keys(W.customDone).sort()) {
      const rec = W.customDone[lineKey], r0 = W.byKey.get(lineKey);
      if (!isObj(rec) || sentToSheet(rec) || (r0 && r0.state !== "gone")) continue;
      done.push({ kind: "customOrder", key: "cdone:rec:" + lineKey, info: true, rows: [], row: null, done: true, record: rec, rid: str(rec.receiptId), alsoSent: null, foldedInto: null, t: doneTime(rec) || num(rec.updatedAtMs), why: customWhy(null, true, rec) });
    }
    // one card for an order in both lists: the sent card folds into the completed one
    const doneBy = new Map(); for (const it of done) if (it.row) doneBy.set(it.key.slice(6), it);
    for (const it of sent) { const d = doneBy.get(it.key.slice(6)); if (!d) continue; it.foldedInto = d.key; if (!d.alsoSent) d.alsoSent = it.record; }
    for (const it of done) if (it.alsoSent) it.t = Math.max(num(it.t), num(it.alsoSent.at));
    done.sort((a, b) => doneTime(b.record) - doneTime(a.record));
    return { open, done, sent, engraving };
  }
  /** The send's time and who sent it (sentRecord 23118: the seals and words are the drawer's). */
  function sentRecord(W, l, decision) {
    const kept = (l && (W.customDone[l.key] || W.customKept[l.key])) || {};
    const at = num(decision.at) || num(decision.decidedAt) || num(decision.completedAt) || 0, by = str(decision.by || decision.decidedBy || decision.completedBy);
    return Object.assign({}, kept, decision, { state: "decided", how: "sheet", at, decidedAt: at, by, receiptId: l ? rid(l) : "", category: (l && l.spec && l.spec.special && l.spec.special.label) || decision.category || "Custom designs" });
  }
  function customWhy(l, done, rec) {
    if (done) {
      if (rec && rec.how === "button") return `Completed${rec.completedBy ? " by " + rec.completedBy : ""}`;
      const by = rec && (rec.lastPrintedBy || rec.completedBy); return `QR label printed${by ? " by " + by : ""}`;
    }
    const spc = (l && l.spec && l.spec.special) || { label: "Custom designs" };
    if (rec && rec.how === "sheet") return "Sent to Sheet";
    if (l && l.poolIds.length) return `${spc.label || "Custom designs"} · on its way to the laser`;
    return `${spc.label || "Custom designs"} · ${spc.notCut ? "not laser cut · print its QR label when it is ready" : "waits on a person"}`;
  }
  const customWhyOf = (l, it) => it.decided ? "Sent to Sheet" : it.why;

  /* ═══ 6 · held orders no card covers (heldCards 23183-23197; CLOUD 3) ═══ */
  function heldCards(W, covered) {
    const first = new Map();
    for (const l of W.lines) { if (["gone", "committed"].includes(l.state)) continue; const r = rid(l); if (first.has(r) || covered.has(r) || !W.heldNow(l)) continue; first.set(r, l); }
    const out = [];
    for (const [r, l] of first) {
      if (W.cancelled.has(r)) continue;
      out.push({ kind: "heldOrder", key: "held:" + r, rid: r, row: l, rows: W.lines.filter(x => rid(x) === r && live(x)), line: l.key, why: `${r} held — ${l.hold || "on hold"}`, t: l.heldAt || (l.order.createTs > 0 ? l.order.createTs * 1000 : 0) });
    }
    return out;
  }

  /* ═══ 7 · resolved Hold Order marks (review-mark doneSettled 247-262) ═══ */
  function doneMarks(W) {
    const out = [];
    for (const [k, m] of Object.entries(W.marks)) {
      const v = m && m.review; if (!isObj(v) || v.state !== "done") continue;
      const r = ridOf(m.receiptId || k), rs = W.lines.filter(x => rid(x) === r && live(x));
      out.push({ key: "ord:rvm:" + r, rowKey: rs[0] ? rs[0].key : "", kind: "reviewMark", why: v.how === "pick" ? "Charm picked" + (v.sku ? ": " + v.sku : "") : v.how === "other" ? "Released to the nesting queue" : "Marked OK", lines: rs.length || 1, orders: [r], by: str(v.doneBy), t: num(v.doneAt) });
    }
    return out.sort((a, b) => (b.t - a.t) || byStr(a.key, b.key)).slice(0, 200);
  }

  /* ═══ 8 · orders released to the nesting queue (releasedCards 23261-23285) ═══ */
  function releasedCards(W, skip, answered) {
    const by = new Map();
    for (const l of W.lines) {
      if (l.hold || ["gone", "skipped"].includes(l.state) || l.problems.length || skip.has(l.key)) continue;
      if ((l.spec && l.spec.special) || W.doneOf(l) || W.decisionOf(l)) continue;
      const at = Math.max(l.frontAt, l.releasing ? l.releasing.at : 0); if (!(at > 0)) continue;
      const r = rid(l); if (!by.has(r)) by.set(r, []); by.get(r).push(l);
    }
    const said = new Map(); for (const x of answered) for (const r of x.settled.orders || []) said.set(str(r), Math.max(said.get(str(r)) || 0, num(x.settled.t)));
    const out = [];
    for (const [r, rows] of by) {
      const at = Math.max(...rows.map(l => Math.max(l.frontAt, l.releasing ? l.releasing.at : 0)));
      if (said.has(r) && said.get(r) >= at - 120000) continue;   // (a card under Resolved already says this release)
      const going = rows.some(l => l.releasing), who = ((rows.find(l => l.releasing && l.releasing.by) || {}).releasing || {}).by || "";
      out.push({ key: "rel:" + r, rowKey: rows[0].key, kind: "released", why: going ? "Being released to the nesting queue" : "Released to the nesting queue", lines: rows.length, orders: [r], by: who, t: at, _rows: rows });
    }
    return out;
  }

  /* ═══ 9 · the tab badge with the production flow (CLOUD 10; charm-nest-flow-stages.js lineRank / orderRank, bridge flowCtx) ═══ */
  function reviewOrders(W, items, openRids) {
    const rev = new Set(); for (const it of items) for (const l of (it.rows && it.rows.length ? it.rows : it.row ? [it.row] : [])) rev.add(l.key);
    const groups = new Map();
    for (const l of W.lines) { const r = rid(l); if (!live(l) || W.shipped.has(r) || W.cancelled.has(r)) continue; if (!groups.has(r)) groups.set(r, []); groups.get(r).push(l); }
    let n = 0;
    for (const [r, rs] of groups) {
      const ranks = rs.map(l => {
        const pl = l.placement;
        if (pl && pl.state === "cancelled") return null;
        if (pl && pl.state === "hand") return "hand";
        if (W.heldNow(l)) return "review";
        if (pl && (pl.state === "sheet" || pl.onSheet) && !pl.partial) return "sheet";
        if (rev.has(l.key) || l.state === "unmatched") return "review";
        return "later";
      }).filter(x => x != null);
      if (ranks.length && (openRids.has(r) || ranks.includes("review"))) n++;
      // (CLOUDWORLD: the Design & Engrave badge, by the same flow: an order no person must decide first whose engraving is still to settle, as flow-stages orderRank puts it on Design)
      else if (ranks.length && W.engraving && W.engraving.has(r)) W.designN = (W.designN || 0) + 1;
    }
    return n;
  }

  /* ═══ 10 · build ═══ */
  function build(snapshot) {
    const W = world(snapshot);
    const items = decisionItems(W);
    // Review.count: what still waits for a DECISION (a line every card's lines sent to a sheet asks nothing)
    const openGroup = it => { const group = rowsOf(it), open = group.filter(l => !W.decisionOf(l)); return !group.length || open.length === group.length ? it : open.length ? Object.assign({}, it, { rows: open, row: open[0] }) : null; };
    const reviewCount = items.filter(it => { const g = rowsOf(it); return !g.length || g.some(l => !W.decisionOf(l)); }).length;
    // openParts
    let all = items.map(openGroup).filter(Boolean);
    const decided = new Set(); for (const it of all) if (it.kind === "customOrder") for (const l of rowsOf(it)) decided.add(l.key);
    const cl = customLists(W, decided);
    const customWaits = it => rowsOf(it).some(l => W.sending(l) || !l.poolIds.length);
    const clWait = cl.open.filter(customWaits);
    const covered = new Set(); for (const it of all.concat(clWait)) { if (it.rid != null) covered.add(str(it.rid)); for (const l of rowsOf(it)) covered.add(rid(l)); }
    all = all.concat(heldCards(W, covered));
    const openList = all.concat(clWait);
    // openOrders: by order number, its first open card
    const openOrders = new Map();
    for (const it of openList) { const ids = new Set(rowsOf(it).map(rid)); if (it.rid) ids.add(str(it.rid)); for (const id of ids) if (id && !openOrders.has(id)) openOrders.set(id, it); }

    // Resolved (render 23504-23518)
    const clLaid = cl.open.filter(it => !customWaits(it));
    const openCustom = new Set(all.filter(it => it.kind === "customOrder").map(it => it.key.slice(4)).concat(cl.open.concat(cl.done, cl.sent).map(it => it.key.slice(6))));
    const sentRids = new Set(cl.sent.flatMap(it => it.rows.map(rid)).filter(Boolean));
    const answered = W.settled.filter(d => !sentToSheet(d) && !(d.kind === "customOrder" && openCustom.has(d.key.slice(4)))).map(d => ({ key: "settled:" + d.key + ":" + d.t, kind: d.kind, settled: d }))
      .concat(doneMarks(W).filter(d => !sentRids.has(str(d.orders[0] || ""))).map(d => ({ key: "settled:" + d.key + ":" + d.t, kind: d.kind, settled: d })));
    const inOpen = new Set(openList.flatMap(it => rowsOf(it).map(l => l.key)));
    const releasedNow = releasedCards(W, inOpen, answered).map(d => ({ key: "settled:" + d.key + ":" + d.t, kind: d.kind, settled: d }));
    const finished = cl.done.concat(cl.sent.filter(it => !it.foldedInto), clLaid, answered, releasedNow);

    // chips (render 23485-23487, 23521-23524)
    const tabOf = it => it.kind === "needsMaterial" ? "needsMapping" : (it.kind === "customOrder" && it.done && it.record && !(it.row && it.row.spec && it.row.spec.special) && kindOfWords(it.record.category)) || it.kind;
    const chipOf = k => Object.keys(CHIPS).find(c => CHIPS[c].kinds.includes(k)) || k, chipIt = it => chipOf(tabOf(it));
    const byKind = new Map(); for (const it of all) byKind.set(chipIt(it), (byKind.get(chipIt(it)) || 0) + 1);
    const customAsk = byKind.get("customOrder") || 0, customN = customAsk + clWait.length;
    if (customN) byKind.set("customOrder", customN); else byKind.delete("customOrder");
    const doneKind = new Map(); for (const it of finished) doneKind.set(chipIt(it), (doneKind.get(chipIt(it)) || 0) + 1);

    // the cards
    const cardOf = (it, state) => {
      const s = it.settled, rows = s ? (s._rows || (s.rowKey && W.byKey.get(s.rowKey) ? [W.byKey.get(s.rowKey)] : [])) : rowsOf(it), l0 = rows[0] || it.row || null;
      const orders = uniq((s ? s.orders : rows.map(rid).concat(it.rid ? [str(it.rid)] : it.record && it.record.receiptId ? [str(it.record.receiptId)] : [])).filter(Boolean)).sort(byStr);
      const p = it.problem || null, chip = chipIt(it);
      const t = s ? num(s.t) : num(it.t);
      const doneAt = s ? num(s.t) : it.record ? Math.max(doneTime(it.record), num(it.record.at)) : 0;
      const at = Math.max(t, doneAt, ...rows.map(activity));
      return {
        key: it.key, mkey: s ? it.key : mkeyOf(it), kind: it.kind, chip: ORDER.includes(chip) ? chip : "", state,
        t, at: state === "Resolved" ? (doneAt || at) : at,
        orders, lineKeys: rows.map(l => l.key), sku: str((p && p.sku) || (l0 && l0.spec && l0.spec.designSku) || (l0 && l0.line.sku) || (it.record && it.record.sku) || ""),
        listingId: str((p && p.listingId) || (l0 && l0.line.listingId) || ""), title: str((l0 && l0.line.title) || (it.record && it.record.title) || ""),
        why: s ? s.why : customWhyOf(l0, it), problems: uniq((it.problems && it.problems.length ? it.problems : p ? [p] : []).map(q => str(q.kind)).filter(Boolean)),
        flags: { info: !!it.info, done: !!it.done, decided: !!it.decided, waits: it.kind === "customOrder" && it.info && !it.done && !it.decided ? customWaits(it) : false, folded: !!it.foldedInto }
      };
    };
    const openCards = openList.map(it => cardOf(it, "Open")).sort((a, b) => (b.at - a.at) || byStr(a.key, b.key));
    const doneCards = finished.map(it => cardOf(it, "Resolved")).sort((a, b) => (b.at - a.at) || byStr(a.key, b.key));

    const counts = { open: all.length + clWait.length, resolved: finished.length, openCount: all.length + clWait.length, reviewCount, customOrders: customN, customAsk, customWait: clWait.length };
    for (const k of ORDER) counts[k] = byKind.get(k) || 0;
    counts.held = counts.heldOrder;
    counts.noChip = { orderChanged: byKind.get("orderChanged") || 0, reviewMark: byKind.get("reviewMark") || 0 };
    counts.resolvedChips = {}; for (const k of ORDER) counts.resolvedChips[k] = doneKind.get(k) || 0;
    counts.reviewOrders = reviewOrders(W, items, new Set(openOrders.keys()));
    if (W.engraving) counts.designOrders = W.designN || 0;

    return {
      at: W.now, cards: openCards.concat(doneCards), counts,
      openOrders: [...openOrders.keys()].sort(byStr),
      openKeys: openList.map(it => it.key).sort(byStr),
      resolvedKeys: finished.map(it => it.key).sort(byStr),
      itemKeys: items.map(it => it.key).sort(byStr)
    };
  }

  return { build, problemText, decisionKey, KIND_WORDS, ORDER, NO_CHIP, CHIPS, _world: world };
});
