/* Charm Nest · Send to Review (window.CharmNestReviewMark). Paul, 12 Oct 2026: "one button per each order ... that allows me to mark an
   order for review and add it to the review tab". This file is the model: a person puts an ORDER into the Review tab on their word, and takes it
   out again. It has no screen of its own but the small additions to a Review row (decorateRow); the button is in the Orders tab (REVBTN).

   What it stores: one document per marked order, "{receiptId}_review", in the custom-order records (Charm_Custom_Orders, or its Sandbox_ copy in
   the sandbox, so a sandbox order is marked in the sandbox and a reset clears it). It rides what the page already reads: the maps' list at load
   and the Review tab's 2 s changes feed (charm-nest-review-live.js), so another computer sees a mark within seconds with NO new listener, NO poll and
   NO extra read. Two writes in all, one when marked and one when resolved (charmNestLibrary reviewMarkPut). It is no completion and no seal.
   The page keeps the records in B.maps.reviewMarks (rid + "_review" -> record), apart from the completed / reopened ones (Orders.mergeCustom and
   Orders.takeCustom route them), and the Review list builds one item for each open mark (Review.syncOrderItems asks itemsFor).

     mark(rid, {by})                     -> Promise<{ok, already, rid, error?}>   idempotent; an order already in Review writes nothing
     resolve(rid, {by, how, sku, rowKey}) -> Promise<{ok, already, error?}>       how: "ok" (the match is right) | "pick" (a vector file was chosen)
     isMarked(rid) / markedRids() / openRids() / info(rid) / reasonOf(rid)         synchronous, no read; true / listed only while unresolved
     onChange(fn)                          fn({rid, kind: "marked" | "resolved" | "synced"}), also the document event "cn-review-mark"
     open(rid)                             the Review tab, at that order's row                                                              */
(function (root) {
  "use strict";
  const doc = root.document;
  const subs = new Set(), flying = new Map(), seen = new Map();
  const ridOf = v => String(v == null ? "" : v).replace(/\D/g, "").slice(0, 30);
  const keyOf = rid => ridOf(rid) + "_review";
  const maps = () => { const B = root.B; if (!B) return {}; if (!B.maps) B.maps = {}; return B.maps.reviewMarks || {}; };
  const recOf = rid => maps()[keyOf(rid)] || null;
  const rvOf = rid => { const r = recOf(rid); return r && r.review && typeof r.review === "object" ? r.review : null; };
  const isMarked = rid => { const v = rvOf(rid); return !!v && v.state === "open"; };
  const markedRids = () => { const out = []; for (const [k, r] of Object.entries(maps())) if (r && r.review && r.review.state === "open") out.push(ridOf(r.receiptId || k)); return out; };
  const toast = (m, kind) => { try { if (typeof root.toast === "function") root.toast(m, kind); } catch (_) {} };
  const sandbox = () => { try { return typeof WORKSPACE_SANDBOX !== "undefined" && !!WORKSPACE_SANDBOX; } catch (_) { return false; } };

  /** What a mark says of itself: the plain words under the card and on the Approve line ("Sent to Review by Name One"). */
  function reasonOf(rid) { const v = isMarked(rid) ? rvOf(rid) : null; return v ? "Sent to Review" + (v.markedBy ? " by " + v.markedBy : "") : ""; }
  function info(rid) {
    const v = rvOf(rid); if (!v) return null;
    return { rid: ridOf(rid), state: v.state === "open" ? "open" : "done", by: v.markedBy || "", at: +v.markedAt || 0, doneBy: v.doneBy || "", doneAt: +v.doneAt || 0, how: v.how || "", sku: v.sku || "", rowKey: v.rowKey || "" };
  }
  const whenOf = t => {
    if (!(t > 0)) return ""; const d = new Date(t), today = new Date();
    const hm = d.toLocaleTimeString(undefined, { hour: "numeric", minute: "2-digit" });
    return d.toDateString() === today.toDateString() ? hm : d.toLocaleDateString(undefined, { month: "short", day: "numeric" }) + " " + hm;
  };
  const whyOf = rid => { const v = rvOf(rid); return v ? reasonOf(rid) + (v.markedAt ? " · " + whenOf(+v.markedAt) : "") : ""; };

  function emit(ev) {
    for (const fn of [...subs]) { try { fn(ev); } catch (e) { console.warn("[ReviewMark] listener", e); } }
    try { if (doc && typeof root.CustomEvent === "function") doc.dispatchEvent(new root.CustomEvent("cn-review-mark", { detail: ev })); } catch (_) {}
  }
  function onChange(fn) { if (typeof fn !== "function") return () => {}; subs.add(fn); return () => subs.delete(fn); }
  const stateSig = rec => { const v = rec && rec.review; return v ? v.state + ":" + (v.state === "open" ? v.markedAt : v.doneAt) + ":" + (v.markedBy || "") : ""; };

  /** The page's records changed behind its back (the maps' list, the 2 s changes feed, another tab): tell the listeners which orders changed. Called by Review.syncOrderItems; cheap. */
  function sync() {
    const m = maps(), now = new Set();
    for (const [k, r] of Object.entries(m)) {
      if (!r || !r.review) continue;
      const rid = ridOf(r.receiptId || k), sig = stateSig(r); now.add(rid);
      if (seen.get(rid) !== sig) { seen.set(rid, sig); emit({ rid, kind: "synced" }); }
    }
    for (const rid of [...seen.keys()]) if (!now.has(rid)) { seen.delete(rid); emit({ rid, kind: "synced" }); }
  }

  function put(rec) {   // the record joins the page's map at once (a new object, as the maps are replaced whole)
    const B = root.B; if (!B || !rec || !rec.key) return;
    B.maps.reviewMarks = Object.assign({}, B.maps.reviewMarks, { [rec.key]: rec });
    try { if (B.maps.customWrites) B.maps.customWrites.set(rec.key, Date.now()); } catch (_) {}
  }
  function drop(key) { const B = root.B; if (!B || !B.maps.reviewMarks || !(key in B.maps.reviewMarks)) return; const n = Object.assign({}, B.maps.reviewMarks); delete n[key]; B.maps.reviewMarks = n; }
  const rowsOfRid = rid => { try { return (root.Orders && root.Orders.rows ? root.Orders.rows() : []).filter(r => r && r.order && String(r.order.receiptId) === rid && r.state !== "gone"); } catch (_) { return []; } };
  const redraw = () => { try { if (root.Review && root.Review.syncOrderItems) root.Review.syncOrderItems(); } catch (e) { console.warn("[ReviewMark] redraw", e); } };

  /** The signed-in login's name (never typed here): the one the page already keeps; none = the small name bar asks, and nothing is written. */
  async function whoIs(opts) {
    let n = String((opts && opts.by) || "").trim();
    if (!n) { try { if (typeof employeeName === "function") n = String(employeeName() || "").trim(); } catch (_) {} }
    if (!n) { try { n = String((root.B && root.B.employee) || "").trim(); } catch (_) {} }
    if (!n) { try { if (typeof needEmployee === "function") n = String((await needEmployee("Kept with this Send to Review.")) || "").trim(); } catch (_) {} }
    return n.slice(0, 80);
  }
  function told(rid, rec) {   // after the cloud answered: the other tabs of this computer hear the record (BroadcastChannel, no read), and the feed reads soon
    try { if (root.ReviewLive && root.ReviewLive.announce) root.ReviewLive.announce("customPut", { record: rec }); } catch (_) {}
    try { if (root.ReviewLive && root.ReviewLive.nudge) root.ReviewLive.nudge(); } catch (_) {}
  }
  function line(rid, text, id, data) {   // a plain line on the order's timeline (a note: no seal)
    try { const rows = rowsOfRid(rid); if (rows[0] && root.CNTimeline) root.CNTimeline.line(rows[0], "note", { id, text: String(text).slice(0, 200), data: Object.assign({ reviewMark: true }, data || {}) }); } catch (_) {}
  }

  function run(rid, kind, body) {
    if (flying.has(rid)) return flying.get(rid);   // a second press joins the one in flight: nothing is written twice
    const p = (async () => body())().finally(() => { flying.delete(rid); });
    flying.set(rid, p); return p;
  }

  async function mark(rid, opts) {
    rid = ridOf(rid); if (!rid) return { ok: false, rid, error: "No order number" };
    if (isMarked(rid)) return { ok: true, already: true, rid };
    return run(rid, "mark", async () => {
      if (isMarked(rid)) return { ok: true, already: true, rid };
      if (!rowsOfRid(rid).length) return { ok: false, rid, error: "This order is not in the list" };
      const by = await whoIs(opts); if (!by) return { ok: false, rid, error: "No name" };
      if (isMarked(rid)) return { ok: true, already: true, rid };
      const key = keyOf(rid), was = recOf(rid), at = Date.now();
      // at once: the Review list and every Hold / button that asks isMarked say so while the cloud writes (an older answer of the feed cannot undo it: customWrites)
      put({ key, kind: "reviewMark", category: "Review", receiptId: rid, state: "open", review: { state: "open", markedAt: at, markedBy: by }, updatedAtMs: 0, pending: true });
      seen.set(rid, stateSig(recOf(rid))); emit({ rid, kind: "marked" }); redraw();
      try {
        const r = await root.api("charmNestLibrary", { op: "reviewMarkPut", action: "mark", rid, by }, { quiet: true });
        if (!r || r.error || !r.record) throw new Error((r && r.error) || "The cloud sent no record");
        put(r.record); seen.set(rid, stateSig(r.record)); told(rid, r.record); redraw();
        if (!r.already) line(rid, "Sent to Review by " + by, `rvmark.${rid}.${(r.record.review && r.record.review.markedAt) || at}`, { by });
        return { ok: true, already: !!r.already, rid };
      } catch (e) {   // not written: the order goes back to what it was (nothing else was touched)
        if (was) put(was); else drop(key);
        seen.set(rid, stateSig(recOf(rid))); emit({ rid, kind: "synced" }); redraw();
        return { ok: false, rid, error: String((e && e.message) || e || "Not saved") };
      }
    });
  }

  async function resolve(rid, opts) {
    rid = ridOf(rid); opts = opts || {}; if (!rid) return { ok: false, rid, error: "No order number" };
    if (!isMarked(rid)) return { ok: true, already: true, rid };
    return run(rid, "resolve", async () => {
      if (!isMarked(rid)) return { ok: true, already: true, rid };
      const by = await whoIs(opts); if (!by) return { ok: false, rid, error: "No name" };
      const how = opts.how === "pick" ? "pick" : opts.how === "ok" ? "ok" : "other", sku = String(opts.sku || "").slice(0, 60), rowKey = String(opts.rowKey || "").slice(0, 120);
      const key = keyOf(rid), was = recOf(rid), at = Date.now();
      const done = Object.assign({}, was.review, { state: "done", doneAt: at, doneBy: by, how }, sku ? { sku } : {}, rowKey ? { rowKey } : {});
      put(Object.assign({}, was, { review: done, updatedAtMs: 0, pending: true }));
      seen.set(rid, stateSig(recOf(rid))); emit({ rid, kind: "resolved" }); redraw();
      try {
        const r = await root.api("charmNestLibrary", { op: "reviewMarkPut", action: "resolve", rid, by, how, sku, rowKey }, { quiet: true });
        if (!r || r.error) throw new Error((r && r.error) || "The cloud sent no answer");
        if (r.record) { put(r.record); seen.set(rid, stateSig(r.record)); told(rid, r.record); }
        redraw();
        if (!r.already) line(rid, (how === "pick" ? "Review: charm picked" + (sku ? " (" + sku + ")" : "") : "Review: marked OK") + " by " + by, `rvmark.${rid}.done.${at}`, { by, how, sku });
        return { ok: true, already: !!r.already, rid };
      } catch (e) {   // not written: it is in Review again
        put(was); seen.set(rid, stateSig(was)); emit({ rid, kind: "synced" }); redraw();
        return { ok: false, rid, error: String((e && e.message) || e || "Not saved") };
      }
    });
  }

  /** The items Review.syncOrderItems adds to the list: one for each open mark whose order has a line in the pull (keys "ord:rvm:" + rid, so the list drops one the moment its mark is resolved). */
  function itemsFor(rows) {
    if (!markedRids().length) return [];
    const by = new Map();
    for (const r of rows || []) {
      if (!r || !r.order || r.state === "gone") continue;
      const rid = String(r.order.receiptId); if (!isMarked(rid)) continue;
      if (!by.has(rid)) by.set(rid, []); by.get(rid).push(r);
    }
    return [...by].map(([rid, rs]) => ({ kind: "reviewMark", key: "ord:rvm:" + rid, rid, row: rs[0], rows: rs, problem: { kind: "reviewMark", rid }, problems: [], why: whyOf(rid) }));
  }

  /** The resolved marks, for Completed (Review.render): who and what, from the cloud's record, so a reload and a second computer show them too. Newest first. */
  const doneCache = new Map();
  function doneSettled(rows) {
    const out = [], live = new Set();
    for (const [k, r] of Object.entries(maps())) {
      const v = r && r.review; if (!v || v.state !== "done") continue;
      const rid = ridOf(r.receiptId || k), rs = (rows || []).filter(x => x && x.order && String(x.order.receiptId) === rid && x.state !== "gone");
      const sig = [v.doneAt, v.doneBy, v.how, v.sku, rs.length, rs[0] && rs[0].key].join("|"); live.add(rid);
      let d = doneCache.get(rid);
      if (!d || d.sig !== sig || d.row !== (rs[0] || null)) {
        d = { sig, key: "ord:rvm:" + rid, row: rs[0] || null, kind: "reviewMark", why: v.how === "pick" ? "Charm picked" + (v.sku ? ": " + v.sku : "") : "Marked OK", lines: rs.length || 1, orders: [rid], by: v.doneBy || "", t: +v.doneAt || 0 };
        doneCache.set(rid, d);
      }
      out.push(d);
    }
    for (const rid of [...doneCache.keys()]) if (!live.has(rid)) doneCache.delete(rid);
    return out.sort((a, b) => b.t - a.t).slice(0, 200);
  }

  const rowHooks = new Set();
  /** onRow(fn): fn(node, item) is called each time a Review row of an open mark is BUILT (the row is kept while it reads the same), after its slots exist: REVSUGGEST fills the slots there. Returns an unsubscribe. */
  function onRow(fn) { if (typeof fn !== "function") return () => {}; rowHooks.add(fn); return () => rowHooks.delete(fn); }

  /** A Review row of an open mark (Review.reviewRow, once, when it is built): one empty slot for each line, where the suggested charms go (REVSUGGEST; an empty slot is nothing), and the one button that says the match was right. */
  function decorateRow(node, it) {
    try {
      if (!node || !it || it.kind !== "reviewMark") return;
      const rid = String(it.rid || ""), idn = node.querySelector(".engravingIdentity");
      if (idn && !idn.querySelector(".rvMarkSlot")) {
        const after = idn.querySelector(".rowExcerpt");
        for (const r of (it.rows && it.rows.length ? it.rows : it.row ? [it.row] : []).slice().reverse()) {
          const s = doc.createElement("div"); s.className = "rvMarkSlot"; s.dataset.row = r.key; s.dataset.rid = rid;
          if (after) after.insertAdjacentElement("afterend", s); else idn.appendChild(s);
        }
      }
      const acts = node.querySelector(".rowActions");
      if (acts && !acts.querySelector("[data-rv-markok]")) {
        const b = doc.createElement("button"); b.type = "button"; b.className = "btn gold sm"; b.dataset.rvMarkok = "1"; b.textContent = "Mark OK";
        b.title = "the match is right: this order leaves Review";
        b.onclick = async e => {
          e.stopPropagation(); if (b.disabled) return;
          b.disabled = true; b.setAttribute("aria-busy", "true"); b.innerHTML = '<span class="spin"></span>Saving';
          const r = await resolve(rid, { how: "ok" });
          if (b.isConnected) { b.disabled = false; b.removeAttribute("aria-busy"); b.textContent = "Mark OK"; }
          if (!r.ok && r.error !== "No name") toast("Not saved: " + r.error, "bad");
        };
        acts.appendChild(b);
      }
      for (const fn of [...rowHooks]) { try { fn(node, it); } catch (e) { console.warn("[ReviewMark] row hook", e); } }
    } catch (e) { console.warn("[ReviewMark] row", e); }
  }

  /** The Review tab, at this order's row (REVBTN: "in Review" opens it there). */
  function open(rid) {
    rid = ridOf(rid); if (!rid || !isMarked(rid)) return false;
    try { if (root.Review && root.Review.showCard) { root.Review.showCard("ord:rvm:" + rid, "open", "reviewMark"); return true; } } catch (e) { console.warn("[ReviewMark] open", e); }
    return false;
  }

  /** One read of the Review live feed right now (the same one the open Review, Orders and Library tabs make every 2 s, which rests while the Nest or Engraving tab is on screen): for a gate on such a tab to ask just before it acts. Resolves true when the feed answered. */
  async function refresh() { try { return !!(root.ReviewLive && root.ReviewLive.poll && await root.ReviewLive.poll({ quiet: true })); } catch (_) { return false; } }

  /** A sandbox reset cleared the records: forget them here too. */
  function reset() { const B = root.B; if (B && B.maps) B.maps.reviewMarks = {}; seen.clear(); doneCache.clear(); }

  root.CharmNestReviewMark = { mark, resolve, isMarked, markedRids, openRids: markedRids, info, reasonOf, onChange, onRow, refresh, open, itemsFor, doneSettled, decorateRow, sync, reset, _key: keyOf, _sandbox: sandbox };
})(typeof window !== "undefined" ? window : globalThis);
