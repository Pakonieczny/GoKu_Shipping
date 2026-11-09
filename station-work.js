/*  station-work.js — Complete Order at the production stations (Paul, 9 Nov 2026: "If a user scans a QR code, it should just open that order and as
 *  long as the user doesn't press complete that order never gets completed").
 *  No panel, no extra step: a scan, a typed number or a phone relay opens the order exactly as before and only records the `scan`. The station's one
 *  Complete Order button completes the order on screen: the station's own completion (config.complete: Welding stamps `welded` per stud line,
 *  Assembly `assembled`, Shipping posts tracking to Etsy and stamps shipped / etsyCompleted as it always did), through StationTimeline.guard (a
 *  cancelled order asks first) unless the station guards itself, then one `complete` activity: the order, its pieces, "Complete Order" and workMs =
 *  the time from the order being opened at this station to the press. Pressed again on an order this station already completed: a `note`, never
 *  counted twice. The live board's order in hand ends at Complete Order, and otherwise after its quiet time (the station zeroes out by itself).
 *
 *    StationWork.init({ station, device, label, complete(orderId) → Promise<{ ok, parts?, detail?, message? }>, guarded?: true })
 *    StationWork.viewed(orderId, { parts })     the page has opened this order (after its scan)
 *    StationWork.complete(orderId)              → Promise<boolean>   (the Complete Order button)
 *  Never touches Firestore or Etsy itself; the activity event goes through station-activity.js's queue. Nothing here throws into the page. */
(function () {
  "use strict";
  if (window.StationWork) return;
  const DONE_KEY = "station_work_done.", DONE_MS = 12 * 3600e3;
  const cfg = { station: "", device: "", label: "", complete: null, guarded: false };
  const S = { opened: new Map(), busy: false, last: null };
  const warn = (...a) => { try { console.warn("[StationWork]", ...a); } catch (_) {} };
  const digits = v => String(v == null ? "" : v).replace(/\D/g, "").slice(0, 30);
  const clean = (v, n) => String(v == null ? "" : v).replace(/[\u0000-\u001f\u007f]/g, " ").replace(/\s+/g, " ").trim().slice(0, n);
  const act = (action, o) => { try { return !!(window.StationActivity && StationActivity.log(action, o)); } catch (_) { return false; } };
  const toast = html => { try { if (window.M && M.toast) M.toast({ html }); } catch (_) {} };
  const esc = s => String(s).replace(/[&<>"]/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));

  /* the orders this station completed (12 h, kept across a reload): a second press is a note, never a second completion */
  function doneMap() {
    try { const o = JSON.parse(localStorage.getItem(DONE_KEY + cfg.device) || "{}"); return o && typeof o === "object" ? o : {}; } catch (_) { return {}; }
  }
  function markDone(id) {
    try {
      const o = doneMap(), now = Date.now(); o[id] = now;
      for (const k of Object.keys(o)) if (!(now - o[k] < DONE_MS)) delete o[k];
      const ks = Object.keys(o); if (ks.length > 300) ks.sort((a, b) => o[a] - o[b]).slice(0, ks.length - 300).forEach(k => delete o[k]);
      localStorage.setItem(DONE_KEY + cfg.device, JSON.stringify(o));
    } catch (_) {}
  }
  const wasDone = id => { const t = Number(doneMap()[id]) || 0; return t > 0 && Date.now() - t < DONE_MS; };

  function viewed(orderId, o) {
    try {
      const id = digits(orderId); if (!id) return;
      const parts = Math.max(0, Math.floor(Number(o && o.parts)) || 0), had = S.opened.get(id);
      S.opened.delete(id);
      S.opened.set(id, { at: had ? had.at : Date.now(), parts: parts || (had ? had.parts : 0) });   // (opened again: the time runs from the first opening)
      while (S.opened.size > 60) S.opened.delete(S.opened.keys().next().value);
    } catch (e) { warn("viewed", e); }
  }

  async function complete(orderId) {
    const id = digits(orderId);
    if (!id) { toast("Open an order first."); return false; }
    if (S.busy) return false;
    if (!cfg.guarded && S.last && S.last.id === id && Date.now() - S.last.at < 3000) return true;   // (a double-click: the order was completed a moment ago; Shipping keeps its own rule)
    S.busy = true;
    try {
      if (!cfg.guarded && window.StationTimeline && typeof StationTimeline.guard === "function" && !(await StationTimeline.guard(id, { action: "Complete Order" }))) return false;
      let r = null;
      try { r = cfg.complete ? await cfg.complete(id) : { ok: true }; } catch (e) { r = { ok: false, message: String((e && e.message) || e || "failed").slice(0, 160) }; }
      if (!r || r.ok === false) { if (r && r.message) toast(esc(r.message)); return false; }
      const seen = S.opened.get(id);
      if (wasDone(id)) act("note", { orderId: id, detail: "Complete pressed on an order already completed" });
      else {
        const ev = { orderId: id, parts: Math.max(0, Math.floor(Number(r.parts)) || 0) || (seen ? seen.parts : 0), orders: 1, detail: "Complete Order" + (r.detail ? " · " + clean(r.detail, 60) : "") };
        if (seen) ev.workMs = Math.max(0, Date.now() - seen.at);
        act("complete", ev);
        markDone(id);
        if (cfg.label && !cfg.guarded) toast(`Order #${esc(id)} completed at ${esc(cfg.label)}.`);
      }
      S.opened.delete(id); S.last = { id, at: Date.now() };
      try { if (!(window.StationLiveOrder && StationLiveOrder.end && StationLiveOrder.end(id)) && window.StationActivity && StationActivity.idle) StationActivity.idle(id); } catch (_) {}
      return true;
    } catch (e) { warn("complete", e); return false; }
    finally { S.busy = false; }
  }

  function init(o) {
    try {
      o = o || {};
      for (const k of ["station", "device", "label"]) if (o[k] != null) cfg[k] = clean(o[k], 40);
      if (typeof o.complete === "function") cfg.complete = o.complete;
      if (o.guarded != null) cfg.guarded = !!o.guarded;
    } catch (e) { warn("init", e); }
    return api;
  }
  const api = { init, viewed, complete, completedHere: id => wasDone(digits(id)), config: () => ({ station: cfg.station, device: cfg.device, label: cfg.label }) };
  window.StationWork = api;
})();
