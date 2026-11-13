/* charm-nest-run-follow.js — A COMPUTER THAT DOES NOT HOLD THE RUN FOLLOWS THE RUN IN THE CLOUD (RUNFOLLOW, Paul, 13 Nov 2026: "How come the other computer didn't update",
   after 24 orders were taken out of the run on the computer that holds it and another computer still listed them as pooled and waiting).

   What there was: the computer that holds the run (B.run) writes the run's lines to the cloud (RunCtl.save, run.lines). Every other computer shows its OWN saved copy of the
   orders (its workspace checkpoint) and, since CLOUDLINES, adds the lines the cloud has that it lacks. A line the run changed (held, pooled, taken out, gone) stayed as that
   copy had it, for as long as that computer lived.

   What this does, on a computer with no run of its own only (the computer that holds the run is the run, and is never touched): the open run's record is read from the cloud
   (runList, then runGet: one read each) at start and whenever the changes feed names it (r:<runId>, the run's lines were saved), and every line of this page is made what the
   cloud says: its state, pool ids, reason, hold, wait and engraving summary. A line the run once held (read from its record, or its order is in the run's orders) that the run's lines no longer carry
   was taken out there: it is gone here too. A line the cloud has and this page lacks is added (Orders.rowFromRecord). Nothing is written to the cloud.

   window.CharmNestRunFollow: state() · now() */
(function (root) {
  "use strict";
  const START_DELAY = 8000, NET_MS = 300000;
  const safe = (f, d) => { try { const v = f(); return v === undefined ? d : v; } catch (_) { return d; } };
  const api = body => root.CN && typeof root.CN.api === "function" ? root.CN.api("charmNestLibrary", body, { quiet: true }) : Promise.reject(new Error("no api"));
  const st = { runId: null, reads: 0, changed: 0, gone: 0, added: 0, last: null, busy: false, skipped: "" };
  const FIELDS = ["state", "poolIds", "reason", "hold", "wait"];
  const holdsRun = () => !!(root.B && root.B.run);

  async function openRunId() {
    const res = await api({ op: "runList", limit: 10 });
    const runs = ((res && res.runs) || []).filter(r => r && r.runId && !["abandoned", "complete", "archived"].includes(r.status));
    return runs.length ? runs[0].runId : null;
  }
  async function now(why) {
    if (st.busy) return st;
    if (holdsRun()) { st.skipped = "this computer holds the run"; return st; }
    st.busy = true;
    try {
      const id = st.runId || await openRunId(); if (!id) { st.skipped = "no open run"; return st; }
      st.runId = id;
      const res = await api({ op: "runGet", runId: id }); st.reads++;
      const run = res && (res.run || res); if (!run || !run.lines) return st;
      if (holdsRun()) return st;   // (the run was opened here while the read was on its way)
      const lines = run.lines, inRun = new Set((run.orders || []).map(String)), O = root.Orders, B = root.B;
      if (!O || !B || !B.orders) return st;
      let changed = 0, gone = 0, added = 0;
      for (const row of O.rows()) {
        if (!row || !row.key || row.parentRow) continue;
        const l = lines[row.key];
        if (!l) {
          if (row.state !== "gone" && row.state !== "committed" && (row.fromRecord || inRun.has(String(row.order && row.order.receiptId)))) {   // (a line read from the run record was in the run; the record moves a gone line out of its lines)
            row.state = "gone"; row.reason = row.reason || "taken out of the run"; row.poolIds = []; row.wait = null; gone++; }
          continue;
        }
        let moved = false;
        for (const f of FIELDS) { const v = l[f] === undefined ? (f === "poolIds" ? [] : null) : l[f]; if (JSON.stringify(row[f] == null ? (f === "poolIds" ? [] : null) : row[f]) !== JSON.stringify(v)) { row[f] = Array.isArray(v) ? v.slice() : v; moved = true; } }
        if (l.engrave && row.engrave && typeof row.engrave === "object") { for (const k of ["state", "approved", "approvedAt", "approvedBy", "decidedAt", "seals", "text"]) if (l.engrave[k] !== undefined && JSON.stringify(row.engrave[k]) !== JSON.stringify(l.engrave[k])) { row.engrave[k] = l.engrave[k]; moved = true; } }
        if (moved) changed++;
      }
      const newOrders = new Set();
      if (typeof O.rowFromRecord === "function") for (const [k, l] of Object.entries(lines)) { if (B.orders.byKey.has(k)) continue; const row = safe(() => O.rowFromRecord(k, l), null); if (row) { B.orders.rows.push(row); B.orders.byKey.set(k, row); added++; newOrders.add(k.split("_")[0]); } }
      // (NEWLINES, Paul 13 Nov 2026: "What about future orders?"): a line that arrives here takes its engraving work from the cloud at once (it may have been written before the line reached this computer)
      if (newOrders.size) safe(() => root.CharmNestEngraveSync && root.CharmNestEngraveSync.pull([...newOrders], "keys"));
      st.changed += changed; st.gone += gone; st.added += added; st.last = { at: Date.now(), why, changed, gone, added };
      if (changed || gone || added) { safe(() => root.Review && root.Review.syncOrderItems()); safe(() => O.render()); safe(() => root.Session && root.Session.schedule()); }
      return st;
    } catch (e) { st.last = { at: Date.now(), why, error: String(e && e.message || e) }; return st; }
    finally { st.busy = false; }
  }
  function follow() {
    const Y = root.CNSync; if (!Y || typeof Y.follow !== "function") return;
    Y.follow({ name: "run-follow", kinds: "r", active: () => true, wants: k => k === "r:*" || !st.runId || k === "r:" + st.runId, read: why => now(why) });
  }
  function boot() {
    let tries = 0;
    const go = () => {
      tries++;
      const ready = !!(root.Orders && root.B && root.B.orders && root.CN && typeof root.CN.api === "function" && safe(() => root.Orders.rows().length > 0, false));
      if (!ready && tries < 120) return setTimeout(go, 1000);
      follow();
      setTimeout(() => now("start"), START_DELAY);
      setInterval(() => { if (!(root.document && root.document.hidden)) now("net"); }, NET_MS);
    };
    go();
  }
  root.CharmNestRunFollow = { state: () => Object.assign({}, st), now };
  if (root.document) { if (root.document.readyState === "loading") root.document.addEventListener("DOMContentLoaded", boot); else boot(); }
})(typeof self !== "undefined" ? self : this);
