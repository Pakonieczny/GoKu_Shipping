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
  const FIELDS = ["state", "poolIds", "reason", "hold", "wait", "heldAt", "_lp"];   // (CLOUDALL S2, Paul 13 Nov 2026: "Everything must be on the cloud nothing can be on one computer"): and the stamp of a line another computer changed
  const holdsRun = () => !!(root.B && root.B.run);

  async function openRunId() {
    const res = await api({ op: "runList", limit: 10 });
    const runs = ((res && res.runs) || []).filter(r => r && r.runId && !["abandoned", "complete", "archived"].includes(r.status));
    return runs.length ? runs[0].runId : null;
  }
  async function now(why) {
    if (holdsRun()) runCommands().catch(() => {});   // (SHEETSANYWHERE: the commands of the computers that follow, apart from the 15 s throttle of applyOthers)
    if (st.busy) return st;
    if (holdsRun()) return applyOthers(why);   // (CLOUDALL S2, Paul 13 Nov 2026: "Everything must be on the cloud nothing can be on one computer"): the run's own computer takes the lines other computers changed
    st.busy = true;
    try {
      const id = st.runId || await openRunId(); if (!id) { st.skipped = "no open run"; return st; }
      st.runId = id;
      const res = await api({ op: "runGet", runId: id }); st.reads++;
      const run = res && (res.run || res); if (!run || !run.lines) return st;
      if (holdsRun()) return st;   // (the run was opened here while the read was on its way)
      // (FOLLOWSTATE, Paul 15 Nov 2026: "Michelle's computer doesn't say that it just says running"): the run's state as the cloud has it, for this computer's banner
      { const was = st.run ? st.run.status + "|" + (st.run.stoppedBy || "") : ""; st.run = { status: run.status || null, stoppedBy: run.stoppedBy || null, step: run.step || null, at: Date.now() }; if (was !== st.run.status + "|" + (st.run.stoppedBy || "")) safe(() => root.RunCtl && root.RunCtl.renderBanner()); }
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
      // (FOLLOWSPEC, Paul 15 Nov 2026: "Cannot read properties of null (reading 'fontText')"): a line added here from the run's record is READ at once (its charm, its words, its font: Orders.interpretAll), as the lines another computer added to the run's own page are; it was left unread, and the engraving view of such an order could not open
      if (added || O.rows().some(r => r && !r.spec && !r.parentRow)) safe(() => O.interpretAll());
      if (newOrders.size) safe(() => root.CharmNestEngraveSync && root.CharmNestEngraveSync.pull([...newOrders], "keys"));
      st.changed += changed; st.gone += gone; st.added += added; st.last = { at: Date.now(), why, changed, gone, added };
      if (changed || gone || added) { safe(() => root.Review && root.Review.syncOrderItems()); safe(() => O.render()); safe(() => root.Session && root.Session.schedule()); }
      // (CLOUDFIRST, Paul 15 Nov 2026: "The library tab is the correct one. The nesting on the nest tab is incorrect"): and the run's sheets, as the cloud has them, on this computer's Nest cards (Recall.follow: read only, at most every ten minutes)
      safe(() => { const f = root.Recall && root.Recall.follow; if (typeof f === "function") Promise.resolve(f(id)).catch(() => {}); });
      return st;
    } catch (e) { st.last = { at: Date.now(), why, error: String(e && e.message || e) }; return st; }
    finally { st.busy = false; }
  }
  /* ── (CLOUDALL S2, Paul 13 Nov 2026: "Everything must be on the cloud nothing can be on one computer") ──
     patchRows(rows): this computer changed these order lines (a piece taken off, held, cancelled): they are written to the cloud run line by line (runLinesPatch), stamped.
     applyOthers(): on the computer whose page runs the run, a line another computer stamped after this page's own copy is taken into its row (state, pieces, hold, engraving), its
     pieces that left are dropped from this page's pool and engraving lists; its next whole save carries the stamp, so the server keeps what the other computer did. */
  const dev = () => { let v = safe(() => root.localStorage.getItem("cn.dev"), ""); if (!/^d[0-9a-z]{4,14}$/.test(v || "")) { v = "d" + Math.random().toString(36).slice(2, 12); safe(() => root.localStorage.setItem("cn.dev", v)); } return v; };
  const who = () => String(safe(() => root.B && root.B.employee, "") || "").trim();
  let lastApply = 0, applyT = 0;
  async function applyOthers(why) {
    const run = root.B && root.B.run; if (!run || !run.runId) return st;
    const t = Date.now(); if (t - lastApply < 15000) { if (!applyT) applyT = setTimeout(() => { applyT = 0; applyOthers("later"); }, 15000 - (t - lastApply)); return st; }
    if (st.busy) return st; st.busy = true; lastApply = t;
    try {
      const res = await api({ op: "runGet", runId: run.runId }); const cr = res && (res.run || res); if (!cr || !cr.lines) return st;
      if (!cr.lineEditAt || (st.appliedAt && +cr.lineEditAt <= +st.appliedAt)) return st;
      const O = root.Orders, B = root.B, me = dev(), takenOff = []; let n = 0;
      for (const row of O.rows()) {
        const l = cr.lines[row.key], lp = l && l._lp; if (!lp || lp.dev === me) continue;
        if (row._lp && +row._lp.at >= +lp.at) continue;
        const was = (row.poolIds || []).slice();
        for (const f of ["state", "poolIds", "reason", "hold", "wait", "heldAt"]) { const v = l[f] === undefined ? (f === "poolIds" ? [] : null) : l[f]; row[f] = Array.isArray(v) ? v.slice() : v; }
        if (l.engrave && row.engrave && typeof row.engrave === "object") for (const k of ["state", "approved", "approvedAt", "approvedBy", "decidedAt", "seals", "text"]) if (l.engrave[k] !== undefined) row.engrave[k] = l.engrave[k];
        const gone = was.filter(id => !(row.poolIds || []).includes(id));
        for (const id of gone) safe(() => B.pool.rows.delete(id));
        if (gone.length) safe(() => { for (const j of root.Engrave.jobsOf(row)) { j.copies = (j.copies || []).filter(id => !gone.includes(id)); if (!j.copies.length) { root.Engrave.items().delete(j.key); if (root.Review) root.Review.remove("eng:" + j.key); } } });
        row._lp = lp; n++;
        if (gone.length) takenOff.push(...gone);
      }
      // (TAKEOFFSOLID G2, Paul, 15 Nov 2026): the pieces that left a line leave this computer's sheets too (in memory, nothing marked dirty: the computer that took them off writes the sheet)
      if (takenOff.length) safe(() => root.SheetWin && root.SheetWin.dropTakenOff && root.SheetWin.dropTakenOff(takenOff));
      // (CLOUDALL S3, Paul 13 Nov 2026: "New orders. They join the run only on that computer" -> any computer that reads new orders writes them into the cloud run): the order lines another computer ADDED to the cloud run come into this page's run and go on as its own new orders (placed at the run's next intake)
      let added = 0; const newRids = new Set();
      for (const [k, l] of Object.entries(cr.lines)) { const lp = l && l._lp; if (!lp || !lp.added || lp.dev === me || B.orders.byKey.has(k)) continue; const row = safe(() => O.rowFromRecord(k, l), null); if (!row) continue; row._lp = lp; B.orders.rows.push(row); B.orders.byKey.set(k, row); added++; newRids.add(String(l.orderId || k.split("_")[0])); }
      if (added) { run.orders = [...new Set((run.orders || []).map(String).concat([...newRids]))]; safe(() => O.interpretAll()); safe(() => { const a = root.Arrivals && root.Arrivals.state && root.Arrivals.state(); if (a) a.pending = true; }); safe(() => root.Arrivals.processPending().catch(() => {})); n += added; st.addedFromOthers = (st.addedFromOthers || 0) + added; }
      st.appliedAt = +cr.lineEditAt; st.applied = (st.applied || 0) + n; st.last = { at: Date.now(), why, fromOthers: n };
      if (n) { safe(() => root.Review && root.Review.syncOrderItems()); safe(() => O.render()); safe(() => root.Engrave && root.Engrave.render()); safe(() => root.Session && root.Session.schedule()); }
      return st;
    } catch (e) { st.last = { at: Date.now(), why, error: String((e && e.message) || e) }; return st; }
    finally { st.busy = false; }
  }
  /* (SHEETSANYWHERE, Paul 15 Nov 2026: "I need that full capability ... drag the sheets from one sheet to another and amalgamate sheets as needed"): the computer that CARRIES the run does what a following computer asked for in the Sheets card
     (runCmdPut on the cloud): one at a time, in the order asked, through the Gate, then answers (runCmdDone) with the result line. Looked at when the feed names the run, at most every 3 s; a command older than 10 minutes is answered "too old". */
  let cmdAt = 0, cmdT = 0, cmdBusy = false; const cmdSeen = new Set();
  async function execCmd(c) {
    const G = root.Gate, a = c.args || {}; if (!G) return { ok: false, text: "The sheets are not ready on the computer that carries the run." };
    if (c.type === "reorder") {
      const rows = safe(() => G.sheetsOf(a.metal), []), have = rows.map(r => r.id), want = [...new Set((a.ids || []).filter(id => have.includes(id)))];
      have.forEach((id, i) => { if (!want.includes(id)) want.splice(Math.min(i, want.length), 0, id); });
      const r = G.reorder(a.metal, want); return r && r.ok ? { ok: true, text: r.changed === false ? "The sheets were already in that order" : "Sheet order changed: " + (r.names || []).join(", ") } : { ok: false, text: (r && r.why) || "The order was not changed" };
    }
    let r = null;
    if (c.type === "repicture") r = root.Recall && typeof root.Recall.repicture === "function" ? await root.Recall.repicture(a.sheetId) : { ok: false, why: "not ready" };   // (REPICTURE: a sheet's saved picture drawn again, nothing else of it written)
    else if (c.type === "consolidate") r = await G.consolidate(a.sourceId, a.targetId);
    else if (c.type === "seat") r = typeof G.seatOn === "function" ? await G.seatOn(a.sheetId, a.partialId) : { ok: false, why: "not ready" };   // (ONEGRID: a nested sheet's pieces onto a physical sheet)
    else if (c.type === "empty") r = await G.emptySheet(a.sheetId);
    else if (c.type === "delete") r = await G.deleteEmpty(a.sheetId);
    else return { ok: false, text: "Unknown command" };
    return r && r.ok !== false ? { ok: true, text: r.text || "Done" } : { ok: false, text: (r && (r.why || r.text)) || "Not done" };
  }
  async function runCommands() {
    if (cmdBusy) return; const t = Date.now();
    if (t - cmdAt < 3000) { if (!cmdT) cmdT = setTimeout(() => { cmdT = 0; runCommands().catch(() => {}); }, 3000 - (t - cmdAt)); return; }
    cmdAt = t; cmdBusy = true;
    try {
      for (let guard = 0; guard < 12; guard++) {
        const run = root.B && root.B.run; if (!run || !run.runId) break;
        const res = await api({ op: "runCmdList", runId: run.runId }), me = dev();
        const next = ((res && res.cmds) || []).filter(c => c && c.id && c.state === "waiting" && c.dev !== me && !cmdSeen.has(c.id)).sort((x, y) => (+x.at || 0) - (+y.at || 0))[0];
        if (!next) break;
        cmdSeen.add(next.id); let out;
        if (Date.now() - (+next.at || 0) > 600000) out = { ok: false, text: "too old" };
        else { try { out = await execCmd(next); } catch (e) { out = { ok: false, text: String((e && e.message) || e) }; } }
        for (let k = 0; k < 3; k++) { try { await api({ op: "runCmdDone", runId: run.runId, id: next.id, ok: !!out.ok, text: String(out.text || "").slice(0, 400), dev: me }); break; } catch (_) { await new Promise(r => setTimeout(r, 1500)); } }
      }
    } catch (_) { /* looked at again at the next feed */ } finally { cmdBusy = false; }
  }
  async function patchRows(rows) {
    rows = (rows || []).filter(r => r && r.key && !r.parentRow); if (!rows.length) return null;
    if (holdsRun()) return null;   // (the run's own page saves these lines with the run: RunCtl.save)
    const runId = st.runId || await openRunId(); if (!runId) return null;
    const O = root.Orders; if (!O || typeof O.lineRecord !== "function") return null;
    const lines = {};
    for (const r of rows) { const rec = safe(() => O.lineRecord(r), null), l = rec && rec[1]; if (!l) continue; lines[r.key] = { state: l.state, poolIds: l.poolIds || [], reason: l.reason || null, hold: l.hold || null, heldAt: r.heldAt || null, holdSeen: !!r.holdSeen, wait: l.wait || null, engrave: l.engrave || null }; }
    if (!Object.keys(lines).length) return null;
    let res = null, err = null;
    for (let t = 0; t < 4; t++) { try { res = await api({ op: "runLinesPatch", runId, lines, dev: dev(), by: who() }); err = null; break; } catch (e) { err = e; await new Promise(r => setTimeout(r, 1500 * (t + 1))); } }
    if (err) throw err;
    if (res && res.at) for (const r of rows) if ((res.applied || []).includes(r.key)) r._lp = { at: res.at, dev: dev(), by: who() };
    st.patched = (st.patched || 0) + ((res && res.applied) || []).length;
    return res;
  }
  /** (CLOUDALL S3, Paul 13 Nov 2026: "New orders. They join the run only on that computer" -> any computer that reads new orders writes them into the cloud run): this computer took in these new order lines; they are written into the cloud run (runLinesAdd) so the run places them whichever computer works it */
  async function addRows(rows) {
    rows = (rows || []).filter(r => r && r.key && !r.parentRow); if (!rows.length) return null;
    if (holdsRun()) return null;   // (this page's own run takes them in and saves them)
    const runId = st.runId || await openRunId(); if (!runId) return null;
    const O = root.Orders; if (!O || typeof O.lineRecord !== "function") return null;
    const lines = {}; for (const r of rows) { const rec = safe(() => O.lineRecord(r), null); if (rec && rec[1]) lines[r.key] = rec[1]; }
    if (!Object.keys(lines).length) return null;
    let res = null, err = null;
    for (let t = 0; t < 4; t++) { try { res = await api({ op: "runLinesAdd", runId, lines, dev: dev(), by: who() }); err = null; break; } catch (e) { err = e; await new Promise(r => setTimeout(r, 1500 * (t + 1))); } }
    if (err) throw err;
    st.added = (st.added || 0) + ((res && res.added) || []).length;
    return res;
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
  root.CharmNestRunFollow = { state: () => Object.assign({}, st), now, patchRows, applyOthers, addRows, dev };
  if (root.document) { if (root.document.readyState === "loading") root.document.addEventListener("DOMContentLoaded", boot); else boot(); }
})(typeof self !== "undefined" ? self : this);
