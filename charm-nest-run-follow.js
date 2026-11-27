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
  const FIELDS = ["state", "poolIds", "reason", "hold", "wait", "heldAt", "_lp"];
  /* (RVSAME, Paul 19 Nov 2026: "Michelle's computer still does not agree with my computer!": measured from the two computers' own reports, Review's Open held the same 34 orders but one each: 4237444149 on Michelle's only (its "Just this order" charm,
     BASKETBALL (HUGGIE), was set on the computer that carries the run and never reached hers) and 4239217080 on Paul's only (its Etsy SKU names two drawings: a line read from the run's record here had no Etsy SKU to read, so the question was never asked here)).
     A person's decisions on a line are the run's, as its state is: taken from the cloud, and the line is read again with them */
  const DECIDE = ["skuOverride", "blockOverride", "releaseAnyway", "releaseHold", "materialOverride", "sizeOverride", "engravingOverride", "frontAt"];   // (CLOUDALL S2, Paul 13 Nov 2026: "Everything must be on the cloud nothing can be on one computer"): and the stamp of a line another computer changed
  const holdsRun = () => !!(root.B && root.B.run);

  async function openRunId() {
    const res = await api({ op: "runList", limit: 10 });
    const runs = ((res && res.runs) || []).filter(r => r && r.runId && !["abandoned", "complete", "archived"].includes(r.status));
    return runs.length ? runs[0].runId : null;
  }
  /* (ITEM 2, Paul 21 Nov 2026: no new orders in Orders / Nest / Library on the staff computers, though the cloud's run held 31 new lines "pulled"): once the cloud world drives a page,
     its lines come from the world only (applyToRows), and a line the cloud's run holds that the world has not put on the page (not read yet, its receipt not read) was shown nowhere.
     On a viewer (mode "on") such a line is added from the run's record, read here as before the world (Orders.rowFromRecord, interpretAll), marked runOnly: it follows the run's
     state, the world takes it over once it reads it, and it goes when the run lets it go. Mode off: never (the page goes as it went). */
  const viewer = () => safe(() => !!(root.CNViewer && typeof root.CNViewer.on === "function" && root.CNViewer.on()), false);
  function runOnly(lines, O, B) {
    if (typeof O.rowFromRecord !== "function" || !B.orders || !Array.isArray(B.orders.rows)) return;
    if (!(B.orders.byKey instanceof Map)) B.orders.byKey = new Map(B.orders.rows.map(r => [r.key, r]));
    let added = 0, moved = 0, gone = 0; const fresh = new Set();
    for (const [k, l] of Object.entries(lines)) {
      if (!l || l.state === "gone" || B.orders.byKey.has(k)) continue;
      const row = safe(() => O.rowFromRecord(k, l), null); if (!row) continue;
      row.runOnly = true; B.orders.rows.push(row); B.orders.byKey.set(k, row); added++; fresh.add(k.split("_")[0]);
    }
    for (const row of O.rows()) {
      if (!row || !row.runOnly || row.parentRow) continue;
      const l = lines[row.key];
      if (!l || l.state === "gone") { if (row.state !== "gone") { row.state = "gone"; row.reason = row.reason || "taken out of the run"; row.poolIds = []; row.wait = null; gone++; } continue; }
      let m = false;
      for (const f of FIELDS) { const v = l[f] === undefined ? (f === "poolIds" ? [] : null) : l[f]; if (JSON.stringify(row[f] == null ? (f === "poolIds" ? [] : null) : row[f]) !== JSON.stringify(v)) { row[f] = Array.isArray(v) ? v.slice() : v; m = true; } }
      if (m) moved++;
    }
    st.added += added; st.changed += moved; st.gone += gone; st.runOnly = { at: Date.now(), added, moved, gone };
    if (!added && !moved && !gone) return;
    if (added) safe(() => O.interpretAll());
    safe(() => root.Review && root.Review.syncOrderItems()); safe(() => O.render());
    if (fresh.size) safe(() => root.CharmNestEngraveSync && root.CharmNestEngraveSync.pull([...fresh], "keys"));
  }
  async function now(why) {
    if (holdsRun()) safe(() => { const G = root.Gate; if (G && typeof G.ensureOrder === "function") ["gold10k", "gold14k"].forEach(m => G.ensureOrder(m)); });   // (ORDERCLOUD: the carrier writes its draft order once, without the Options window having to be opened, so every computer numbers the drafts alike)
    if (holdsRun()) runCommands().catch(() => {});   // (SHEETSANYWHERE: the commands of the computers that follow, apart from the 15 s throttle of applyOthers)
    // (SHEETFRESH, Paul 27 Nov 2026: "the propagation of the updates throughout the system is intermittent": a name (r:<run>) that came while the run was being read was dropped, and the next read was the
    //  five-minute net; the read on its way may have left before that write: it is kept and read once more as soon as this read ends: one read, never a loop)
    if (st.busy) { st.again = why || "again"; return st; }
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
      // (CLOUDWORLD stage B, Paul 20 Nov 2026: "Only the cloud can decide"): the lines come whole from the cloud world, never from the run record and this computer's own reading
      const CW = root.CloudWorld; if (viewer() && CW && typeof CW.firstRead === "function") await CW.firstRead();   // (FASTOPEN: a computer that follows the cloud took every line from the run's record and read it itself, 0.6 s, a moment before the cloud's lines replaced them all)
      if (CW && typeof CW.drives === "function" && CW.drives()) {
        safe(() => CW.applyToRows("follow"));
        if (viewer()) runOnly(lines, O, B);
        st.retired = ((run.intakeRecovery && run.intakeRecovery.retire) || []).map(String);
        { const sk = Object.keys(run.sheets || {}).sort().join("|") + "#" + st.retired.slice().sort().join("|"), force = st.sheetSig !== undefined && st.sheetSig !== sk; st.sheetSig = sk;
          safe(() => { const f = root.Recall && root.Recall.follow; if (typeof f === "function") Promise.resolve(f(id, force ? { force: true } : undefined)).catch(() => {}); }); }
        st.last = { at: Date.now(), why, cloud: true }; return st;
      }
      let changed = 0, gone = 0, added = 0, needRead = false, crhMoved = false;
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
        { let decided = false; for (const f of DECIDE) { const v = l[f] === undefined ? null : l[f]; if (JSON.stringify(row[f] == null ? null : row[f]) !== JSON.stringify(v)) { if (v == null) delete row[f]; else row[f] = v && typeof v === "object" ? JSON.parse(JSON.stringify(v)) : v; decided = true; } }   // (RVSAME: the run's decisions on the line)
          const bought = l.snap && typeof l.snap.bought === "string" ? l.snap.bought : null;
          if (row.fromRecord && row.line && bought != null && !l.sku && row.line.sku !== bought) { row.line.sku = bought; row.line.etsySku = bought; decided = true; }   // (RVSAME: a line the run has not matched is read here under the SKU Etsy sold it under, as the run reads it)
          if (decided) { row.spec = null; needRead = true; moved = true; }
          if (Object.prototype.hasOwnProperty.call(l, "crh") && (row.crh === undefined || row.crh !== (l.crh || null))) { row.crh = l.crh || null; crhMoved = true; moved = true; } }   // (RVSAME: the reading the run uses for this line)
        if (moved) changed++;
      }
      const newOrders = new Set();
      if (typeof O.rowFromRecord === "function") for (const [k, l] of Object.entries(lines)) { if (B.orders.byKey.has(k)) continue; const row = safe(() => O.rowFromRecord(k, l), null); if (row) { if (viewer()) row.runOnly = true; B.orders.rows.push(row); B.orders.byKey.set(k, row); added++; newOrders.add(k.split("_")[0]); } }
      // (NEWLINES, Paul 13 Nov 2026: "What about future orders?"): a line that arrives here takes its engraving work from the cloud at once (it may have been written before the line reached this computer)
      // (FOLLOWSPEC, Paul 15 Nov 2026: "Cannot read properties of null (reading 'fontText')"): a line added here from the run's record is READ at once (its charm, its words, its font: Orders.interpretAll), as the lines another computer added to the run's own page are; it was left unread, and the engraving view of such an order could not open
      if (added || needRead || O.rows().some(r => r && !r.spec && !r.parentRow)) safe(() => O.interpretAll());
      if (crhMoved || added) safe(() => root.CustomRead && root.CustomRead.later(300));   // (RVSAME: the run's readings, taken now)
      if (newOrders.size) safe(() => root.CharmNestEngraveSync && root.CharmNestEngraveSync.pull([...newOrders], "keys"));
      st.changed += changed; st.gone += gone; st.added += added; st.last = { at: Date.now(), why, changed, gone, added };
      if (changed || gone || added) { safe(() => root.Review && root.Review.syncOrderItems()); safe(() => O.render()); safe(() => root.Session && root.Session.schedule()); }
      // (CLOUDFIRST, Paul 15 Nov 2026: "The library tab is the correct one. The nesting on the nest tab is incorrect"): and the run's sheets, as the cloud has them, on this computer's Nest cards (Recall.follow: read only, at most every ten minutes)
      // (ORDERCLOUD 2: a sheet added to the run or taken out of it on the carrying computer is shown here at once, not at the next ten-minute look)
      // (GONESHEET, Paul 15 Nov 2026: a sheet deleted on the carrying computer stayed on the computer that follows until the run's next checkpoint archived it): the sheets the run has let go of are not shown here
      st.retired = ((run.intakeRecovery && run.intakeRecovery.retire) || []).map(String);
      { const sk = Object.keys(run.sheets || {}).sort().join("|") + "#" + st.retired.slice().sort().join("|"), force = st.sheetSig !== undefined && st.sheetSig !== sk; st.sheetSig = sk;
        safe(() => { const f = root.Recall && root.Recall.follow; if (typeof f === "function") Promise.resolve(f(id, force ? { force: true } : undefined)).catch(() => {}); }); }
      return st;
    } catch (e) { st.last = { at: Date.now(), why, error: String(e && e.message || e) }; return st; }
    finally { st.busy = false; if (st.again) { const w = st.again; st.again = null; setTimeout(() => { now(w).catch(() => {}); }, 0); } }   // (SHEETFRESH: named while this read was out)
  }
  /* ── (CLOUDALL S2, Paul 13 Nov 2026: "Everything must be on the cloud nothing can be on one computer") ──
     patchRows(rows): this computer changed these order lines (a piece taken off, held, cancelled): they are written to the cloud run line by line (runLinesPatch), stamped.
     applyOthers(): on the computer whose page runs the run, a line another computer stamped after this page's own copy is taken into its row (state, pieces, hold, engraving), its
     pieces that left are dropped from this page's pool and engraving lists; its next whole save carries the stamp, so the server keeps what the other computer did. */
  const dev = () => { let v = safe(() => root.localStorage.getItem("cn.dev"), ""); if (!/^d[0-9a-z]{4,14}$/.test(v || "")) { v = "d" + Math.random().toString(36).slice(2, 12); safe(() => root.localStorage.setItem("cn.dev", v)); } return v; };
  const who = () => String(safe(() => root.B && root.B.employee, "") || "").trim();
  let lastApply = 0, applyT = 0;
  const CLOUD_ROW_FIELDS = ["holdSeen", "etsyClosed", "frontAt", "releasing", "skuOverride", "blockOverride", "releaseHold", "releaseAnyway", "changePending", "repoolChanged", "arrivedAt", "activityAt", "materialOverride", "sizeOverride", "engravingOverride", "groupSize", "material", "poolProblems", "designHold", "changedAt", "changedWhy"];
  /** (OTHERSLINES) The run save's answer named when another computer last touched the run's lines: a time newer than what this page has taken is read now (applyOthers keeps its own 15 s pace). */
  function othersSaid(at) { if (!holdsRun() || !(+at > 0)) return; if (st.appliedAt && +at <= +st.appliedAt) return; applyOthers("save").catch(() => {}); }
  async function applyOthers(why) {
    const run = root.B && root.B.run; if (!run || !run.runId) return st;
    const t = Date.now(); if (t - lastApply < 15000) { if (!applyT) applyT = setTimeout(() => { applyT = 0; applyOthers("later"); }, 15000 - (t - lastApply)); return st; }
    if (st.busy) { st.again = why || "again"; return st; } st.busy = true; lastApply = t;   // (SHEETFRESH: kept, read once this read ends)
    try {
      const res = await api({ op: "runGet", runId: run.runId }); const cr = res && (res.run || res); if (!cr || !cr.lines) return st;
      if (!cr.lineEditAt || (st.appliedAt && +cr.lineEditAt <= +st.appliedAt)) return st;
      const O = root.Orders, B = root.B, me = dev(), takenOff = [], released = []; let n = 0;
      for (const row of O.rows()) {
        const l = cr.lines[row.key], lp = l && l._lp; if (!lp || lp.dev === me) continue;
        if (row._lp && +row._lp.at >= +lp.at) continue;
        // (RELEASEHOLD, Paul 20 Nov 2026: a line the cloud released from hold (op releaseAsk, pressed on any computer) is made up here, by this run's intake, once)
        if (lp.dev === "cloud" && +l.releasedAt > 0 && !l.hold && !(+row._releasedTaken >= +l.releasedAt)) { row._releasedTaken = +l.releasedAt; released.push(row); }
        const was = (row.poolIds || []).slice();
        for (const f of ["state", "poolIds", "reason", "hold", "wait", "heldAt"]) { const v = l[f] === undefined ? (f === "poolIds" ? [] : null) : l[f]; row[f] = Array.isArray(v) ? v.slice() : v; }
        // (CLOUDDRIVE P3, cloud plan v2 §2.2; DORMANT: only the cloud stamps a line with dev "cloud", and it writes no line while the drive is off): a line the cloud changed last
        //  brings every field of it the row keeps (the cloud's line fields, _charmNestWorld.js RUN_FIELDS); what the row reads from its spec is read again by interpretAll
        if (lp.dev === "cloud") for (const f of CLOUD_ROW_FIELDS) if (l[f] !== undefined) row[f] = l[f] && typeof l[f] === "object" ? JSON.parse(JSON.stringify(l[f])) : l[f];
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
      if (released.length) repoolReleased(released);
      return st;
    } catch (e) { st.last = { at: Date.now(), why, error: String((e && e.message) || e) }; return st; }
    finally { st.busy = false; if (st.again) { const w = st.again; st.again = null; setTimeout(() => { (holdsRun() ? applyOthers(w) : now(w)).catch(() => {}); }, 0); } }   // (SHEETFRESH: named while this read was out: read once more; applyOthers keeps its own 15 s pace)
  }
  /* RELEASEHOLD (Paul, 20 Nov 2026: "a Release Hold press on ANY computer releases the hold in the cloud at once ... placing the pieces is the intake's normal work; no more waiting on the computer
     that carries the run"): the release itself is the cloud's (op releaseAsk). On the computer that carries the run, its lines come back here as the cloud left them ("pulled", no hold, the front of
     the queue) and are made up by the intake now, the way a released line always was (Review.repool: its home sheet first, else the intake's sheet), one after the other. Never a line this page is
     releasing itself, nor while the run is before its pool step (that step makes every "pulled" line up). */
  let relChain = Promise.resolve();
  function repoolReleased(rows) {
    const run = root.B && root.B.run, O = root.CharmNestOrders, R = root.Review;
    if (!run || !R || typeof R.repool !== "function") return;
    if (O && typeof O.stepIndex === "function" && !(O.stepIndex(run.step) >= O.stepIndex("pool"))) return;
    for (const row of rows) relChain = relChain.then(async () => {
      const rid = String((row.order && row.order.receiptId) || "");
      if (safe(() => !!(root.OrderHold && root.OrderHold.releaseStatus && root.OrderHold.releaseStatus(rid).running), false)) return;
      if (!root.B || root.B.run !== run || row.state === "gone" || row.hold || !["pulled", "pooled"].includes(row.state)) return;
      try { await R.repool(row); safe(() => root.agent && root.agent({ run: run.runId, pool: true }, "info", `${rid}: released from hold in the cloud; its pieces are made up and placed by this run`)); }
      catch (e) { safe(() => console.warn("[run-follow] a released line was not made up:", row.key, e && e.message)); }
    }).catch(() => {});
  }
  /* (SHEETSANYWHERE, Paul 15 Nov 2026: "I need that full capability ... drag the sheets from one sheet to another and amalgamate sheets as needed"): the computer that CARRIES the run does what a following computer asked for in the Sheets card
     (runCmdPut on the cloud): one at a time, in the order asked, through the Gate, then answers (runCmdDone) with the result line. Looked at when the feed names the run, at most every 3 s; a command older than 10 minutes is answered "too old". */
  let cmdAt = 0, cmdT = 0, cmdBusy = false; const cmdSeen = new Set();
  const LONG_CMDS = new Set(["release", "skuonce"]);   // (ANYWHERE, Paul 19 Nov 2026: what a person asked of the run is kept in the cloud until a computer that carries it does it)
  /* MOVESHOW, Paul 15 Nov 2026: "no animation no feedback no progress nothing": while this computer does a request it says what step it is at (runCmdStep), so the computer that asked can show it.
     The first call also marks the request as running (it can no longer be cancelled). At most one write every 2.5 s (the Firebase budget): the newest step waits for the next slot. */
  function stepper(c) {
    const run = root.B && root.B.run, runId = run && run.runId; let lastAt = 0, timer = 0, pend = null, sentOnce = false;
    const send = async s => { lastAt = Date.now(); sentOnce = true; try { await api({ op: "runCmdStep", runId, id: c.id, dev: dev(), text: String((s && s.text) || "").slice(0, 160), ...(s && Number.isFinite(+s.pct) ? { pct: +s.pct } : {}) }); } catch (_) { /* progress is only shown */ } };
    const fn = s => {
      if (!runId || !s || !s.text) return; pend = s;
      const wait = 2500 - (Date.now() - lastAt);
      if (!sentOnce || wait <= 0) { if (timer) { clearTimeout(timer); timer = 0; } const x = pend; pend = null; send(x); }
      else if (!timer) timer = setTimeout(() => { timer = 0; const x = pend; pend = null; if (x) send(x); }, wait);
    };
    fn.stop = () => { if (timer) { clearTimeout(timer); timer = 0; } pend = null; };
    return fn;
  }
  async function execCmd(c) {
    const G = root.Gate, a = c.args || {}; if (!G) return { ok: false, text: "The sheets are not ready on the computer that carries the run." };
    if (c.type === "reorder") {
      const rows = safe(() => G.sheetsOf(a.metal), []), have = rows.map(r => r.id), want = [...new Set((a.ids || []).filter(id => have.includes(id)))];
      have.forEach((id, i) => { if (!want.includes(id)) want.splice(Math.min(i, want.length), 0, id); });
      const r = G.reorder(a.metal, want); return r && r.ok ? { ok: true, text: r.changed === false ? "The sheets were already in that order" : "Sheet order changed: " + (r.names || []).join(", ") } : { ok: false, text: (r && r.why) || "The order was not changed" };
    }
    let r = null;
    if (c.type === "repicture") r = root.Recall && typeof root.Recall.repicture === "function" ? await root.Recall.repicture(a.sheetId) : { ok: false, why: "not ready" };   // (REPICTURE: a sheet's saved picture drawn again, nothing else of it written)
    else if (c.type === "consolidate" || c.type === "empty") {   // (MOVESHOW: what the merge says it is doing is passed on, so the computer that asked can show it)
      const step = stepper(c), iv = setInterval(() => { const b = safe(() => G.mergeBusy && G.mergeBusy(a.metal), null); if (b && b.text) step({ text: b.text }); }, 1000); step({ text: "Starting…" });
      try { r = c.type === "empty" ? await G.emptySheet(a.sheetId) : await G.consolidate(a.sourceId, a.targetId); } finally { clearInterval(iv); step.stop(); }
    }
    else if (c.type === "seat") { const step = stepper(c); step({ text: "Reading the sheet…" }); try { r = typeof G.seatOn === "function" ? await G.seatOn(a.sheetId, Array.isArray(a.partialIds) && a.partialIds.length ? a.partialIds : a.partialId, { onStep: step, finish: true }) : { ok: false, why: "not ready" }; } finally { step.stop(); } }   // (ONEGRID: a nested sheet's pieces onto a physical sheet; MOVESHOW: the physical sheets in the order chosen, and it stays until the nest has ended so the answer says where the pieces lie)
    else if (c.type === "delete") r = await G.deleteEmpty(a.sheetId);
    // (UNKEEP, Paul 16 Nov 2026: "Remove all of those shipped orders from the sheets"): orders Etsy has closed that were put back on their sheets by hand (the run's keepShipped list, 13 Nov) are let go:
    //  they leave that list, the run is saved, and the page's own check of the orders against Etsy (Orders.revalidate) takes their pieces off, as it does for any order that ships. A sheet the
    //  laser has cut, one with a recorded cut, and one whose set went to the station keep their pieces, as always (takeOffGone's own rule)
    else if (c.type === "unkeep") {
      const run = safe(() => (typeof B !== "undefined" ? B.run : root.B && root.B.run), null), Or = safe(() => (typeof Orders !== "undefined" ? Orders : root.Orders), null), RC = safe(() => (typeof RunCtl !== "undefined" ? RunCtl : root.RunCtl), null);
      const want = new Set((a.ids || a.orders || []).map(x => String(x).replace(/\D/g, "")).filter(Boolean));
      if (!run || !Or || typeof Or.revalidate !== "function" || !RC) r = { ok: false, why: "not ready" };
      else if (!want.size) r = { ok: false, why: "No order was named." };
      else {
        const step = stepper(c), was = (run.keepShipped || []).map(String), let_go = was.filter(x => want.has(x));
        {
          // (UNKEEP 2: the first send took the orders off the list and then could not ask Etsy, "the Design Station frame is not open": the station's frame is opened first, as an intake does (STATIONOPEN),
          //  and a send for orders already off the list still runs the check, so it can be sent again)
          run.keepShipped = was.filter(x => !want.has(x));
          step({ text: `${let_go.length ? let_go.length + " order(s) no longer kept: " : ""}checking the orders against Etsy…` });
          try {
            if (let_go.length) await RC.save(run);
            const DL = safe(() => (typeof DesignLink !== "undefined" ? DesignLink : root.DesignLink), null);
            if (DL && typeof DL.ensure === "function") { try { await DL.ensure(); } catch (_) { /* said below if the check then fails */ } }
            const v = await Or.revalidate(run, "shipped orders let go");
            const gone = (v && v.gone || []).map(String).filter(x => want.has(x));
            r = { ok: true, text: `${let_go.length} shipped order(s) let go; ${gone.length} of them were shipped at our stations or cancelled: their pieces were taken off every sheet that is still open` };
          } catch (e) { r = { ok: false, why: `The orders left the keep list, but the check against Etsy did not finish (${String((e && e.message) || e).slice(0, 160)}); it runs again at the next check of the orders.` }; }
        }
      }
    }
    // (STRANDED, Paul 16 Nov 2026): pieces the run counts as pooled that stand on no sheet go back to the intake and are placed (charm-nest-bridge.js placeStranded)
    else if (c.type === "placewaiting") {
      const f = root.CNPlaceStranded;
      if (typeof f !== "function") r = { ok: false, why: "not ready" };
      else { const step = stepper(c); step({ text: "Placing the pieces that stand on no sheet…" });
        try { const v = await f(); r = { ok: true, text: v.pieces ? `${v.placed} of ${v.pieces} stranded piece(s) (${v.lines} order line(s)) went onto a sheet` : "No piece stands outside a sheet." }; }
        catch (e) { r = { ok: false, why: String((e && e.message) || e).slice(0, 200) }; } }
    }
    // (REMOTESEND, Paul 16 Nov 2026: "The charm was removed from the sheet but the updated charm did not appear"): the designs a following computer sent for an order are put on the sheets here (charm-nest-bridge.js CustomSheet.applyRemote)
    else if (c.type === "customsend") {
      const CS = safe(() => (typeof CustomSheet !== "undefined" ? CustomSheet : root.CustomSheet), null);
      if (!CS || typeof CS.applyRemote !== "function" || !a.ck) r = { ok: false, why: "not ready" };
      else { const step = stepper(c); step({ text: "Reading the designs that were sent…" }); try { r = await CS.applyRemote(a.ck, { onStep: step }); } catch (e) { r = { ok: false, why: String((e && e.message) || e).slice(0, 300) }; } finally { step.stop(); } }
    }
    // (SETREBUILD, Paul 16 Nov 2026: "Remove that sheet from the set of sheets it was incorrectly added to"): the set's own repair ("Rebuild this set correctly", charm-nest-set-repair.js: the sheets that do not belong leave, nothing is deleted, no sheet is nested again), asked from another computer
    else if (c.type === "setrebuild") {
      const SRp = root.SetRepair || root.CharmNestSetRepair, run = safe(() => (typeof B !== "undefined" ? B.run : root.B && root.B.run), null);
      if (!SRp || typeof SRp.rebuild !== "function" || !a.setId || !run) r = { ok: false, why: "not ready" };
      else { const step = stepper(c); step({ text: "Reading the set…" });
        try { const v = await SRp.rebuild({ setId: a.setId, runId: run.runId }, { skipConfirm: true, onState: (_, t) => step({ text: String(t || "Rebuilding the set…") }) });
          r = v && v.ok ? { ok: true, text: String((v.words) || (v.plan && v.plan.words && v.plan.words.summary) || "The set was rebuilt") } : { ok: false, why: String((v && ((v.refused && v.refused.reason) || v.error || (v.still && v.still[0]))) || "The set was not rebuilt").slice(0, 380) }; }
        catch (e) { r = { ok: false, why: String((e && e.message) || e).slice(0, 300) }; } finally { step.stop(); } }
    }
    // (PAIRNOW: lines that wait for the sheet still filling are sent on now: charm-nest-bridge.js CNPairNow)
    else if (c.type === "pairnow") {
      const f = root.CNPairNow;
      if (typeof f !== "function" || !(a.ids || []).length) r = { ok: false, why: "not ready" };
      else { const step = stepper(c); step({ text: "Placing the waiting piece…" });
        try { const v = await f(a.ids); r = v.placed ? { ok: true, text: `${v.placed} piece(s) placed on ${v.names.join(" and ")}` } : { ok: false, why: v.lines ? `Not placed: ${(v.waiting && v.waiting[0]) || "the intake did not take it"}` : "That line does not wait for a sheet to fill." }; }
        catch (e) { r = { ok: false, why: String((e && e.message) || e).slice(0, 300) }; } finally { step.stop(); } }
    }
    // (LEAVESET, Paul 16 Nov 2026: "Remove that sheet from the set of sheets it was incorrectly added to"): one sheet taken out of the set it is in, as a person's own take-out does (Gate.leaveSet: nothing deleted, nothing nested again); it may join the NEXT set by the usual rules
    else if (c.type === "leaveset") {
      const pages = safe(() => (typeof allSheets === "function" ? allSheets() : root.allSheets()), []) || [], pg = pages.find(x => x && x.sheetId === a.sheetId);
      if (!G || typeof G.leaveSet !== "function" || !pg) r = { ok: false, why: pg ? "not ready" : "That sheet is not on the computer that carries the run." };
      else { const step = stepper(c), was = pg.fileBase || pg.sheetId; step({ text: "Taking the sheet out of its set…" });
        try { await G.leaveSet(a.sheetId); if (pg.setId && !pg.draft) r = { ok: false, why: "The sheet is still in its set." }; else { delete pg.leftSet; r = { ok: true, text: `${was} was taken out of its set; it is ${pg.fileBase || "a draft"} now and waits for the next set` }; } }
        catch (e) { r = { ok: false, why: String((e && e.message) || e).slice(0, 300) }; } finally { step.stop(); } }
    }
    // (SENDHOME: a piece placed on another sheet than the one it was taken off goes back on its own sheet: charm-nest-bridge.js CNSendHome)
    else if (c.type === "sendhome") {
      const f = root.CNSendHome;
      if (typeof f !== "function" || !(a.ids || []).length) r = { ok: false, why: "not ready" };
      else { const step = stepper(c); step({ text: "Putting the piece back on its own sheet…" });
        try { const v = await f(a.ids); r = v.moved ? { ok: true, text: `${v.moved} order line(s) back on ${v.names.join(" and ")}${v.why.length ? " · " + v.why.join("; ") : ""}` } : { ok: false, why: (v.why.join("; ") || "Nothing was moved.").slice(0, 380) }; }
        catch (e) { r = { ok: false, why: String((e && e.message) || e).slice(0, 300) }; } finally { step.stop(); } }
    }
    // (REMAKELINE: order lines made up again from what they read as now, in place: charm-nest-bridge.js CNRemakeLines)
    else if (c.type === "remakeline") {
      const f = root.CNRemakeLines;
      if (typeof f !== "function" || !(a.ids || []).length) r = { ok: false, why: "not ready" };
      else { const step = stepper(c); step({ text: "Making the order's pieces again…" });
        try { const v = await f(a.ids); r = v.made ? { ok: true, text: `${v.made} order line(s) made again: ${v.pieces} piece(s) on ${[...new Set(v.names)].join(" and ")}${v.why.length ? " · " + v.why.join("; ") : ""}`.slice(0, 380) } : { ok: false, why: (v.why.join("; ") || "Nothing was made.").slice(0, 380) }; }
        catch (e) { r = { ok: false, why: String((e && e.message) || e).slice(0, 300) }; } finally { step.stop(); } }
    }
    // (KEEPREMOTE, Paul 17 Nov 2026: a design saved to the repository on a following computer: the repository and the saved links are read again HERE, now, and the open orders of these SKUs are made again in place: the listing's lines the link moved (Review.relinkNow) and the pieces that hold an older drawing of the SKU (Review.redrawStale))
    else if (c.type === "keepsync") {
      const Rv = root.Review, Ms = safe(() => (typeof Master !== "undefined" ? Master : root.Master), null), Or = safe(() => (typeof Orders !== "undefined" ? Orders : root.Orders), null);
      if (!Rv || typeof Rv.redrawStale !== "function" || !(a.ids || []).length) r = { ok: false, why: "not ready" };
      else { const step = stepper(c); step({ text: "Reading the repository again…" });
        const snap = typeof Rv.redrawStale.snap === "function" ? safe(() => Rv.redrawStale.snap(a.ids), null) : null;   // (TRUEREDRAW, Paul 17 Nov 2026: what the open pieces hold BEFORE the library is read: the lines the sweep Master.load queues are made before redrawStale is asked, and are counted against this)
        try {
          if (Ms && typeof Ms.load === "function") { try { await Ms.load({ quiet: true, ttl: 1, fresh: true }); } catch (_) { /* the index it holds */ }   /* (REPOEDIT: what changed is read, not the whole index) */ }
          if (Or && typeof Or.loadMaps === "function") { try { await Or.loadMaps(true); } catch (_) { /* the links it holds */ } }
          try { if (Or && Or.interpretAll) Or.interpretAll(); } catch (_) { /* read at the next look */ }
          step({ text: "Making the other orders again…" });
          let moved = 0, movedKeys = new Set(); try { const v = typeof Rv.relinkNow === "function" ? await Rv.relinkNow() : null; moved = ((v && v.swapped) || []).length; movedKeys = new Set(((v && v.swapped) || []).map(x => x && x.row && x.row.key).filter(Boolean)); } catch (_) { /* the next sweep */ }
          const v2 = await Rv.redrawStale(a.ids, { snap, not: movedKeys });
          try { if (root.CNStrandedWatch) root.CNStrandedWatch("a design saved on another computer"); } catch (_) { /* its own look */ }
          /* TRUEREDRAW (Paul, 17 Nov 2026): the answer says what was made ("made again: N line(s)", N counted from the pieces), and a line that still waits for its sheet (one being arranged or saved: the sweep tries again by itself) is never
             a finished green line: it is answered as not done yet, in words that say the line follows as soon as its sheet is free (the computer that asked shows that as a plain note, not as done) */
          r = v2 && v2.ok !== false ? (v2.waiting ? { ok: false, why: `${moved ? moved + " order line(s) moved to the new design; " : ""}${String(v2.text || "")}`.slice(0, 380) } : { ok: true, text: `${moved ? moved + " order line(s) moved to the new design; " : ""}${String(v2.text || "done")}`.slice(0, 380) }) : { ok: false, why: String((v2 && (v2.why || v2.text)) || "not done").slice(0, 300) };
        } catch (e) { r = { ok: false, why: String((e && e.message) || e).slice(0, 300) }; } finally { step.stop(); } }
    }
    else if (c.type === "remake") r = root.Review && typeof root.Review.redrawStale === "function" ? await root.Review.redrawStale(a.ids || []) : { ok: false, why: "not ready" };   // (REMAKESTALE: open pieces that hold an older drawing of these SKUs are made up again from the library's drawing)
    // (REMOTENEST, Paul 15 Nov 2026: "you re-nest it and then check that everything is OK everywhere"): a draft sheet of the run is nested again here, exactly as its Re-nest button does (never a sheet that was cut or is in a committed set: startNest's own rules stand), and the answer waits for the nest and its save
    else if (c.type === "renest") {
      const all = safe(() => (typeof allSheets === "function" ? allSheets() : root.allSheets()), []) || [], nest = safe(() => (typeof startNest === "function" ? startNest : root.startNest), null), pg = all.find(p => p && p.sheetId === a.sheetId);   // (the page's own functions, by their names: they are not properties of the window)
      if (!pg) r = { ok: false, why: "That sheet is not on the computer that carries the run." };
      else if (pg.roseCutAt || +pg.laserDoneAt > 0) r = { ok: false, why: "That sheet was already cut: it is not nested again." };
      else if (["nesting", "finishing", "queued"].includes(pg.status)) r = { ok: false, why: "That sheet is being nested right now." };
      else if (typeof nest !== "function") r = { ok: false, why: "not ready" };
      else {
        const step = stepper(c), name = pg.fileBase || pg.sheetId, t0 = Date.now(); step({ text: "Starting the nest…" });
        try {
          if (pg.recalled && !(pg.charms || []).length && root.Recall && root.Recall.rebuild) await root.Recall.rebuild(pg);
          pg._byHand = true; nest(pg);
          await new Promise(res => setTimeout(res, 1500));
          while (Date.now() - t0 < 9 * 60000 && (["nesting", "finishing", "queued"].includes(pg.status) || (pg.persisted && !pg.persistedDone && !pg.problem))) { step({ text: pg.status === "nesting" ? `Nesting ${(pg.placements || []).length} of ${(pg.charms || []).length}…` : "Saving the sheet…" }); await new Promise(res => setTimeout(res, 1000)); }
          const ok = ["complete", "partial"].includes(pg.status) && !pg.problem;
          r = ok ? { ok: true, text: `${name} was nested again: ${(pg.placements || []).length} of ${(pg.charms || []).length} placed${pg.verification ? (pg.verification.ok ? ", verified" : ", verification flagged") : ""}` } : { ok: false, why: pg.problem ? String(pg.problem.message || pg.problem) : `the nest did not finish (${pg.status})` };
        } finally { step.stop(); }
      }
    }
    // (FOLLOWRELEASE, Paul 18 Nov 2026: order 4235861617 released on Michelle's computer was on hold again minutes later: a hold released on a computer that only follows the run lifted nothing in the cloud, the run's line stayed "held", and the next
    //  look at the order held it again): the release is done HERE, by the page that carries the run, exactly as its own Release Hold does it (charm-nest-order-release.js OrderHold.release: the questions set aside, the pieces made up and put on their sheet, the
    //  timeline step), in the name of the person who pressed it
    else if (c.type === "release") {
      const OH = root.OrderHold, rid = String((a.ids || [])[0] || "").replace(/\D/g, "");
      if (!OH || typeof OH.release !== "function" || !rid) r = { ok: false, why: "not ready" };
      else {
        const step = stepper(c); step({ text: `Releasing order ${rid} from hold…` });
        try {
          const t0 = Date.now(), el = () => ` · ${Math.round((Date.now() - t0) / 1000)} s`;   // (RELWAIT: what it is doing and for how long, so the computer that asked can say it)
          const v = await OH.release(rid, { name: String(c.by || "").trim() || undefined, onStep: s => { if (!s) return; if (s.type === "queued") step({ text: `Its place at the front is saved${el()}` }); else if (s.type === "target" && s.label) step({ text: `Placing on ${s.label}…${el()}` }); else if (s.type === "flight") step({ text: `Moving its pieces${el()}` }); else if (s.type === "placed") step({ text: `Placed on ${s.label || "the sheet"}${el()}` }); else if (s.type === "qr") step({ text: `Making the sheet's QR label${el()}` }); } });
          const where = ((v && v.sheets) || []).map(x => x && x.label).filter(Boolean).join(", ");
          r = v && v.released ? { ok: true, text: `Order ${rid} released from hold${where ? " and placed on " + where : ", at the front of the queue"}${v.error ? " · " + String(v.error).slice(0, 200) : ""}`.slice(0, 380) } : { ok: false, why: String((v && (v.error || (v.steps && v.steps[v.steps.length - 1] && v.steps[v.steps.length - 1].message))) || "The order was not released").slice(0, 380) };
        } catch (e) { r = { ok: false, why: String((e && e.message) || e).slice(0, 300) }; } finally { step.stop(); }
      }
    }
    // (ANYWHERE, Paul 19 Nov 2026: "Just this order" from a computer that only follows the run): the line's own charm for this one order, set and its piece swapped on its sheet exactly as the carrying computer's own window does it, in the name of the person who asked
    else if (c.type === "skuonce") {
      const Rv = root.Review, Or = root.Orders, key = String((a.ids || [])[0] || ""), sku = String(a.sku || "").trim().toUpperCase();
      const row = Or && typeof Or.rows === "function" ? safe(() => Or.rows().find(x => x && x.key === key), null) : null;
      if (!Rv || typeof Rv.editSkuOnce !== "function" || !key || !sku) r = { ok: false, why: "not ready" };
      else if (!row) r = { ok: false, why: `The piece ${key} is not in the run on the computer that carries it` };
      else {
        const step = stepper(c); step({ text: `${sku} for this order only…` });
        try { const v = await Rv.editSkuOnce(row, sku, { by: String(c.by || "").trim() }); r = v && v.ok ? { ok: true, text: `${sku} for order ${row.order && row.order.receiptId} only` } : { ok: false, why: String((v && v.why) || "not changed").slice(0, 300) }; }
        catch (e) { r = { ok: false, why: String((e && e.message) || e).slice(0, 300) }; } finally { step.stop(); }
      }
    }
    else return { ok: false, text: "Unknown command" };
    return r && r.ok !== false ? { ok: true, text: r.text || "Done", ...(r.freed && r.freed.stockId ? { freed: r.freed.stockId } : {}) } : { ok: false, text: (r && (r.why || r.text)) || "Not done" };
  }
  async function runCommands() {
    if (cmdBusy) return; const t = Date.now();
    if (t - cmdAt < 3000) { if (!cmdT) cmdT = setTimeout(() => { cmdT = 0; runCommands().catch(() => {}); }, 3000 - (t - cmdAt)); return; }
    cmdAt = t; cmdBusy = true;
    try {
      for (let guard = 0; guard < 12; guard++) {
        const run = root.B && root.B.run; if (!run || !run.runId) break;
        const res = await api({ op: "runCmdList", runId: run.runId }), me = dev();
        // (CLOUDWORLD stage E, Paul 20 Nov 2026: nothing waits or hangs unseen): a request a computer began and never finished (it closed or stopped answering: no step for 10 minutes) is answered as failed, with why, for every computer to see; never run twice
        for (const c of ((res && res.cmds) || []).filter(c => c && c.id && c.state === "running" && !cmdSeen.has(c.id) && Date.now() - (+c.stepAt || +c.at || 0) > 600000)) { cmdSeen.add(c.id); safe(() => api({ op: "runCmdDone", runId: run.runId, id: c.id, ok: false, text: `Stopped before it finished: the computer doing it (${c.stepBy || "another computer"}) stopped answering at "${String(c.step || "").slice(0, 80)}". Ask again.`, dev: me })); }
        // (CLOUDDRIVE P9, build step 13, DORMANT while the drive is off: a request whose lane "cmd:<type>" is the cloud's, while the cloud carries the run, is the cloud's to do)
        if (root.CNViewer && typeof root.CNViewer.holdNew === "function" && root.CNViewer.holdNew()) break;   // (build step 14: a viewer does no request; a page handing its run to the cloud starts none: the cloud does them)
        const cloudLane = c => !!(root.CNViewer && typeof root.CNViewer.lane === "function" && root.CNViewer.lane("cmd:" + c.type, run.runId));
        const next = ((res && res.cmds) || []).filter(c => c && c.id && c.state === "waiting" && c.dev !== me && !cmdSeen.has(c.id) && !cloudLane(c)).sort((x, y) => (+x.at || 0) - (+y.at || 0))[0];
        if (!next) break;
        cmdSeen.add(next.id); let out;
        // (and whenever the drive is not off, a request is claimed before it is done: one carrier does it, whoever else looks, P9)
        if (root.CNViewer && root.CNViewer.known() && root.CNViewer.mode() !== "off") { let cl = null; try { cl = await api({ op: "runCmdClaim", runId: run.runId, id: next.id, dev: me, by: who() }); } catch (_) { cl = null; } if (!cl || !cl.claimed) continue; }
        if (Date.now() - (+next.at || 0) > (LONG_CMDS.has(next.type) ? 6 * 3600000 : 600000)) out = { ok: false, text: `Not done: asked ${Math.round((Date.now() - (+next.at || 0)) / 60000)} min ago by ${next.by || "another computer"}, while no computer carried the run. Ask again if it is still wanted.` };   // (stage E: said, never a bare "too old")   // (ANYWHERE: a person's release or one-order charm waits for this computer for hours, as it is asked of the cloud, not of a computer)
        else { try { out = await execCmd(next); } catch (e) { out = { ok: false, text: String((e && e.message) || e) }; } }
        for (let k = 0; k < 3; k++) { try { await api({ op: "runCmdDone", runId: run.runId, id: next.id, ok: !!out.ok, text: String(out.text || "").slice(0, 400), ...(out.freed ? { freed: out.freed } : {}), dev: me }); break; } catch (_) { await new Promise(r => setTimeout(r, 1500)); } }
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
  /** (S1, Paul 21 Nov 2026: Review answers made on a staff computer in cloud mode "on" stayed in that page's memory): a person's answer (Skip piece, Use this size, a material) is
   *  written to the cloud run's lines as a person's press: runLinesPatch with decision: true, each line's own fields (state, reason, hold, heldAt, materialOverride, sizeOverride) and
   *  `decided`, its step on the order's timeline (the cloud writes it, once). patches: { lineKey: fields }. → the cloud's answer { ok, applied, lines: { key: what the line now says }, at }.
   *  A refusal throws at once with the cloud's words; a passing failure is tried four times. Only on a page that holds no run (the run's own page saves its lines with the run). */
  async function decideRows(patches) {
    const keys = Object.keys(patches || {}).filter(k => k && patches[k] && typeof patches[k] === "object").slice(0, 300); if (!keys.length) return null;
    if (holdsRun()) throw new Error("this computer carries the run: its answer is saved with the run");
    const runId = st.runId || await openRunId(); if (!runId) throw new Error("no run is open in the cloud");
    const lines = {}; for (const k of keys) lines[k] = patches[k];
    let res = null, err = null;
    for (let t = 0; t < 4; t++) {
      try { res = await api({ op: "runLinesPatch", runId, lines, decision: true, dev: dev(), by: who() }); err = null; break; }
      catch (e) { err = e; if (e && e.status && e.status < 500 && e.status !== 408 && e.status !== 429) break; await new Promise(r => setTimeout(r, 1500 * (t + 1))); }
    }
    if (err) throw err;
    if (res && res.error) throw new Error(res.error);
    const O = root.Orders;
    if (res && res.at && O) for (const k of res.applied || []) { const r = safe(() => O.rows().find(x => x && x.key === k), null); if (r) r._lp = { at: res.at, dev: dev(), by: who() }; }
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
  root.CharmNestRunFollow = { state: () => Object.assign({}, st), now, patchRows, decideRows, applyOthers, othersSaid, addRows, dev };
  if (root.document) { if (root.document.readyState === "loading") root.document.addEventListener("DOMContentLoaded", boot); else boot(); }
})(typeof self !== "undefined" ? self : this);
