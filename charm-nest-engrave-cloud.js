/* charm-nest-engrave-cloud.js — THE ENGRAVE LANE ON A PAGE (cloud plan v2, build step 11, P9: one lane, one place). DORMANT.

   While the cloud carries this page's run (Charm_Nest_Run_Carrier says "cloud") AND "engrave" is one of the drive's lanes (Charm_Nest_Open/drive.cloudSteps), an approved engraving's back
   file is the cloud's to build, check, upload and record (netlify/functions/_charmEngraveFiles.js). This page then sends the person's Approve as ONE press, engraveApprove, and writes no
   back file of its own (charm-nest-bridge.js: saveBacks, writeBacks, resumeBacks and retryBacksLater ask carries() and stand down; approveOnce asks lane() and sends the press). That can
   never be true while the drive is off: the carrier is never "cloud" then, carries() is false, and every page approves and writes exactly as today.

     noteCarrier(runId, answer)  the run's carrier, as any runCarrier answer says it (RunCtl's heartbeat calls this: no read of its own)
     carries(runId?)             sync: does the cloud write this run's engraving files now? (the last answers seen; never true while the drive is off)
     lane(job)                   async, at a press: the same, read again when the copy is older than a minute (one read of the carrier document; the drive's switch only when the
                                 cloud carries)
     approve(job, by)            the press: the words, the place and the fields the person approved, the line's work sent first (CharmNestEngraveSync.flush), then engraveApprove.
                                 → { ok, at, taskId, jobKey } | { ok: false, local (the cloud does not take it: this page approves as today), message }
                                 (ITEM 9, Paul 21 Nov 2026) an approved face EDITED from the Library sheet preview (job.editingBack) is sent as an edit of its line's job (`edit`: the piece,
                                 its sheet, the approval it replaces): the cloud reopens that job and writes that one piece's file; the seal comes down here when the task says it is done
                                 (Engrave.cloudEditSaved), its reason is said when it is not
     watching()                  the approvals this page sent that the cloud is still writing (the card says "Saving…"); when the cloud's record says the face is written the
                                 waiting mark comes off; when its task ended without saving, the reason is put on the face and said
   Nothing here builds, checks or uploads a file. COST: nothing while the cloud does not carry the run; a press reads the carrier once a minute at most; while the cloud writes a file
   this page sent, one small read every 4 s (engraveFilesGet), until it is written. */
(function (root) {
  "use strict";
  const FRESH_MS = 60000, POLL_MS = 4000, WATCH_MAX_MS = 30 * 60000;
  const FIELD_KEYS = ["lines", "lineInput", "lineMode", "lineGap", "fontKey", "fontName", "fontAsked", "wantSize", "nudged", "turnUp", "upSystem", "orientVersion", "decision", "decidedAt", "decidedBy"];
  const st = { runId: null, carrierCloud: false, carrierAt: 0, drive: null, driveAt: 0, driveAsk: null, watch: new Map(), timer: 0, polls: 0, sent: 0, last: null };
  const safe = (f, d) => { try { const v = f(); return v === undefined ? d : v; } catch (_) { return d; } };
  const plain = v => { try { return v == null ? v : JSON.parse(JSON.stringify(v)); } catch (_) { return undefined; } };
  const wait = ms => new Promise(r => setTimeout(r, ms));
  const api = (body, opts) => (root.CN && typeof root.CN.api === "function" ? root.CN.api("charmNestLibrary", body, Object.assign({ quiet: true }, opts || {})) : Promise.reject(new Error("the page's cloud calls are not ready")));
  /* (VIEWAPPROVE, Paul 21 Nov 2026, Michelle's Approve refused with "the approval could not be saved on this browser": "Why is this happening?"): the run this page works in is the run it
     HOLDS (B.run, a computer that carries it) or, on every other computer, the run it FOLLOWS (CharmNestRunFollow, as charm-nest-viewer.js followedRun reads it). With the cloud carrying
     every run no page holds one, so the lane was never seen and Approve fell to the old save on the browser, which a viewer never makes. */
  const runIdNow = () => safe(() => (root.B && root.B.run && root.B.run.runId) || (root.CharmNestRunFollow && typeof root.CharmNestRunFollow.state === "function" && root.CharmNestRunFollow.state().runId) || null, null);
  const viewerOn = () => safe(() => !!(root.CNViewer && typeof root.CNViewer.on === "function" && root.CNViewer.on()), false);
  const devOf = () => safe(() => (root.CharmNestRunFollow && typeof root.CharmNestRunFollow.dev === "function" ? root.CharmNestRunFollow.dev() : ""), "");
  const say = (text, kind, ms) => { safe(() => { if (typeof root.toast === "function") root.toast(text, kind || "", ms || 6000); }); };
  const render = () => { safe(() => root.Engrave && root.Engrave.render && root.Engrave.render()); safe(() => root.Orders && root.Orders.render && root.Orders.render()); };

  function noteCarrier(runId, res) {
    if (!runId || !res || !res.ok) return;
    st.runId = runId; st.carrierCloud = !!(res.carrier && res.carrier.dev === "cloud"); st.carrierAt = Date.now();
    if (st.carrierCloud && Date.now() - st.driveAt > FRESH_MS) readDrive().catch(() => {});
  }
  function readDrive() {
    if (st.driveAsk) return st.driveAsk;
    st.driveAsk = api({ op: "driveGet" }).then(r => { const d = r && r.drive; st.drive = { mode: (r && r.mode) || "off", cloudSteps: d && Array.isArray(d.cloudSteps) ? d.cloudSteps.map(String) : [] }; st.driveAt = Date.now(); return st.drive; })
      .finally(() => { st.driveAsk = null; });
    return st.driveAsk;
  }
  function carries(runId) {
    const id = runId || runIdNow();
    return !!(id && st.runId === id && st.carrierCloud && st.drive && st.drive.mode !== "off" && st.drive.cloudSteps.includes("engrave"));
  }
  async function lane() {
    let runId = runIdNow();
    // (VIEWAPPROVE: a viewer pressed Approve before its first look at the run, 8 s after it opened: it looks now, once)
    if (!runId && viewerOn()) { try { const F = root.CharmNestRunFollow; if (F && typeof F.now === "function") await Promise.race([F.now("approve"), wait(8000)]); } catch (_) { /* no run seen */ } runId = runIdNow(); }
    if (!runId) return false;
    if (st.runId !== runId || Date.now() - st.carrierAt > FRESH_MS) {
      const dev = devOf(); if (!dev) return false;
      try { noteCarrier(runId, await api({ op: "runCarrier", runId, dev, check: true })); } catch (_) { return carries(runId); }
    }
    if (!st.carrierCloud) return false;
    if (!st.drive || Date.now() - st.driveAt > FRESH_MS) { try { await readDrive(); } catch (_) { /* the last copy stands */ } }
    return carries(runId);
  }

  /** The place the person approved, as the line's record keeps a job's place (charm-nest-engrave-sync.js jobRec), with the view's own centre (the cloud makes the very same view again). */
  function placeOf(job) {
    const f = job && job.fit, v = job && job.view; if (!f) return null;
    if (f.artOnly) return { artOnly: true, upAngle: v ? v.upAngle : null, ...(v ? { cx: v.cx, cy: v.cy } : {}) };
    if (!Array.isArray(f.centre) || !(f.size > 0)) return null;
    return { centre: f.centre.slice(), angle: f.angle || 0, size: f.size, lines: (Array.isArray(f.lines) && f.lines.length ? f.lines : job.lines || []).slice(), weight: f.weight || "Regular",
      lineGap: f.lineGap != null ? f.lineGap : job.lineGap, fittedMax: f.fittedMax || f.size, upAngle: v ? v.upAngle : null, ...(v ? { cx: v.cx, cy: v.cy } : {}), fontKey: job.fontKey || job.fontId || null };
  }
  /** (ITEM 9) The line's job an edited piece belongs to: the page's own job of that piece and face when it holds one (an earlier edit may have taken the piece out of it: copyOverrides), else
      the job key its saved record names (the line is the piece's pool id less its copy number; the slot and face as recorded). */
  function editTarget(job) {
    const pid = (job.copies || [])[0], rec = job.editOriginal || {}, F = root.CharmNestEngraveFaces;
    if (!pid || !/_\d{1,3}$/.test(pid)) return null;
    const faceOf = x => (F && typeof F.faceOf === "function" ? F.faceOf(x) : (x && x.face === "front" ? "front" : "back")), face = faceOf(job);
    const lineKey = String(pid).replace(/_\d{1,3}$/, "");
    const own = safe(() => [...root.Engrave.items().values()].find(j => j && !j.editingBack && !j.archived && String(j.key).startsWith(lineKey) && ((j.copies || []).includes(pid) || (j.copyOverrides || []).includes(pid)) && faceOf(j) === face), null);
    const jobKey = own ? own.key : (F && typeof F.jobKey === "function" ? F.jobKey(lineKey, rec.slot || null, face) : lineKey + (face === "front" ? "~front" : ""));
    return { lineKey, jobKey, edit: { poolId: pid, sheetId: (job.editSheet && job.editSheet.sheetId) || rec.sheetId || null, was: +job.expectedApprovedAt || +rec.approvedAt || null, sku: rec.sku || null, copy: rec.copy || null } };
  }
  async function approve(job, by) {
    const ed = job && job.editingBack ? editTarget(job) : null;
    if (job && job.editingBack && !ed) return { ok: false, message: "Not approved: this piece's order line could not be read from its saved back. Open it again from the sheet" };
    const runId = runIdNow(), line = job && job.row && (job.row.parentRow || job.row), lineKey = ed ? ed.lineKey : line && line.key, jobKey = ed ? ed.jobKey : job && job.key, place = placeOf(job);
    if (!runId || !lineKey) return { ok: false, local: true, message: "" };
    if (!place) return { ok: false, message: "Not approved: the words are not placed on the charm yet" };
    const fields = {}; for (const k of FIELD_KEYS) if (job[k] !== undefined) { const v = plain(job[k]); if (v !== undefined) fields[k] = v; }
    const at = Date.now();
    // (the line's own work first, so the cloud's copy of the job is this page's: the press then carries what the person approved)
    try { const ES = root.CharmNestEngraveSync; if (ES && typeof ES.flush === "function") await Promise.race([ES.flush(), wait(3000)]); } catch (_) { /* the press carries what matters */ }
    let res;
    try { res = await api({ op: "engraveApprove", runId, lineKey, jobKey, at, by, dev: devOf(), text: job.text == null ? "" : String(job.text), place, fields, artAtBy: job.artAtBy || null, ...(ed ? { edit: ed.edit } : {}) }); }
    catch (e) { const d = (e && e.data) || {}; return { ok: false, local: !!d.local, message: d.error || (e && e.message) || "the cloud did not answer" }; }
    if (!res || !res.ok) return { ok: false, local: !!(res && res.local), message: (res && res.error) || "the cloud did not take the approval" };
    st.sent++; st.last = { key: job.key, at: res.at || at, taskId: res.taskId || null };
    if (!res.already) watch(job, res.at || at, res.taskId, ed ? jobKey : null);
    return { ok: true, at: res.at || at, taskId: res.taskId || null, already: !!res.already, ...(ed ? { jobKey } : {}) };
  }

  /* ── the approvals the cloud is writing for this page ── */
  // (ITEM 9: an edit is watched under its own card's key; its task is the line's job's, `jobKey`)
  function watch(job, at, taskId, jobKey) { st.watch.set(job.key, { job, at: +at, taskId: taskId || null, since: Date.now(), order: String(jobKey || job.key).split("_")[0], jobKey: jobKey || null }); schedule(); }
  const taskKeyOf = (key, w) => (w && w.jobKey) || key;
  function schedule() { if (st.timer || !st.watch.size) return; st.timer = setTimeout(() => { st.timer = 0; poll().catch(() => {}).finally(schedule); }, POLL_MS); }
  async function poll() {
    if (!st.watch.size) return;
    st.polls++;
    for (const [key, w] of [...st.watch]) {
      const j = safe(() => root.Engrave.items().get(key), null) || w.job;
      if (["approved", "written"].includes(j.state) && +j.approvedAt >= w.at) { if (j.approving && j.approving.cloud) delete j.approving; st.watch.delete(key); render(); continue; }
      if (!j.approving || +j.approving.at !== w.at || Date.now() - w.since > WATCH_MAX_MS) { st.watch.delete(key); continue; }
    }
    if (!st.watch.size) return;
    let res = null; try { res = await api({ op: "engraveFilesGet", jobKeys: [...new Set([...st.watch].map(([k, w]) => taskKeyOf(k, w)))] }); } catch (_) { return; }
    const orders = new Set(), seen = new Set();
    let open = 0;
    for (const t of (res && res.tasks) || []) {
      const hit = [...st.watch].find(([k, w]) => taskKeyOf(k, w) === t.jobKey && +t.at === w.at); if (!hit) continue;
      const key = hit[0], w = hit[1];
      seen.add(key);
      const j = safe(() => root.Engrave.items().get(key), null) || w.job;
      if (t.state === "done") {   // (the record says "written" a moment later: read it now)
        orders.add(w.order);
        // (ITEM 9: an edited piece's card is no line's job: its file is written, so its approval is made here, its sheet's backs read again, and the seal comes down)
        if (w.jobKey) { st.watch.delete(key); safe(() => Promise.resolve(root.Engrave.cloudEditSaved(j, w.at)).catch(() => {})); }
        continue;
      }
      if (t.state === "failed" || t.state === "superseded") {
        if (j.approving && j.approving.cloud && +j.approving.at === w.at) { delete j.approving; if (t.state === "failed" || w.jobKey) { j.reason = t.why || "the cloud could not save the file"; say(String(j.reason), "bad", 8000); } }   // (ITEM 9: an edit the cloud did not write is always said)
        st.watch.delete(key); orders.add(w.order); render();
      } else { open++; if (j.approving && j.approving.cloud) j.approving.note = t.note || null; }
    }
    // (an approval the cloud has no task for (lost), or one sent while the cloud carried the run and it carries it no more (the drive turned off, a person took the run back): it is let go, said, and Approve works here again)
    const gone = [...st.watch.keys()].filter(k => !seen.has(k) && Date.now() - st.watch.get(k).since > 20000);
    if (open && !(await lane().catch(() => true))) for (const k of st.watch.keys()) if (seen.has(k)) gone.push(k);
    for (const k of gone) { const w = st.watch.get(k); if (!w) continue; const j = safe(() => root.Engrave.items().get(k), null) || w.job; if (j.approving && j.approving.cloud && +j.approving.at === w.at) { delete j.approving; j.reason = "The cloud did not save this approval: press Approve again"; say(j.reason, "bad", 8000); } st.watch.delete(k); render(); }
    if (orders.size) safe(() => root.CharmNestEngraveSync && root.CharmNestEngraveSync.pull && root.CharmNestEngraveSync.pull([...orders]));
  }
  /** The approvals sent to the cloud that this page holds but does not watch (a reload): watched again, so their seal comes down, or they are let go. A look at this page's memory, no read. */
  function adopt() {
    const items = safe(() => root.Engrave.items(), null); if (!items) return;
    for (const j of items.values()) if (j && j.approving && j.approving.cloud && !st.watch.has(j.key)) st.watch.set(j.key, { job: j, at: +j.approving.at, taskId: null, since: Date.now(), order: String(j.approving.jobKey || j.key).split("_")[0], jobKey: j.approving.jobKey || null });   // (ITEM 9: an edit's task is its line job's)
    schedule();
  }
  setInterval(adopt, 30000);
  const watching = () => [...st.watch.entries()].map(([key, w]) => ({ key, at: w.at, taskId: w.taskId, since: w.since }));
  const state = () => ({ runId: st.runId, carrierCloud: st.carrierCloud, carrierAt: st.carrierAt, drive: st.drive, driveAt: st.driveAt, watching: st.watch.size, polls: st.polls, sent: st.sent, last: st.last });

  root.CharmNestEngraveCloud = { noteCarrier, carries, lane, approve, placeOf, watching, state, FIELD_KEYS };
})(typeof window !== "undefined" ? window : globalThis);
