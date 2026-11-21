/*  charm-nest-drive.js — THE CLOUD DRIVE ON THIS PAGE (cloud plan v2, build steps 8 and 15; P8, P12, m:P3). DORMANT while Charm_Nest_Open/drive is absent or "off":
 *  the cloud tells this page its mode in every answer to the run's heartbeat (runCarrier); with "off" nothing here acts, and every hook below answers as if it were not loaded.
 *
 *  What it does:
 *    · Settings → Cloud (beside Etsy): the owner's control. Off / Standby / Cloud carries and the cloud's run mode (driveSet, admin-locked like the other Settings
 *      buttons), and what the cloud says (driveStatus): who carries the open run, a change that waits or was refused, what the cloud does, what "Cloud carries" needs.
 *    · THE HAND-OVER (step 15, P12): when "Cloud carries" was pressed and this computer carries the run, it starts nothing new (the run's loop waits at the next step's
 *      boundary, the intake and the gathering wait), lets what it is doing finish, saves the run's record, sends every change in its outbox, then tells the cloud it is idle
 *      (runCarrier idle: true). Until then it says what it is still doing (runCarrier busy), so the owner reads it in Settings. The cloud flips the mode and takes the run in
 *      one transaction; this page then sees that it no longer carries the run and reloads to follow it, as it does today for any take-over.
 *    · A RUN THE CLOUD CARRIES (step 8, P8): a Resume on this computer asks for it back (runCarrier restoreFromCloud): the cloud hands it back once it has finished what it is
 *      doing (handingBack, said in words, asked again every 15 s), then the run's record and sheets are read from the cloud, never from this computer's older copy; a reload
 *      while the cloud carries (or after it carried) drops this computer's saved sheets of that run and follows it (staleCheckpoint).
 *    · releasedAt (m:P3): the moment a sheet was first seen completed is kept on its record (outside "off"), so a set forms in the same order wherever it is formed.
 *    · caps(): what this page can do for the cloud, reported with the computer (charm-nest-drift.js): "standby drain" here; the viewer build (step 14) adds "viewer". */
(function (root) {
  "use strict";
  const CLOUD = "cloud", REPORT_MS = 15000, ASK_MS = 15000, ASK_TRIES = 48;
  const st = { mode: "off", pending: null, draining: null, held: null, lastReport: 0, saidIdle: false };
  const CAPS = new Set(["standby", "drain"]);
  const safe = (f, d) => { try { return f(); } catch (_) { return d; } };
  const sleep = t => new Promise(r => setTimeout(r, t));
  const say = (text, kind, ms) => safe(() => root.toast(text, kind || "", ms || 6000));
  const log = (runId, kind, text) => safe(() => root.agent(runId ? { run: runId } : { bridge: true }, kind, text));
  const dev = () => safe(() => root.CharmNestRunFollow.dev(), "") || "";
  const who = () => safe(() => { const w = root.StationSession && root.StationSession.who && root.StationSession.who(), n = typeof w === "string" ? w : (w && (w.name || w.username || w.user)) || ""; return n || root.localStorage.getItem("cn.employee") || ""; }, "") || "";
  const call = (body, opts) => root.api("charmNestLibrary", body, Object.assign({ quiet: true }, opts || {}));
  const nameOf = p => safe(() => (root.CharmNestSheetName ? root.CharmNestSheetName.short(p) : null), null) || p.fileBase || ("Sheet " + (p.page || ""));
  const pagesOf = runId => safe(() => (typeof allSheets === "function" ? allSheets() : []).filter(p => p && p.runId === runId), []);   // (allSheets: charm-nest-1.html's own list of pages)
  const clock = t => safe(() => new Date(t).toLocaleTimeString([], { hour: "numeric", minute: "2-digit" }), "");

  /** What the cloud said of its drive (every runCarrier answer carries it): its mode and a change that waits. */
  function note(res) {
    if (!res || typeof res !== "object") return;
    if (typeof res.driveMode === "string") st.mode = res.driveMode;
    if (Object.prototype.hasOwnProperty.call(res, "driveMode")) st.pending = res.drivePending || null;
  }
  const mode = () => st.mode;

  /* ── THE HAND-OVER (step 15) ── */
  /** Whether this computer is handing the run r to the cloud: it starts nothing new for it (Arrivals, the gathering, the run's next step). */
  function draining(r) { return !!(st.draining && r && r.runId && st.draining.runId === r.runId); }
  /** The run's loop asks before each step: while the hand-over waits it holds here, between two steps, and the run's record is saved once. */
  function holdLoop(r) {
    if (!draining(r) || r.status !== "running") return false;
    let h = st.held;
    if (!h || h.runId !== r.runId) h = st.held = { runId: r.runId, step: r.step, at: Date.now(), saved: false };
    if (h.saved === false || (h.saved === "failed" && Date.now() - h.at > 10000)) {
      h.saved = "saving"; h.at = Date.now();
      Promise.resolve(safe(() => root.RunCtl.save(r), null)).then(() => { h.saved = true; }, () => { h.saved = "failed"; h.at = Date.now(); });
    }
    return true;
  }
  /** What this computer still does for the run, in words (none: it is idle and may hand it over). */
  async function busyWords(r) {
    const why = [];
    if (r.status === "running") { const h = st.held; if (!h || h.runId !== r.runId) why.push(`in the middle of the run's ${r.step} step`); else if (h.saved !== true) why.push("saving the run's record"); }
    if (r.arrivalBusy) why.push("taking in new orders");
    for (const p of pagesOf(r.runId)) {
      if (["nesting", "finishing", "queued"].includes(p.status)) why.push(`${p.status} ${nameOf(p)}`);
      else if ((p.persisted && !p.persistedDone) || p._operationStarting || p._learnedStarting) why.push(`saving ${nameOf(p)}`);
    }
    if (safe(() => root.CharmNestOperations.busy("production:" + r.runId), false)) why.push("a sheet operation");
    try { const S0 = root.CharmNestStore; if (S0 && typeof S0.outbox === "function") { const box = await S0.outbox(); if (box && box.length) why.push(`sending ${box.length} change${box.length === 1 ? "" : "s"}`); } } catch (_) { /* no disk: nothing waits there */ }
    return why;
  }
  let reporting = false;
  async function report(r) {
    if (reporting || Date.now() - st.lastReport < REPORT_MS) return; reporting = true;
    try {
      const why = await busyWords(r), body = { op: "runCarrier", runId: r.runId, dev: dev(), by: who() };
      if (why.length) body.busy = why.slice(0, 4).join("; "); else body.idle = true;
      const res = await call(body); st.lastReport = Date.now(); note(res);
      if (!why.length && !st.saidIdle) { st.saidIdle = true; log(r.runId, "cloud", "This computer finished what it was doing and told the cloud it is idle: the cloud takes the run at its next look"); }
    } catch (_) { /* told at the next beat */ } finally { reporting = false; }
  }
  function startDrain(r, p) {
    st.draining = { runId: r.runId, at: +p.at || 0, since: Date.now() }; st.held = null; st.saidIdle = false; st.lastReport = 0;
    log(r.runId, "cloud", "The owner asked the cloud to carry every run: this computer starts nothing new, finishes what it is doing, then hands the run to the cloud");
    say("The cloud is taking over every run: this computer finishes what it is doing, then follows", "", 9000);
  }
  function stopDrain(why) {
    const r = st.draining; st.draining = null; st.held = null;
    if (r) { log(r.runId, "cloud", `The hand-over to the cloud ${why || "was called off"}: this computer carries on with the run`); say("The hand-over to the cloud was called off: this computer carries on", "", 6000); }
  }
  /** Called with every answer to the run's heartbeat (RunCtl carrierBeat, every 20 s). */
  function beat(res, r) {
    note(res);
    const p = st.pending;
    if (res && res.ok && res.mine && r && r.runId && p && p.to === "on") {
      if (!st.draining || st.draining.at !== (+p.at || 0) || st.draining.runId !== r.runId) startDrain(r, p);
      report(r).catch(() => {});
    } else if (st.draining && res && res.ok && res.mine && !(p && p.to === "on")) stopDrain();
  }

  /* ── A RUN THE CLOUD CARRIES (step 8, P8) ── */
  /** At a boot: this computer's saved copy of the run must not be used (the cloud carries it, or took it after the copy was saved). c: runCarrier { check: true }; d: the checkpoint. */
  function staleCheckpoint(c, d) {
    if (!c || !c.ok || !c.carrier || !d) return false;
    note(c);
    const car = c.carrier, took = car.took || null, at = +d.at || 0, since = +car.since || 0;
    if (!c.mine && car.dev === CLOUD) return true;
    if (took && took.dev === CLOUD && since > at) return true;
    if (c.driveMode && c.driveMode !== "off" && since > at) return true;
    return false;
  }
  /** A person's Resume of a run the cloud carries: asks for it back until the cloud has handed it over (in words meanwhile). → the carrier answer that makes it this
   *  computer's, the answer of a drive that is off (an ordinary take follows), or null (not taken: said). ask(o) sends runCarrier with o. */
  async function takeFromCloud(runId, ask) {
    let told = false;
    for (let i = 0; i < ASK_TRIES; i++) {
      let a = null; try { a = await ask({ restoreFromCloud: true }); } catch (_) { a = null; }
      note(a);
      if (!a || !a.ok) { say("The run could not be taken back from the cloud: press Resume again", "bad", 8000); return null; }
      if (a.mine) { log(runId, "cloud", "This computer carries the run again: the cloud handed it back; its record and sheets are read from the cloud"); return a; }
      if (a.handingBack) { if (!told) { told = true; say(`The cloud is handing this run back: ${a.why || "it is finishing a step"}. This computer takes it as soon as that ends.`, "", 12000); } await sleep(ASK_MS); continue; }
      if (a.cloud) { say("The cloud carries every run now: this computer follows it (Settings → Cloud)", "bad", 9000); return null; }
      return a;
    }
    say("The cloud did not hand the run back within 12 minutes: press Resume again", "bad", 9000);
    return null;
  }

  /* ── releasedAt on the sheet's record (m:P3): pages newly seen completed (seen), and pages filling again (lost). Outside "off" only. ── */
  function releasedAt(seen, lost) {
    if (st.mode === "off") return;
    const send = (p, v) => { if (!p || !p.sheetId || p.recalled || p.followed) return; call({ op: "putSheet", sheet: { id: p.sheetId, releasedAt: v } }).catch(() => {}); };
    for (const p of seen || []) send(p, +p.releasedAt || null);
    for (const p of lost || []) send(p, null);
  }

  /* ── Settings → Cloud: the owner's control (driveStatus, driveSet) ── */
  const HELP = {
    off: "Off: the computers carry every run, as before. The cloud does nothing.",
    standby: "Standby: a computer that is open keeps carrying its run. When no computer has been seen for 90 seconds the cloud carries the run: new orders are taken in, placed, nested and saved with every computer closed. Sets, labels, engraving files and commit wait for a computer: press Resume on one, and it takes the run back once the cloud has finished what it is doing.",
    on: "Cloud carries: the cloud carries every run. The computers show it and send presses."
  };
  let last = null, poll = null;
  const $ = id => document.getElementById(id);
  const esc = s => String(s == null ? "" : s).replace(/[&<>"]/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" })[c]);
  function carrierWords(s) {
    const c = s.carrier, r = s.run; if (!r) return "No run is open.";
    const run = `The open run ${esc(String(r.runId).slice(-8))} (${esc(r.status || "")}${r.step ? " at " + esc(r.step) : ""})`;
    if (!c || !c.dev) return `${run} is carried by no computer.`;
    if (c.cloud) return `${run} is carried by the cloud${c.since ? " since " + clock(c.since) : ""}${c.released ? " (let go: any computer may take it)" : ""}.`;
    return `${run} is carried by ${esc(c.by || "a computer")}${c.ageS != null ? `, seen ${c.ageS} s ago` : ""}${c.busy && c.busy.why ? `: still ${esc(c.busy.why)}` : c.idleAt ? ": idle" : ""}.`;
  }
  function render(s) {
    const pill = $("stDriveState"), help = $("stDriveHelp"), more = $("stDriveMore"), seg = $("stDriveSeg"), rm = $("stDriveRunMode");
    if (!pill) return;
    const btns = seg ? [...seg.querySelectorAll("button[data-drive]")] : [];
    if (!s || !s.ok) { pill.className = "pill neutral"; pill.textContent = "Could not read the cloud"; return; }
    if (s.sandbox) { pill.className = "pill neutral"; pill.textContent = "Sandbox: the computers carry its runs"; btns.forEach(b => { b.disabled = true; }); if (rm) rm.disabled = true; if (help) help.textContent = "The cloud carries production runs only."; if (more) more.innerHTML = ""; return; }
    const to = s.pending && s.pending.to;
    pill.className = "pill " + (to ? "warn" : s.mode === "off" ? "neutral" : s.mode === "on" ? "ok" : "info");
    pill.textContent = to ? ({ on: "Handing over to the cloud…", standby: "Handing the runs back…", off: "Turning the cloud off…" })[to] || "Changing…" : ({ off: "Off · the computers carry the runs", standby: "Standby · the cloud carries a run no computer carries", on: "The cloud carries every run" })[s.mode] || s.mode;
    btns.forEach(b => { b.classList.toggle("on", b.dataset.drive === (to || s.mode)); b.disabled = !!(to && to !== "on"); });
    if (rm) { rm.disabled = false; rm.value = s.runMode === "auto" ? "auto" : "manual"; }
    if (help) help.textContent = HELP[s.mode] || "";
    if (!more) return;
    const lines = [carrierWords(s)];
    if (to && s.pendingWait && s.pendingWait.why) lines.push(`<b>Waiting:</b> ${esc(s.pendingWait.why)}`);
    else if (to) lines.push(`<b>Asked</b> by ${esc(s.pending.by || "someone")} at ${clock(s.pending.at)}: the cloud looks within a minute.${to === "on" ? " Press the mode that is on now to call it off." : ""}`);
    if (s.pendingRefused && s.pendingRefused.why && (!s.flippedAt || +s.pendingRefused.at > +s.flippedAt)) lines.push(`<b>Not changed</b> (${clock(s.pendingRefused.at)}): ${esc(s.pendingRefused.why)}`);
    if (s.flippedAt) lines.push(`Changed to ${esc(s.mode)} at ${clock(s.flippedAt)}${s.flippedBy ? " by " + esc(s.flippedBy) : ""}.`);
    if (s.mode !== "off") lines.push(`The cloud does: ${s.owned && s.owned.length ? esc(s.owned.join(", ")) : "nothing yet"}${(s.cloudSteps || []).filter(x => !(s.owned || []).includes(x)).length ? ` · listed but not built yet: ${esc(s.cloudSteps.filter(x => !(s.owned || []).includes(x)).join(", "))} (the run parks there, in words)` : ""}.`);
    if (s.mode === "standby" && s.ready) lines.push(s.ready.ok ? "Cloud carries can be pressed: every step is built and every computer has the viewer build." : `<b>Before the cloud can carry every run:</b> ${esc(s.ready.missing.join(" · "))}.`);
    more.innerHTML = lines.map(l => `<div>${l}</div>`).join("");
  }
  async function refresh() {
    try { last = await call({ op: "driveStatus" }); } catch (e) { last = null; }
    render(last);
    clearTimeout(poll);
    const d = $("dlgSettings");
    if (last && last.pending && d && d.open) poll = setTimeout(() => { if (d.open) refresh(); }, 10000);   // (a change that waits is looked at again while Settings stays open)
    return last;
  }
  // (the answer is said on a line under the control, as the purge's is: the Settings dialog covers a toast)
  function told(text, bad) { const n = $("stDriveNote"); if (n) { n.textContent = text || ""; n.style.color = bad ? "var(--clay)" : ""; } else if (text) say(text, bad ? "bad" : "ok", 9000); }
  async function set(body, what) {
    const btns = [...document.querySelectorAll("#stDriveSeg button[data-drive], #stDriveRunMode")];
    btns.forEach(b => { b.disabled = true; }); told(what + "…");
    try {
      const res = await call(Object.assign({ op: "driveSet", by: who() }, body), { label: what });
      told((res && res.said) || "Saved in the cloud");
      st.mode = (res && res.mode) || st.mode;
    } catch (e) { told(String((e && e.message) || e), true); }
    finally { btns.forEach(b => { b.disabled = false; }); }
    return refresh();
  }
  const ASK = {
    off: "Turn the cloud off?\n\nThe computers carry every run, as before. A run the cloud carries is parked and let go: press Resume on a computer to carry it there.",
    standby: "Standby?\n\nA computer that is open keeps its run. The cloud carries a run only when no computer has been seen for 90 seconds, and hands it back on a person's Resume.",
    on: "Let the cloud carry every run?\n\nThe computer that carries the run finishes what it is doing and hands it over; every computer then follows the cloud and sends presses. Nothing changes until that computer says it is idle (10 minutes at most)."
  };
  function press(to) {
    const cur = last && last.ok ? (last.pending && last.pending.to) || last.mode : null;
    if (to === cur) return;
    const callOff = last && last.pending && last.pending.to === "on" && to === last.mode;
    if (!callOff && !root.confirm(ASK[to] || "Change who carries the runs?")) return;
    return set({ mode: to }, callOff ? "Calling the hand-over off" : "Changing who carries the runs");
  }
  if (typeof document !== "undefined") {
    document.addEventListener("click", e => { const b = e.target && e.target.closest && e.target.closest("#stDriveSeg button[data-drive]"); if (!b) return; e.preventDefault(); press(b.dataset.drive); });
    document.addEventListener("change", e => { if (e.target && e.target.id === "stDriveRunMode") set({ runMode: e.target.value === "auto" ? "auto" : "manual" }, "Changing the cloud's run mode"); });
  }

  const api = { build: "drive1", mode, pending: () => st.pending, note, beat, draining, holdLoop, busyWords, staleCheckpoint, takeFromCloud, releasedAt, refresh, render,
    caps: () => [...CAPS].join(" "), cap: name => { if (name) CAPS.add(String(name).slice(0, 20)); }, state: () => Object.assign({}, st) };
  root.CharmNestDrive = api;
})(typeof window !== "undefined" ? window : globalThis);
