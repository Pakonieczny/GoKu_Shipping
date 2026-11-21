/* charm-nest-viewer.js — PAGES BECOME VIEWERS (cloud plan v2, build step 14; C1, C2, E4, P12; m:C5, m:E6).
   (Paul, 20 Nov 2026: "NO production work runs on any computer; the cloud does all of it. Pages only show the cloud and send a person's presses.")

   The one switch is Charm_Nest_Open/drive (driveGet, read here at boot and every few minutes). While its mode is absent, "off" or "standby", NOTHING here changes what a page
   does: every gate below answers "go on as today". Under "on" the cloud carries every run, and this page is a viewer:
     · boot (m:C5): the page reads the switch before it restores its workspace or starts any production loop (Session.restore, Arrivals, Upkeep, AutoCancel, the backs, the
       keeper); in mode "on" it restores no workspace, writes none (Session.freeze), starts none of them, and keeps only what is on screen;
     · RunCtl is a view: Start, Stop, Resume, Next, Commit and Abandon send runControl (Start: runOpen first; the cloud mints a run it carries); nothing is looped here;
     · Nest and Nest all send nestStart (the cloud nests); a hold sends takeOffAsk, a release releaseAsk, a take-off from the sheet window takeOffAsk (sheetOff); a press whose
       cloud path is not on yet (its lane not in drive.cloudSteps, or the op not in this build) is refused in words, never done here;
     · every request to charmNestLibrary and charmNestOutput carries cv (this build): the server refuses a press from a page older than this one in mode "on" (C1);
     · no SKU table is asked for by itself (E4): "Load Etsy SKU list" stays a person's button;
     · the drain (P12): on the computer that CARRIES a run while drive.pending asks for "on", no new work starts (gather, intake, take-offs, Manual Control files, a nest), and
       once nothing is in hand (no production operation, no sheet nesting, finishing, queued, saving or changed, the outbox empty) it says so: runCarrier { idle: true };
     · backupAndClear (charm-nest-cloud-world.js) copies this browser's workspace and outbox to the cloud before the workspace is removed (the outbox is kept and drains: a
       person's press is never lost; keep intents are not moved anywhere, m:E6).
   A page that learns the mode changed (to "on", or from "on" back) reloads, so it boots as the new mode says.

   window.CNViewer: mode() · on() · known() · ready() · boot() · holdNew() · draining() · cv() · lane(name, runId?) · wait(name, fn) · pill(e) · control(action) · startRun(o)
                    nest(sh, fresh) · release(rid, o) · hold(rid, o) · holdPlan(rid) · takeOff(o) · state()                                                                   */
(function (root) {
  "use strict";
  const CV = "cloud14-20261120";
  const BOOT_WAIT_MS = 20000, LOOK_MS = 300000, LOOK_CARRYING_MS = 120000, DRAIN_MS = 5000, CARRIER_MS = 30000;
  const safe = (f, d) => { try { const v = f(); return v === undefined ? d : v; } catch (_) { return d; } };
  const api = (body, o) => (root.CN && typeof root.CN.api === "function" ? root.CN.api("charmNestLibrary", body, Object.assign({ quiet: true }, o || {})) : Promise.reject(new Error("the page is not ready")));
  const toast = (t, tone, ms) => safe(() => (typeof root.toast === "function" ? root.toast(t, tone || "ok", ms || 7000) : null));
  const dev = () => safe(() => root.CharmNestRunFollow.dev(), "");
  const who = () => String(safe(() => (typeof root.employeeName === "function" ? root.employeeName() : root.B && root.B.employee), "") || "").trim();
  const S = { known: false, assumed: false, mode: "off", drive: null, at: 0, fails: 0, reading: null, draining: false, idleAt: 0, carrier: null, carrierAt: 0, booted: false };
  let readyResolve = null; const readyP = new Promise(r => { readyResolve = r; });
  const settle = () => { if (!S.known) { S.known = true; readyResolve(S.mode); } };

  /* ── the switch ── */
  async function read(why) {
    if (S.reading) return S.reading;
    S.reading = (async () => {
      try {
        const r = await api({ op: "driveGet" }, { timeoutMs: 15000 });
        if (!r || r.error) throw new Error((r && r.error) || "no answer");
        const was = S.known && !S.assumed ? S.mode : null;
        S.drive = r.drive || null; S.mode = r.mode === "on" || r.mode === "standby" ? r.mode : "off"; S.at = Date.now(); S.fails = 0;
        const first = !S.known || S.assumed; S.assumed = false; settle();
        if (!first && was !== null && was !== S.mode && (was === "on" || S.mode === "on")) changed(was);
        else if (first && S.booted && S.mode === "on") changed("unknown");   // (the boot went on as today without an answer, and the cloud says "on": this page boots again as a viewer)
        drain();
      } catch (e) {
        S.fails++;
        // (no answer before the boot: asked again twice, 2 s apart; then the page goes on as today (the server refuses a production write in mode "on", and a later answer
        //  of "on" reloads this page as a viewer), so a computer that boots offline is not kept waiting)
        if (!S.known) { if (S.fails >= 3) { S.assumed = true; settle(); } else setTimeout(() => read("again").catch(() => {}), 2000); }
      }
      finally { S.reading = null; }
      return S.mode;
    })();
    return S.reading;
  }
  let reloading = false;
  function changed(was) {
    if (reloading) return; reloading = true;
    toast(S.mode === "on" ? "The cloud now carries every run: this page reloads to show it" : "The cloud no longer carries every run: this page reloads", "ok", 6000);
    // (never sooner than a minute after this page loaded: a cloud slow to answer at every boot reloads it at most once a minute, never in a loop)
    const up = safe(() => root.performance.now(), 60000);
    setTimeout(() => safe(() => root.location.reload()), Math.max(2500, 60000 - up));
    void was;
  }
  const mode = () => S.mode, on = () => S.mode === "on", known = () => S.known;
  /** The boot's question (m:C5): is this page a viewer? Waits for the first answer (BOOT_WAIT_MS at most: no answer goes on as today, the server being the net, and a later
   *  answer of "on" reloads the page). */
  async function boot() {
    read("boot").catch(() => {});
    await Promise.race([readyP, new Promise(r => setTimeout(r, BOOT_WAIT_MS))]);
    if (!S.known) { S.assumed = true; settle(); }
    S.booted = true;
    if (on()) {
      safe(() => root.Session && root.Session.freeze && root.Session.freeze());
      safe(() => root.CloudWorld && root.CloudWorld.backupAndClear && root.CloudWorld.backupAndClear().catch(() => {}));
      // (Q16: the design saves begun on this computer are not finished by this page any more: what reached the cloud is finished by its repair cron; said once, in words)
      const ki = safe(() => JSON.parse(root.localStorage.getItem("cn.keepIntents.v1") || "null"), null), n = ki ? (Array.isArray(ki) ? ki.length : Object.keys(ki).length) : 0;
      if (n) setTimeout(() => toast(`${n} design save(s) begun on this computer are not finished here any more: the cloud's repair finishes what reached the cloud, every 10 minutes`, "bad", 12000), 4000);
    }
    return on();
  }
  /** A production starter that may run only once the switch is known: deferred until then (and never in mode "on"). true when the caller must not go on now. */
  function wait(name, fn) {
    if (!S.known) { readyP.then(() => { if (!on() && typeof fn === "function") safe(fn); }); return true; }
    return on();
  }
  /** No new work starts: the page is a viewer, or it hands its run to the cloud (the drain). */
  const holdNew = () => on() || S.draining;

  /* ── the run's carrier and the lanes (P9: one lane, one place) ── */
  async function carrierOf(runId) {
    if (!runId) return null;
    if (S.carrier && S.carrier.runId === runId && Date.now() - S.carrierAt < CARRIER_MS) return S.carrier.c;
    const d = dev(); if (!d) return null;
    let c = null; try { const r = await api({ op: "runCarrier", runId, dev: d, check: true }); c = (r && r.carrier) || null; } catch (_) { c = null; }
    S.carrier = { runId, c }; S.carrierAt = Date.now(); return c;
  }
  const steps = () => (S.drive && Array.isArray(S.drive.cloudSteps) ? S.drive.cloudSteps.map(String) : []);
  /** Whether a lane is the cloud's for this run: the drive is not off, the lane is in cloudSteps, and the cloud carries the run (as last read; mode "on": always). */
  function lane(name, runId) {
    if (S.mode === "off" || !steps().includes(String(name))) return false;
    if (on()) return true;
    const c = S.carrier && (!runId || S.carrier.runId === runId) ? S.carrier.c : null;
    if (runId && (!S.carrier || S.carrier.runId !== runId || Date.now() - S.carrierAt > CARRIER_MS)) carrierOf(runId).catch(() => {});
    return !!(c && c.dev === "cloud");
  }
  const followedRun = () => safe(() => (root.CharmNestRunFollow && root.CharmNestRunFollow.state().runId) || (root.B && root.B.run && root.B.run.runId), null);

  /* ── RunCtl as a view (mode "on") ── */
  async function control(action) {
    let runId = followedRun();
    if (!runId) { try { const r = await api({ op: "runOpen", only: "get" }); runId = r && r.runId; } catch (_) { runId = null; } }
    if (!runId) { toast("No run is open in the cloud", "bad"); return { ok: false }; }
    let r = null; try { r = await api({ op: "runControl", runId, action, by: who(), dev: dev() }); } catch (e) { r = { error: (e && e.message) || String(e) }; }
    if (!r || r.error) { toast(`Not sent: ${(r && r.error) || "no answer"}`, "bad"); return { ok: false }; }
    const said = { stop: "Stop sent: the cloud stops the run", resume: "Resume sent: the cloud carries on", next: "Next sent", commit: "Commit sent: the cloud labels and commits", abandon: "Abandon sent: the cloud lets the run go", start: "Start sent: the cloud starts the run" }[action] || "Sent";
    toast(said, "ok", 5000);
    safe(() => root.CharmNestRunFollow && root.CharmNestRunFollow.now("control"));
    return { ok: true, runId, seq: r.seq };
  }
  async function startRun(o) {
    let r = null; try { r = await api({ op: "runOpen", mode: (o && o.mode) || "auto", day: safe(() => new Intl.DateTimeFormat("en-CA", { timeZone: "America/Toronto" }).format(new Date()), "") }); } catch (e) { r = { error: (e && e.message) || String(e) }; }
    if (!r || r.error || !r.runId) { toast(`The run was not started: ${(r && r.error) || "no answer"}`, "bad"); return null; }
    await control("start");
    return { runId: r.runId, cloud: true };
  }
  /** The run banner's buttons in mode "on": true when handled here (the rest of the menu, history and navigation, goes on as today). */
  function pill(e) {
    if (!on()) return false;
    const b = e && e.target && e.target.closest ? e.target.closest("button") : null; if (!b || b.disabled) return false;
    const close = () => safe(() => { const m = root.document.getElementById("runMenu"); if (m) m.open = false; });
    const act = b.dataset && b.dataset.rbres ? "resume" : { rbNow: "start", rbCommit: "commit", rbResume: "resume", rbStop: "stop", rbAbandon: "abandon" }[b.id];
    if (!act) return false;
    close();
    if (act === "abandon" && !safe(() => root.confirm("Let the cloud give up this run? Its unfinished pieces are let go for the next run; what was cut and committed stays."), false)) return true;
    (act === "start" ? startRun({ mode: "auto" }) : control(act)).catch(() => {});
    return true;
  }

  /* ── presses with a cloud path ── */
  async function nest(sh, fresh) {
    if (!sh || !sh.sheetId) { toast("This sheet is not saved in the cloud yet: the cloud nests it when its pieces are placed", "bad"); return false; }
    let r = null; try { r = await api({ op: "nestStart", sheetId: sh.sheetId, fresh: !!fresh, why: `${fresh ? "Nest" : "Nest the rest"} pressed by ${who() || "a person"}`, by: who() }); } catch (e) { r = { error: (e && e.message) || String(e) }; }
    if (!r || r.error) { toast(`Not nested: ${(r && r.error) || "no answer"}`, "bad"); return false; }
    toast(`${sh.fileBase || "The sheet"} is nesting in the cloud`, "ok", 5000);
    return true;
  }
  /** Release Hold in mode "on" (OrderHold.release): the release is made in the cloud (releaseAsk); the cloud places the pieces. Answers in OrderHold.release's own shape. */
  async function release(rid, o) {
    o = o || {}; rid = String(rid || "").replace(/\D/g, "");
    const steps = [], emit = s => { s.at = Date.now(); steps.push(s); safe(() => o.onStep && o.onStep(s)); return s; };
    const name = String(o.name || who() || "").trim();
    emit({ type: "start", rid });
    if (!name) { emit({ type: "error", rid, message: "A name is needed for the record: sign in, then press Release hold again", stage: "start" }); return { ok: false, released: false, rid, steps, error: "A name is needed for the record" }; }
    let r = null; try { r = await api({ op: "releaseAsk", orderId: rid, by: name, dev: dev(), runId: followedRun() || undefined }); } catch (e) { r = { error: (e && e.message) || String(e) }; }
    if (!r || r.error) { const m = `Order ${rid} is still on hold: ${(r && r.error) || "the cloud did not answer"}`; emit({ type: "error", rid, message: m, stage: "start" }); return { ok: false, released: false, rid, steps, error: m }; }
    emit({ type: "queued", rid, front: true, text: "Released in the cloud: the cloud places its pieces at the front of the queue" });
    emit({ type: "released", rid, sheets: [], placed: false }); emit({ type: "done", placed: false, why: "the cloud places it" });
    safe(() => root.CharmNestRunFollow && root.CharmNestRunFollow.now("released"));
    return { ok: true, released: true, rid, placed: false, sheets: [], steps, via: "cloud", frontAt: r.frontAt };
  }
  /** Hold (OrderHold.run) in mode "on": takeOffAsk { kind: hold }. Refused in words while the cloud's take-off lane is not on. */
  async function hold(rid, o) {
    o = o || {}; rid = String(rid || "").replace(/\D/g, "");
    const steps = [], emit = s => { s.at = Date.now(); steps.push(s); safe(() => o.onStep && o.onStep(s)); return s; };
    emit({ type: "start", rid });
    const fail = m => { emit({ type: "error", rid, message: m }); return { ok: false, rid, steps, error: m }; };
    if (!lane("takeoff")) return fail("Hold is not in the cloud yet: the cloud does not take orders off sheets in this build. Nothing was changed");
    const name = String(o.name || o.by || who() || "").trim(); if (!name) return fail("A name is needed for the record: sign in, then press Hold again");
    let r = null; try { r = await api({ op: "takeOffAsk", orderId: rid, kind: o.kind === "review" ? "review" : "hold", note: String(o.note || o.reason || "").slice(0, 300), by: name, dev: dev(), ...(o.only ? { only: o.only } : {}) }); } catch (e) { r = { error: (e && e.message) || String(e) }; }
    if (!r || r.error) return fail(`Not put on hold: ${(r && r.error) || "the cloud did not answer"}`);
    emit({ type: "queued", rid, text: "On hold in the cloud: the cloud takes its pieces off and fills the room" });
    emit({ type: "done", rid });
    safe(() => root.CharmNestRunFollow && root.CharmNestRunFollow.now("held"));
    return { ok: true, rid, steps, queued: true, held: true, via: "cloud" };
  }
  async function holdPlan(rid) {
    rid = String(rid || "").replace(/\D/g, "");
    if (!lane("takeoff")) return { canHold: false, blockedWhy: "Hold is not in the cloud yet: the cloud does not take orders off sheets in this build", estimate: true, pieces: [], sheets: [], fills: [], stays: [], effects: [] };
    try { const r = await api({ op: "holdPlanGet", orderId: rid }); if (r && !r.error) return r.plan || r; return { canHold: true, estimate: true, pieces: [], sheets: [], fills: [], stays: [], effects: [], note: r && r.error }; }
    catch (_) { return { canHold: true, estimate: true, pieces: [], sheets: [], fills: [], stays: [], effects: [] }; }
  }
  /** The sheet window's Take off (scope order or sheet) in mode "on": takeOffAsk { kind: sheetOff }. */
  async function takeOff(o) {
    o = o || {};
    if (o.mode === "cancel") { toast("A cancelled order is taken off its sheets by the cloud (its cancel is recorded): nothing more is needed here", "ok", 6000); return { ok: true, cloud: true }; }
    if (!lane("takeoff")) { toast("Take off is not in the cloud yet: nothing was changed", "bad"); return { ok: false }; }
    const rid = String(o.orderId || o.rid || "").replace(/\D/g, ""), name = String(o.by || who() || "").trim();
    if (!rid || !name) { toast(!name ? "Sign in first: the change is saved under your name" : "Pick an order", "bad"); return { ok: false }; }
    let r = null; try { r = await api({ op: "takeOffAsk", orderId: rid, kind: "sheetOff", sheetId: o.sheetId || undefined, scope: o.scope === "sheet" ? "sheet" : "order", note: String(o.note || "").slice(0, 300), by: name, dev: dev(), ...(o.only ? { only: o.only } : {}) }); } catch (e) { r = { error: (e && e.message) || String(e) }; }
    if (!r || r.error) { toast(`Not taken off: ${(r && r.error) || "no answer"}`, "bad"); return { ok: false }; }
    toast(`Order ${rid}: the cloud takes it off`, "ok", 5000);
    return { ok: true, queued: true };
  }

  /** A sheet changed by hand (Manual Control) in mode "on": its files are the cloud's sheet writer's (finishSheet, build step 7b); the record already owes them. */
  async function filesAsk(sh) {
    if (!sh || !sh.sheetId) return false;
    try { const r = await api({ op: "finishSheet", sheetId: sh.sheetId, why: "changed by hand", by: who() }); return !!(r && !r.error); } catch (_) { return false; }
  }

  /* ── the drain (P12): the computer that carries a run hands it to the cloud only once nothing is in hand ── */
  let drainT = 0;
  const carrying = () => !!safe(() => root.B && root.B.run && root.B.run.runId, false);
  async function idleNow() {
    const run = safe(() => root.B.run, null); if (!run || !run.runId) return { idle: false, why: "no run" };
    const ops = root.CharmNestOperations;
    if (ops && typeof ops.busy === "function" && safe(() => ops.busy("production:" + run.runId), false)) return { idle: false, why: "a production step is under way" };
    const pages = safe(() => (typeof root.allSheets === "function" ? root.allSheets() : []), []) || [];
    const busy = pages.find(p => p && p.runId === run.runId && (["nesting", "finishing", "queued"].includes(p.status) || p._operationStarting || (p.persisted && !p.persistedDone && !p.problem) || p.dirty || p.saveOwed));
    if (busy) return { idle: false, why: `${busy.fileBase || "a sheet"} is nesting, saving or changed` };
    if (safe(() => root.AutoCancel && root.AutoCancel.state && root.AutoCancel.state().running, false)) return { idle: false, why: "a cancelled order is being taken off" };
    const St = root.CharmNestStore;
    if (St && typeof St.outbox === "function") { const box = await St.outbox().catch(() => null); if (!box) return { idle: false, why: "the outbox could not be read" }; if (box.length) return { idle: false, why: `${box.length} change(s) still to be sent to the cloud` }; }
    return { idle: true };
  }
  function drain() {
    const p = S.drive && S.drive.pending, want = !!(p && p.to === "on" && S.mode === "standby" && carrying());
    if (!want) { if (S.draining) { S.draining = false; toast("The cloud is not taking the run after all: this computer carries on", "ok"); } if (drainT) { clearInterval(drainT); drainT = 0; } return; }
    if (!S.draining) { S.draining = true; toast("The cloud is taking the run over: this computer starts no new work and finishes what it has in hand", "ok", 9000); }
    if (drainT) return;
    const tick = async () => {
      if (!S.draining) { clearInterval(drainT); drainT = 0; return; }
      const v = await idleNow(); S.idleWhy = v.idle ? "" : v.why;
      if (!v.idle || Date.now() - S.idleAt < 60000) return;
      const run = safe(() => root.B.run, null), d = dev(); if (!run || !d) return;
      try { const r = await api({ op: "runCarrier", runId: run.runId, dev: d, idle: true }); if (r && r.idle) { S.idleAt = Date.now(); toast("This computer has nothing in hand: the cloud takes the run", "ok", 6000); } } catch (_) { /* told again at the next look */ }
    };
    drainT = setInterval(() => { tick().catch(() => {}); }, DRAIN_MS);
  }

  /* ── the switch looked at again: every 5 minutes, every 2 on the computer that carries a run or while a change of mode waits ── */
  setInterval(() => {
    const due = carrying() || (S.drive && S.drive.pending) ? LOOK_CARRYING_MS : LOOK_MS;
    if (Date.now() - S.at >= due && !(root.document && root.document.hidden && !carrying())) read("look").catch(() => {});
  }, 30000);

  /* ── OrderHold under mode "on": the release, the hold and its plan are the cloud's; nothing is resumed from this browser (applied once the switch says "on") ── */
  readyP.then(() => {
    if (!on()) return;
    const OH = root.OrderHold; if (!OH) return;
    OH.release = (rid, o) => release(rid, o);
    OH.run = (rid, o) => hold(rid, o);
    OH.plan = rid => holdPlan(rid);
    OH.resumeHolds = async () => 0;
  });

  root.CNViewer = { mode, on, known, ready: () => readyP, boot, holdNew, draining: () => S.draining, cv: () => CV, lane, carrierOf, wait, pill, control, startRun, nest, release, hold, holdPlan, takeOff, filesAsk, idleNow, read,
    state: () => ({ known: S.known, assumed: S.assumed, mode: S.mode, at: S.at, fails: S.fails, draining: S.draining, idleAt: S.idleAt, idleWhy: S.idleWhy || "", cloudSteps: steps(), pending: (S.drive && S.drive.pending) || null }) };
  // the first look as soon as the page can ask (CN.api exists once the page's script ran)
  const first = () => { if (root.CN && typeof root.CN.api === "function") read("start").catch(() => {}); else setTimeout(first, 300); };
  first();
})(typeof window !== "undefined" ? window : globalThis);
