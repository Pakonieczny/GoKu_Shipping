/* charm-nest-viewer.js — PAGES BECOME VIEWERS (cloud plan v2, build step 14; C1, C2, E4, P12; m:C5, m:E6).
   (Paul, 20 Nov 2026: "NO production work runs on any computer; the cloud does all of it. Pages only show the cloud and send a person's presses.")

   The one switch is Charm_Nest_Open/drive (driveGet, read here at boot and every few minutes). While its mode is absent, "off" or "standby", NOTHING here changes what a page
   does: every gate below answers "go on as today". Under "on" the cloud carries every run, and this page is a viewer:
     · boot (m:C5): the page reads the switch before it restores its workspace or starts any production loop (Session.restore, Arrivals, Upkeep, AutoCancel, the backs, the
       keeper); in mode "on" it restores no workspace, writes none (Session.freeze), starts none of them, and keeps only what is on screen;
     · RunCtl is a view: Start, Stop, Resume, Next, Commit and Abandon send runControl (Start: runOpen first; the cloud mints a run it carries); nothing is looped here;
     · Nest and Nest all send nestStart (the cloud nests); a hold sends takeOffAsk, a release releaseAsk, a take-off from the sheet window takeOffAsk (sheetOff), a Cancel Order
       cancelPut (the record the page's own path keeps; the cloud's take-off lane takes the pieces off: ITEM 1, Paul 21 Nov 2026); a press whose
       cloud path is not on yet (its lane not in drive.cloudSteps, or the op not in this build) is refused in words, never done here;
     · every request to charmNestLibrary and charmNestOutput carries cv (this build): the server refuses a press from a page older than this one in mode "on" (C1);
     · no SKU table is asked for by itself (E4): "Load Etsy SKU list" stays a person's button;
     · the drain (P12): on the computer that CARRIES a run while drive.pending asks for "on", no new work starts (gather, intake, take-offs, Manual Control files, a nest), and
       once nothing is in hand (no production operation, no sheet nesting, finishing, queued, saving or changed, the outbox empty) it says so: runCarrier { idle: true };
     · backupAndClear (charm-nest-cloud-world.js) copies this browser's workspace and outbox to the cloud before the workspace is removed (the outbox is kept and drains: a
       person's press is never lost; keep intents are not moved anywhere, m:E6).
   A page that learns the mode changed (to "on", or from "on" back) reloads, so it boots as the new mode says.

   window.CNViewer: mode() · on() · known() · ready() · boot() · holdNew() · draining() · cv() · lane(name, runId?) · wait(name, fn) · pill(e) · control(action) · startRun(o)
                    nest(sh, fresh) · release(rid, o) · hold(rid, o) · asViewer() · holdPlan(rid) · takeOff(o) · state()
                    retry(rid, o) · ack(rid, at, o) · lastAct()   (S5: Try again on a take-off the cloud held or gave up on; Set aside on a cancelled order's notice)
                    filesLane(sh) · filesGate(sh, o) · filesAsk(sh, o)   (build step 7b: the files lane on a page, below)
                    handArt(metal, charms, o) · charmPress(sh, c, change, o) · cmdFollow(runId, id, o)   (S3: artwork added by hand, a hand piece's Exclude / Include / Move)
                    keepSaves() · keepResumes()   (item 15, Paul 21 Nov 2026: the design saves begun on this computer, checked against the cloud, said once)
                    acceptChange(o) · settledPut(d) · settledPull() · settledApply() · settledState()   (S2, Paul 21 Nov 2026: Review's Accept the new order and Resolved, below)                                                                 */
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
      // (Q16, and item 15, Paul 21 Nov 2026: the design saves begun on this computer, checked against the cloud first and said once, in calm words: keepSaves below)
      setTimeout(() => keepSaves().catch(() => {}), 4000);
    }
    return on();
  }

  /* ── the design saves begun on this computer (item 15, Paul 21 Nov 2026: "messages must tell the truth") ──
     The keeper's intents (charm-nest-keep.js, localStorage cn.keepIntents.v1 = { v, intents: { key: rec } }) outlive the save: a done or switched-off one stays 14 days, and
     one the cloud finished while this page was a viewer is never told so. The boot said "N design save(s) ... are not finished" at EVERY reload (counting the record's own
     two fields, v and intents, whatever was in it), in red. Now:
       · only an intent still wanted (wanted, running, waiting) or needing a person (attention) is looked at; each is asked of the cloud (customSaveGet, a READ) and one the
         cloud finished (saved to the repository, and linked on Etsy or matched by the listing's own answer, as customSavePending reads it) is removed from this browser;
       · what is left is said in words that are true on this page: nothing about the live ones when this page's keeper resumes them (keepResumes() below: the keeper says "Finishing N design save(s)" itself, item 14), else what the
         cloud does (one in the repository: its repair finishes it) and what needs a person (the order's design window), naming the orders;
       · calm ("ok") unless a person must act; said ONCE per intent and its state (localStorage cn.keepIntentsSaid.v1): the next reload says nothing about the same intents. */
  const INTENTS_KEY = "cn.keepIntents.v1", SAID_KEY = "cn.keepIntentsSaid.v1", KEEP_CHECK_MAX = 20;
  const ls = () => safe(() => root.localStorage, null);
  const readJson = k => safe(() => JSON.parse(ls().getItem(k) || "null"), null);
  const writeJson = (k, v) => safe(() => { if (v == null) ls().removeItem(k); else ls().setItem(k, JSON.stringify(v)); return true; }, false);
  /** Does this page's save keeper resume the saves begun here, in mode "on"? (item 14 makes it so; whichever of these it exposes, a boolean or a function) */
  function keepResumes() {
    const ask = x => (typeof x === "function" ? !!safe(() => x(), false) : x === true);
    return ask(safe(() => root.CharmNestKeep && root.CharmNestKeep.resumesOnViewer, null)) || ask(safe(() => root.CustomSheet && root.CustomSheet.keepResumes, null));
  }
  /** The cloud finished this intent's save: in the repository (this design's own record, not an older one of the listing), and linked on Etsy or matched by the listing's answer. */
  function cloudFinished(rec, save) {
    if (!save || !save.sku || save.phase !== "repo" || save.superseded) return false;
    if (rec.sku && String(rec.sku).toUpperCase() !== String(save.sku).toUpperCase()) return false;
    if (!rec.etsyOnly && !rec.repo && +save.at && +rec.at && +save.at < +rec.at) return false;   // (a record older than the switch: another drawing of the listing, a correction waits)
    if (save.etsy && save.etsy.state === "linked") return true;
    const K = root.CharmNestSku;   // (ETSYALIAS, as customSavePending reads it: no Etsy write is needed when the listing's answer matches the SKU Etsy carries)
    return save.alias !== "none" && !!save.oldSku && !!(K && typeof K.aliasWorks === "function") && !!safe(() => K.aliasWorks(String(save.oldSku), String(save.sku)), false);
  }
  const plural = (n, one, many) => (n === 1 ? one : many);
  const ordersOf = list => { const ids = [...new Set(list.map(r => String(r.rid || "")).filter(Boolean))]; const shown = ids.slice(0, 3).join(", ") + (ids.length > 3 ? ` and ${ids.length - 3} more` : ""); return ids.length ? `${plural(ids.length, "order", "orders")} ${shown}` : "an order"; };
  /** The boot's check (mode "on" only): { live, attention, dropped, said, text, tone }. Exposed for tests as CNViewer.keepSaves(). */
  async function keepSaves() {
    const out = { live: 0, attention: 0, dropped: 0, said: false, text: "", tone: "" };
    if (!on()) return out;
    const box = readJson(INTENTS_KEY), all = box && box.intents && typeof box.intents === "object" ? box.intents : {};
    const open = Object.values(all).filter(r => r && r.key && !r.sandbox && (r.state === "wanted" || r.state === "running" || r.state === "waiting" || r.state === "attention")).slice(0, KEEP_CHECK_MAX);
    const finished = [];
    for (const rec of open) {
      let r = null; try { r = await api({ op: "customSaveGet", rid: String(rec.rid), listingId: String(rec.listingId) }); } catch (_) { r = null; }
      if (r && !r.error && cloudFinished(rec, r.save)) finished.push(rec.key);   // (no answer: kept and counted, never guessed finished)
    }
    if (finished.length) {   // removed from this browser, read again just before the write (the keeper of this or another tab may have written meanwhile)
      const now = readJson(INTENTS_KEY);
      if (now && now.intents && typeof now.intents === "object") { const next = Object.assign({}, now.intents); for (const k of finished) delete next[k]; if (writeJson(INTENTS_KEY, Object.assign({}, now, { intents: next }))) out.dropped = finished.length; }
    }
    const left = open.filter(r => !finished.includes(r.key)), resumes = keepResumes();
    const live = left.filter(r => r.state !== "attention"), attention = left.filter(r => r.state === "attention");
    out.live = live.length; out.attention = attention.length;
    // once per intent: what was said is kept by key and state; the same intents in the same state are not said again at the next reload
    const sigOf = r => [r.state === "attention" ? "attention" : "live", r.repo ? 1 : 0, resumes ? 1 : 0].join("|");
    const said = readJson(SAID_KEY) || {}, mem = {}; let fresh = false;
    for (const r of left) { mem[r.key] = sigOf(r); if (said[r.key] !== mem[r.key]) fresh = true; }
    writeJson(SAID_KEY, left.length ? mem : null);
    if (!fresh) return out;
    const parts = []; let person = false;
    // (item 14 merged: the page's own keeper says "Finishing N design save(s)…" itself and tells each one done; said here too it would be said twice)
    if (live.length && resumes) { /* said by the keeper (charm-nest-bridge.js keepViewerResume) */ }
    else if (live.length) {
      const inRepo = live.filter(r => r.repo), notYet = live.filter(r => !r.repo);
      if (inRepo.length) parts.push(`${inRepo.length} design ${plural(inRepo.length, "save", "saves")} started on this computer ${plural(inRepo.length, "is", "are")} in the repository; the cloud's repair finishes ${plural(inRepo.length, "it", "them")} within 10 minutes (${ordersOf(inRepo)})`);
      if (notYet.length) { person = true; parts.push(`${notYet.length} design ${plural(notYet.length, "save", "saves")} started on this computer did not reach the repository: open the design window of ${ordersOf(notYet)} to save ${plural(notYet.length, "it", "them")} again`); }
    }
    if (attention.length) { person = true; parts.push(`${attention.length} design ${plural(attention.length, "save", "saves")} started on this computer ${plural(attention.length, "needs", "need")} a person: open the design window of ${ordersOf(attention)}`); }
    if (!parts.length) return out;
    out.said = true; out.text = parts.join(". "); out.tone = person ? "bad" : "ok";
    toast(out.text, out.tone, 12000);
    return out;
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
  /* GAP G2 (Paul 21 Nov 2026: the sheet hop with no owner step): a request lane covered by a lane on already, the copy of _charmNestStandby.js CMD_LANE_ALIAS: "cmd:hop" is the
     cloud's while "cmd:hop", "cmd:seat" or "cmd:renest" is (the same family of sheet commands). */
  const LANE_ALIAS = { "cmd:hop": ["cmd:seat", "cmd:renest"] };
  const laneListed = name => { const on = steps(), n = String(name); return on.includes(n) || (LANE_ALIAS[n] || []).some(a => on.includes(a)); };
  /** Whether a lane is the cloud's for this run: the drive is not off, the lane is in cloudSteps (or one that covers it, LANE_ALIAS), and the cloud carries the run (as last read; mode "on": always). */
  function lane(name, runId) {
    if (S.mode === "off" || !laneListed(name)) return false;
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
    // (S4: a sheet the cloud is nesting already is not asked twice: its card shows "Nesting in the cloud…" with Stop; Nest all passes it by in silence)
    if (nestOf(sh)) { if (fresh) toast(`${nameOfSheet(sh)} is already nesting in the cloud: press Stop on its card to stop it`, "warn", 6000); return false; }
    sh._s4Asking = true; paintSheet(sh);
    let r = null; try { r = await api({ op: "nestStart", sheetId: sh.sheetId, fresh: !!fresh, why: `${fresh ? "Nest" : "Nest the rest"} pressed by ${who() || "a person"}`, by: who() }); } catch (e) { r = { error: (e && e.message) || String(e) }; }
    finally { delete sh._s4Asking; }
    if (!r || r.error) { paintSheet(sh); toast(`Not nested: ${(r && r.error) || "no answer"}`, "bad"); return false; }
    const st = String(r.status || "queued");
    if (r.jobId) NESTS.set(sh.sheetId, { jobId: String(r.jobId), status: st, at: Date.now(), by: who() });
    paintSheet(sh);
    if (!NEST_LIVE.includes(st)) { toast(st === "done" ? `${nameOfSheet(sh)} is already nested in the cloud as it stands: nothing changed since its last nest` : `${nameOfSheet(sh)}: the cloud did not start a nest (${r.why || st})`, st === "done" ? "ok" : "bad", 8000); return false; }
    toast(`${nameOfSheet(sh)} is nesting in the cloud`, "ok", 5000);
    watchNests();
    return true;
  }
  /* ── S4 (Paul 21 Nov 2026: a person's Stop of a nest the cloud runs): the cloud's nest of each sheet, as this page knows it — from the Nest press's answer (nestStart: its jobId), from
     the sheet's record (nestJob { id, status, at }, read by the change feed: charm-nest-cloud-sheets.js calls sawSheet) and, while one runs, from its job (getJob, a READ, every 5 s).
     The card (charm-nest-1.html renderCard) shows "Nesting in the cloud…" and Stop while it runs; Stop sends stopJob { id, for: "person" } (a PRESS: taken from a viewer), and the
     card says "Stopping in the cloud…" until the job itself reads stopped. Nothing here runs in mode "off". ── */
  const NESTS = new Map(), NEST_LIVE = ["queued", "running", "starting"], NEST_STALE_MS = 15 * 60000;
  const numOf = v => (Number.isFinite(+v) ? +v : 0);
  const nameOfSheet = sh => String(safe(() => (root.CharmNestSheetName && root.CharmNestSheetName.short(sh)) || sh.fileBase, "") || (sh && sh.fileBase) || "This sheet");
  const nestLive = n => !!(n && n.jobId && NEST_LIVE.includes(String(n.status || "queued")) && !(n.fromRecord && !n.checked && numOf(n.at) > 0 && Date.now() - numOf(n.at) > NEST_STALE_MS));   // (a record's mark older than a nest's lease, not yet read from its job: a nest that died)
  const pageOfId = id => safe(() => (root.CN && typeof root.CN.allSheets === "function" ? root.CN.allSheets() : []).find(p => p && p.sheetId === id), null) || null;
  function paintSheet(sh) { if (sh) safe(() => { if (root.CN && typeof root.CN.renderCard === "function") root.CN.renderCard(sh); else if (typeof root.renderCard === "function") root.renderCard(sh); }); }
  /** The cloud's nest of this sheet while it runs (or the Nest press while it is being asked): { jobId, status, stopping? } | null. Mode "on" only. */
  function nestOf(sh) {
    if (!on() || !sh) return null;
    if (sh._s4Asking) return { jobId: null, status: "asking", asking: true };
    const id = sh.sheetId || null; if (!id) return null;
    const n = NESTS.get(id); if (n) return nestLive(n) ? n : null;
    // (a card still showing a saved record: the record's own mark)
    const nj = (sh.recalled && sh.recalled.nestJob) || (sh._rec && sh._rec.nestJob) || null;
    if (nj && typeof nj === "object" && sawRecord(id, nj)) { const m = NESTS.get(id); if (nestLive(m)) { watchNests(); return m; } }
    return null;
  }
  function sawRecord(id, nj) {
    const jobId = String(nj.id || nj.jobId || ""); if (!jobId) return false;
    const st = String(nj.status || nj.state || "queued"), was = NESTS.get(id);
    if (was && was.jobId !== jobId && numOf(was.at) > numOf(nj.at)) return false;   // (an older record than the nest this page asked for)
    if (was && was.jobId === jobId && (was.status === st || !NEST_LIVE.includes(was.status))) return false;   // (nothing new; a job read as ended stays ended)
    NESTS.set(id, Object.assign({ fromRecord: true }, was && was.jobId === jobId ? was : {}, { jobId, status: st, at: numOf(nj.at) || Date.now() }));
    return true;
  }
  /** The change feed read a sheet's record (charm-nest-cloud-sheets.js): its nestJob is what the cloud is doing with it. */
  function sawSheet(p, rec) {
    if (!on() || !rec || !(rec.id || (p && p.sheetId))) return;
    const id = rec.id || p.sheetId, nj = rec.nestJob && typeof rec.nestJob === "object" ? rec.nestJob : null;
    if (!nj || !sawRecord(id, nj)) return;
    paintSheet(p || pageOfId(id));
    if (nestLive(NESTS.get(id))) watchNests();
  }
  let nestT = 0;
  function watchNests() { if (nestT || !on()) return; nestT = setTimeout(() => { nestT = 0; nestLook().catch(() => {}); }, 5000); safe(() => nestT && typeof nestT.unref === "function" && nestT.unref()); }
  async function nestLook() {
    if (!on()) return;
    for (const [id, n] of [...NESTS]) {
      if (!nestLive(n)) continue;
      let r = null; try { r = await api({ op: "getJob", id: n.jobId }); } catch (_) { continue; }
      if (!r || r.error || !Object.prototype.hasOwnProperty.call(r, "job")) continue;
      const j = r.job, was = n.status, st = j ? String(j.status || j.state || was) : "gone";
      n.status = st; n.checked = true; n.stage = j && j.stage ? String(j.stage) : n.stage;
      if (st === was) continue;
      const p = pageOfId(id), name = p ? nameOfSheet(p) : "The sheet";
      if (!NEST_LIVE.includes(st)) n.stopping = 0;
      paintSheet(p);
      if (NEST_LIVE.includes(st)) continue;
      if (st === "stopped") toast(`${name}: the cloud stopped its nest${j && j.stopBy ? ` (Stop pressed by ${j.stopBy})` : ""}. Press Nest when it should nest again`, "ok", 8000);
      else if (st === "error") toast(`${name}: the cloud's nest failed — ${String((j && j.error) || "no reason given").slice(0, 200)}. Press Nest to try again`, "bad", 12000);
      else if (st === "gone") toast(`${name}: its nest is no longer in the cloud. Press Nest if it still needs one`, "warn", 9000);
    }
    if ([...NESTS.values()].some(nestLive)) watchNests();
  }
  /** Stop on a card whose sheet the cloud is nesting: stopJob { id, for: "person" } (the cloud then waits for that sheet's Nest press). Said in words; the card says "Stopping…" until the job reads stopped. */
  async function stopNest(sh) {
    const n = nestOf(sh), name = nameOfSheet(sh);
    if (!n || !n.jobId) { toast(`${name}: no nest of it is running in the cloud, so there is nothing to stop`, "warn", 6000); paintSheet(sh); return false; }
    if (n.stopping) { toast(`${name}: Stop was already sent; the cloud stops after its current step`, "ok", 5000); return false; }
    n.stopping = Date.now(); paintSheet(sh);
    let r = null; try { r = await api({ op: "stopJob", id: n.jobId, for: "person", by: who() || "", why: `Stop pressed by ${who() || "a person"} on ${dev() || "a computer"}` }); } catch (e) { r = { error: (e && e.message) || String(e) }; }
    if (!r || r.error || !r.ok) { n.stopping = 0; paintSheet(sh); toast(`${name}: not stopped — ${(r && r.error) || "no answer"}. Press Stop again`, "bad", 9000); return false; }
    toast(`Stop sent: the cloud stops nesting ${name} after its current step`, "ok", 6000);
    watchNests();
    return true;
  }
  /** Release Hold in mode "on" (OrderHold.release): the release is made in the cloud (releaseAsk); the cloud places the pieces. Answers in OrderHold.release's own shape. */
  async function release(rid, o) {
    o = o || {}; rid = String(rid || "").replace(/\D/g, "");
    /* (ITEM 8, Paul 21 Nov 2026: "release anyway" / a blocked SKU cut anyway, pressed on a viewer, lifted the hold in the cloud while the line kept its questions or its block: only
       { orderId, by, dev, runId } went.) An order whose held lines are on this page is released by the page's own release (charm-nest-order-release.js), which on a computer that
       carries no run (every viewer) is the cloud's: releaseAsk with the lines' keys, what the press set aside on each (releaseAnyway, blockOverride), the seal and the note, the
       cloud's lines put back on the rows, said as the cloud did it. Only an order this page does not hold goes as before. */
    const own = S.ownRelease, mine = safe(() => root.Orders.rows().filter(r => r && r.order && String(r.order.receiptId) === rid && r.state !== "gone"), []) || [];
    if (own && !safe(() => root.B && root.B.run, null) && mine.some(r => r.hold || r.releasing || safe(() => root.Orders.heldNow(r), false))) return own(rid, o);
    const steps = [], emit = s => { s.at = Date.now(); steps.push(s); safe(() => o.onStep && o.onStep(s)); return s; };
    const name = String(o.name || who() || "").trim();
    emit({ type: "start", rid });
    if (!name) { emit({ type: "error", rid, message: "A name is needed for the record: sign in, then press Release hold again", stage: "start" }); return { ok: false, released: false, rid, steps, error: "A name is needed for the record" }; }
    let r = null; try { r = await api(Object.assign({ op: "releaseAsk", orderId: rid, by: name, dev: dev(), runId: followedRun() || undefined }, o.seal ? { seal: o.seal } : {})); } catch (e) { r = { error: (e && e.message) || String(e) }; }
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
  /** The sheet window's Take off (scope order or sheet) in mode "on": takeOffAsk { kind: sheetOff }.
      (S5, Paul 21 Nov 2026: "Take off and hold", "Only this charm" or "The whole order", was refused on every staff computer: no sheet went with it ("which sheet the order comes off
      is required"), and the pieces chosen were dropped, so "Only this charm" would have taken the whole order off.) Now the sheet (o.sheetId), the pieces chosen (o.only: only those
      come off, the rest of the order stays) and the scope go with the press, and the cloud's answer is said: its take-off job (the key takeOffAsk answers) followed to its end. */
  async function takeOff(o) {
    o = o || {};
    if (o.mode === "cancel") return cancel(o);
    if (!lane("takeoff")) { toast("Take off is not in the cloud yet: nothing was changed", "bad"); return { ok: false }; }
    const rid = String(o.orderId || o.rid || "").replace(/\D/g, ""), name = String(o.by || who() || "").trim();
    if (!rid || !name) { toast(!name ? "Sign in first: the change is saved under your name" : "Pick an order", "bad"); return { ok: false }; }
    const only = [...new Set((Array.isArray(o.only) ? o.only : []).map(x => String((x && typeof x === "object" ? x.id : x) || "")).filter(Boolean))], sheetId = o.sheetId ? String(o.sheetId) : "";
    if (!sheetId && !only.length) { toast(`Order ${rid} was not taken off: the sheet or the pieces to take off were not known. Nothing was changed`, "bad", 9000); return { ok: false }; }
    let r = null; try { r = await api({ op: "takeOffAsk", orderId: rid, kind: "sheetOff", sheetId: sheetId || undefined, scope: o.scope === "sheet" ? "sheet" : "order", note: String(o.note || "").slice(0, 300), by: name, dev: dev(), ...(only.length ? { only } : {}) }); } catch (e) { r = { error: (e && e.data && e.data.error) || (e && e.message) || String(e) }; }
    if (!r || r.error) { toast(`Order ${rid} was not taken off: ${String((r && r.error) || "the cloud did not answer").replace(/\.$/, "")}. Nothing was changed`, "bad", 9000); return { ok: false, error: (r && r.error) || "no answer" }; }
    const n = only.length, what = n === 1 ? "this piece" : n ? `these ${n} pieces` : "it";
    toast(`Order ${rid}: the cloud takes ${what} off and puts ${n === 1 || !n ? "it" : "them"} on hold${n ? "; the rest of the order stays where it is" : ""}`, "ok", 6000);
    safe(() => root.CharmNestRunFollow && root.CharmNestRunFollow.now("held"));
    if (r.job) follow(rid, String(r.job), offWords(rid), { by: name });
    return { ok: true, queued: true, via: "cloud", askId: r.askId, job: r.job || null };
  }
  const placesOf = l => (l || []).map(x => x && x.where).filter(Boolean).join(", ");
  const piecesOf = l => (l || []).reduce((s, x) => s + (+(x && x.n) || 0), 0);
  const plainWhy = (j, d) => String((j && j.why) || d || "").replace(/\.$/, "");
  /** What the person is told of a sheet Take off (kind sheetOff), as the cloud's job ends. */
  const offWords = rid => ({
    done: j => { const off = placesOf(j.taken), left = placesOf(j.left), n = piecesOf(j.taken);
      if (!off && !left) return [`Order ${rid}: the cloud found none of those pieces on a sheet any more: nothing was taken off`, "bad", 9000];
      return [`Order ${rid}: the cloud took ${n === 1 ? "1 piece" : n ? n + " pieces" : "its pieces"} off ${off || "no sheet"} and put ${n === 1 ? "it" : "them"} on hold${left ? `; its pieces on ${left} stay (${(j.left || []).map(x => x && x.why).filter(Boolean).join(", ") || "already cut"})` : ""}`, left ? "bad" : "ok", left ? 12000 : 6000]; },
    held: j => `Order ${rid} was not taken off: ${plainWhy(j, j && j.status)}`,
    late: j => `Order ${rid}: the cloud has not finished taking it off in 20 minutes${j && j.why ? " (" + plainWhy(j) + ")" : ""}, and keeps at it`
  });
  /** What the person is told of a cancel's take-off (kind cancel). */
  const cancelWords = rid => ({
    done: j => { const off = placesOf(j.taken), left = placesOf(j.left); return [`Order ${rid}: the cloud took its pieces off${off ? " " + off : ""}${left ? `; set aside its pieces on ${left} (already cut)` : ""}`, left ? "bad" : "ok", left ? 12000 : 6000]; },
    held: j => `Order ${rid} is cancelled, but the cloud could not take its pieces off: ${plainWhy(j, j && j.status)}`,
    late: j => `Order ${rid} is cancelled; the cloud has not finished taking its pieces off in 20 minutes${j && j.why ? " (" + plainWhy(j) + ")" : ""}, and keeps at it`
  });
  /** A toast with one button beside its words (S5: Try again). The page's toast answers its element; the button acts once and the toast goes. */
  function toastAct(text, tone, ms, label, fn) {
    const el = toast(text, tone, ms);
    S.lastAct = { text, label, fn };   // (the last such button, for the page's tests)
    safe(() => {
      const doc = root.document; if (!el || !doc || typeof el.appendChild !== "function" || (el.querySelector && el.querySelector("[data-cn-act]"))) return;
      const b = doc.createElement("button"); b.type = "button"; b.className = "btn xs"; b.setAttribute("data-cn-act", label); b.textContent = label; b.style.marginLeft = "8px";
      b.onclick = e => { safe(() => e && e.stopPropagation && e.stopPropagation()); b.disabled = true; safe(() => el._go && el._go()); Promise.resolve().then(fn).catch(() => {}); };
      const x = el.querySelector && el.querySelector(".c"); if (x && typeof el.insertBefore === "function") el.insertBefore(b, x); else el.appendChild(b);
    });
    return el;
  }
  /** (S5, Paul 21 Nov 2026: a take-off the cloud held or gave up on said "press Try again" with no Try again anywhere.) Try again: the order's held, stuck or waiting take-offs start
      again in the cloud (takeOffAsk { kind: retry }: the lane makes each due, its tries at 0). key: the job followed again after it; words: how its end is said. */
  async function retry(rid, o) {
    o = o || {}; rid = String(rid || "").replace(/\D/g, "");
    const name = String(o.by || who() || "").trim();
    const no = m => { toast(m, "bad", 9000); return { ok: false, error: m }; };
    if (!rid) return no("Pick an order");
    if (!name) return no(`Order ${rid}: sign in first, then press Try again (it is saved under your name)`);
    if (!lane("takeoff")) return no(`Order ${rid} was not tried again: the cloud does not take orders off sheets in this build. Nothing was changed`);
    let r = null; try { r = await api({ op: "takeOffAsk", orderId: rid, kind: "retry", note: "Try again", by: name, dev: dev() }); } catch (e) { r = { error: (e && e.data && e.data.error) || (e && e.message) || String(e) }; }
    if (!r || r.error) return no(`Order ${rid} was not tried again: ${String((r && r.error) || "the cloud did not answer").replace(/\.$/, "")}. Nothing was changed`);
    toast(`Order ${rid}: the cloud tries its take-off again`, "ok", 6000);
    safe(() => root.CharmNestRunFollow && root.CharmNestRunFollow.now("retry"));
    if (o.key) follow(rid, String(o.key), o.words || offWords(rid), { seenAt: o.seenAt, by: name });
    return { ok: true, askId: r.askId };
  }
  /** The cloud's take-off job `key` of order rid, followed every 5 s (at most 20 min) until it ends, and said once: done, held or stuck (with Try again), or still at it.
      seenAt: the job's updatedAt when Try again was pressed: held or stuck as it was then is not said again (the cloud has not taken the press yet). */
  function follow(rid, key, words, o) {
    const t0 = Date.now(), seenAt = o && o.seenAt != null ? +o.seenAt : null, by = String((o && o.by) || "");   // (by: who pressed; Try again goes under whoever is signed in then, else them)
    const next = () => setTimeout(() => { look().catch(() => next()); }, CANCEL_FOLLOW_MS);
    const look = async () => {
      let g = null; try { g = await api({ op: "takeOffGet", orderIds: [rid] }); } catch (_) { g = null; }
      const job = g && g.journals && g.journals[rid] && g.journals[rid].jobs && g.journals[rid].jobs[key];
      const old = !!job && seenAt != null && +job.updatedAt === seenAt;
      if (job && job.status === "done") { const [t, tone, ms] = words.done(job); toast(t, tone, ms); return; }
      if (job && !old && ["held", "stuck"].includes(job.status)) { const at = job.updatedAt; toastAct(words.held(job), "bad", 12000, "Try again", () => retry(rid, { key, words, seenAt: at, by: who() || by })); return; }
      if (job && job.status === "restored") return;   // (not cancelled any more: whoever restored it was told)
      if (Date.now() - t0 > CANCEL_FOLLOW_MAX_MS) { if (job) toast(words.late(job), "bad", 9000); return; }
      next();
    };
    next();
  }
  /** (S5, Paul 21 Nov 2026: "Set aside" on a cancelled order's notice in the run pill, pressed on a staff computer, wrote its steps with production ops the server refuses in mode "on",
      and said nothing.) In mode "on" the press is takeOffAck { orderId, at, by }: the cloud takes the notice out of the shared store (every page's pill), records "set aside by <who>"
      on the cancel record's removals and the order's timeline, and marks its take-off job's notice seen. → { ok, removed } or { ok: false, error } (said in words). */
  async function ack(rid, at, o) {
    o = o || {}; rid = String(rid || "").replace(/\D/g, "");
    const name = String(o.by || who() || "").trim();
    const no = m => { toast(m, "bad", 9000); return { ok: false, error: m }; };
    if (!rid) return no("Pick an order");
    if (!name) return no(`Order ${rid}: sign in first, then press Set aside (it is saved under your name)`);
    let r = null; try { r = await api({ op: "takeOffAck", orderId: rid, ...(+at > 0 ? { at: Math.round(+at) } : {}), by: name, dev: dev() }); } catch (e) { r = { error: (e && e.data && e.data.error) || (e && e.message) || String(e) }; }
    if (!r || r.error || !r.ok) return no(`Order ${rid}: not set aside: ${String((r && r.error) || "the cloud did not answer").replace(/\.$/, "")}. Nothing was changed`);
    // (the cloud found no such notice: it was set aside already, on another screen, and is gone from every pill: nothing more is recorded, and that is said)
    if (!(+r.removed > 0)) { toast(`Order ${rid}: this notice was already set aside on another screen; nothing more was recorded`, "ok", 7000); return { ok: true, removed: 0, already: true }; }
    toast(`Order ${rid}: its pieces are set aside, by ${name}`, "ok", 5000);
    return { ok: true, removed: +r.removed };
  }
  /* (ITEM 1, Paul 21 Nov 2026: Cancel Order on an order with pieces on a sheet, pressed on a staff computer in mode "on", only said "its cancel is recorded" and recorded nothing: the order
     stayed open and its pieces stayed on the sheet.) The cancel is now recorded exactly as this page's own path records it (cancelPut { orderId, by, why, at: when pressed, record: the order },
     o.cancelRec, built by the sheet window's cancelRecOf), and nothing is taken off here: the cloud's take-off lane reads every cancel written since its cursor and takes the pieces off
     (cancelPut wakes the driver). The person is told in words: cancelled, then what the cloud did (its take-off journal, takeOffGet, followed every 5 s for at most 20 min), or refused with
     the reason. Answers in SheetWin.takeOffOrder's own shape. */
  const CANCEL_FOLLOW_MS = 5000, CANCEL_FOLLOW_MAX_MS = 20 * 60000;
  async function cancel(o) {
    const rid = String(o.orderId || o.rid || "").replace(/\D/g, ""), rec = Object.assign({}, o.cancelRec || {}), name = String(rec.by || o.by || who() || "").trim();
    const no = m => { toast(m, "bad", 9000); return { ok: false, error: m, mode: "cancel", scope: "order", removed: [], stayed: [] }; };
    if (!rid) return no("Pick an order");
    if (!name) return no(`Order ${rid} was not cancelled: sign in first, the cancel is saved under your name`);
    if (!lane("takeoff")) return no(`Order ${rid} was not cancelled: the cloud does not take orders off sheets in this build. Nothing was changed`);
    Object.assign(rec, { orderId: rid, by: name }); if (rec.why == null) rec.why = String(o.note || ""); if (!(+rec.at > 0)) rec.at = Date.now();
    let r = null;
    try { r = root.Cancelled && typeof root.Cancelled.put === "function" ? await root.Cancelled.put(rec) : await api(Object.assign({ op: "cancelPut" }, rec)); }   // (Cancelled.put: the page's own writer, so Orders › Cancelled shows it at once)
    catch (e) { r = { error: (e && e.data && e.data.error) || (e && e.message) || String(e) }; }
    if (!r || r.error || !r.ok) return no(`Order ${rid} was not cancelled: ${String((r && r.error) || "the cloud did not answer").replace(/\.$/, "")}. Nothing was changed`);
    const at = Math.round(+(r.record && r.record.at) || +rec.at);
    toast(`Order ${rid} cancelled: its record is under Orders › Cancelled, and the cloud takes any of its pieces still on a sheet off`, "ok", 7000);
    safe(() => root.CharmNestRunFollow && root.CharmNestRunFollow.now("cancelled"));
    cancelFollow(rid, at, name);
    return { ok: true, mode: "cancel", scope: "order", removed: [], stayed: [], queued: true, via: "cloud", at };
  }
  /** The cloud's take-off of this cancel (its journal job cancel-<at>), followed until it ends: said once, in words; held or stuck with its Try again (S5). A cancel whose order has
      nothing on a sheet makes no job: nothing more is said. */
  function cancelFollow(rid, at, by) { follow(rid, `cancel-${at}`, cancelWords(rid), { by }); }

  /* ── THE FILES LANE ON A PAGE (cloud plan v2, build step 7b; P9: one lane, one place; P1). DORMANT: while the drive is off nothing here reads or sends anything, and the page's sheet
     file writers (mcFiles, mcFilesResume, mcWriteStandalone in charm-nest-sheetwin.js, writeAsPlaced in charm-nest-1.html) never ask it: they write exactly as today. While the cloud writes
     a run's sheet files (mode "on"; or "files" is one of the drive's lanes and Charm_Nest_Run_Carrier says the cloud carries the sheet's run) they send ONE press instead, finishSheet
     { id, reason, by }, write no file, and the person is told in words: asked, then done (the sheet's record no longer owes its files: finishGet followed every 5 s, at most 20 min),
     or refused with the reason (the cloud's answer, or its writer's last job). ── */
  const FILES_FOLLOW_MS = 5000, FILES_FOLLOW_MAX_MS = 20 * 60000, filesFollow = new Map();
  const sheetName = sh => String((sh && (sh.fileBase || sh.metalLabel || sh.sheetId || sh.id)) || "This sheet");
  /** Whether the cloud writes this sheet's files now (never in mode off; no read then). */
  async function filesLane(sh) {
    if (S.mode === "off" || !sh) return false;
    if (on()) return true;
    if (!steps().includes("files")) return false;
    const runId = Object.prototype.hasOwnProperty.call(sh, "runId") ? sh.runId : followedRun(); if (!runId) return false;
    const c = await carrierOf(runId).catch(() => null);
    return !!(c && c.dev === "cloud");
  }
  /** The gate the page's sheet file writers ask, only when the mode is not off. → Promise: null (this page writes, as today) | true (the cloud was asked, the person told) | false (refused,
   *  the person told why; nothing was written here). o: { reason, jobK (an outbox file job the cloud now owns: settled once asked), again } */
  async function filesGate(sh, o) {
    if (S.mode === "off" || !sh) return null;
    try {
      if (!on() && !(Date.now() - (+S.at || 0) < 60000)) await read("files").catch(() => {});   // (the switch as it is now: a lane turned on a minute ago is the cloud's)
      if (!(await filesLane(sh))) return null;
    } catch (_) { return null; }
    return filesAsk(sh, o);
  }
  /** The press: finishSheet { id, reason, by }. → true (asked) | false (refused). Said in words either way, then followed to done. */
  async function filesAsk(sh, o) {
    o = o || {};
    const id = sh && (sh.sheetId || sh.id), name = sheetName(sh), reason = String(o.reason || "asked from this page").slice(0, 120);
    try { if (sh && typeof sh === "object") sh._filesCloudSaid = Date.now(); } catch (_) { /* a frozen copy: nothing to mark */ }
    if (!id) { toast(`${name}: its files are the cloud's to write, and this sheet is not saved in the cloud yet: nothing was written here, and the cloud writes them once it is saved`, "bad", 9000); return false; }
    let r = null;
    try { r = await api({ op: "finishSheet", id, reason, by: who() || "", ...(o.again === true ? { again: true } : {}) }); } catch (e) { r = { error: (e && e.data && e.data.error) || (e && e.message) || String(e) }; }
    if (!r || r.error || !r.ok) { toast(`${name}: refused — the cloud did not take its files (${String((r && r.error) || "no answer").replace(/\.$/, "")}). Nothing was written on this computer`, "bad", 11000); return false; }
    toast(`${name}: asked — the cloud writes its files (${reason})`, "ok", 5000);
    if (o.jobK) safe(() => root.CharmNestStore.settle(o.jobK, { ok: true, answer: { files: "cloud" } }));
    filesFollowStart(id, name, Date.now());
    return true;
  }
  /** Followed until the cloud's record no longer owes the files (done), or its writer's last job, newer than the press, ended without them (refused). */
  function filesFollowStart(id, name, at) {
    if (filesFollow.has(id)) { filesFollow.get(id).at = Math.min(filesFollow.get(id).at, at); return; }
    const f = { at, t0: Date.now() }; filesFollow.set(id, f);
    const next = () => setTimeout(() => { look().catch(() => next()); }, FILES_FOLLOW_MS);
    const look = async () => {
      if (Date.now() - f.t0 > FILES_FOLLOW_MAX_MS) { filesFollow.delete(id); toast(`${name}: the cloud has not finished its files in 20 minutes; they are still owed, and the cloud keeps at them`, "bad", 9000); return; }
      let g = null; try { g = await api({ op: "finishGet", id }); } catch (_) { g = null; }
      const sheet = g && g.sheet, ld = g && g.task && g.task.lastDone;
      if (sheet && !(+sheet.filesOwed > 0) && !sheet.filesJob) { filesFollow.delete(id); toast(`${name}: done — the cloud wrote its files`, "ok", 5000); return; }
      if (ld && (+ld.at || 0) >= f.at && ["failed", "skipped"].includes(ld.outcome)) { filesFollow.delete(id); toast(`${name}: refused — the cloud did not write its files: ${String(ld.why || ld.outcome).replace(/\.$/, "")}`, "bad", 11000); return; }
      next();
    };
    next();
  }

  /* ── S3 (Paul 21 Nov 2026): artwork added by hand, and a hand piece's Exclude / Include / Move to <metal>, on a staff computer in mode "on" ──
     A dropped or chosen .ai / .pdf / .zip used to be put on this page's newest sheet (charm-nest-1.html flushManualIntake) and "nested in the cloud": the cloud nested ITS record
     of that sheet, without the new pieces, and this page could save neither the files nor the pieces (a viewer saves no sheet). The charm window's Exclude / Include and Move to
     <metal> of a piece of no order changed only this page's memory. Now:
       handArt(metal, charms, o): each design of the file (one per geometry hash; its copies are its quantity) is saved as its own one-charm file in the cloud's storage
         (charmnest/charms/<hash>.ai, the path the page's own charm backup writes: o.fileOf builds it, o.upload saves it), recorded as a piece of no order the cloud places
         (customPiecePut { cloudPlace, hand }: numbered CUSTOM-<day>-<n>, on the run the cloud carries) and ONE request asks the cloud to place them (runCmdPut placewaiting
         { ids }), followed until the cloud answers: → { ok, text: the cloud's words, error, pending?, poolIds }. Nothing is put on a sheet here, nothing is nested here.
       charmPress(sh, c, change): sheetLayoutPatch { id, by, charms: { <pool id or charm id>: { excluded } | { metal } } } (charmNestLibrary.js sheetCharmPatch): saved on the sheet's
         record, honoured by the cloud's nest, shown on every computer; a move is placed by the cloud on a sheet of the new metal (its request is queued by the same press and
         followed here). Said in words either way: done, or refused with the reason, nothing changed.
     Mode off: neither is called (the page's own intake and charm window, unchanged). */
  const HAND_FOLLOW_MS = 600000, M_LABEL = { gold: "Gold Filled", silver: "Sterling Silver", rose: "Rose Gold", gold10k: "10K Gold", gold14k: "14K Gold" };
  const nap = ms => new Promise(r => setTimeout(r, ms));
  const errOf = e => String((e && e.data && e.data.error) || (e && e.message) || e || "no answer").replace(/\.$/, "");
  const devOr = () => dev() || "dviewer";
  /** A person's request in the run's queue, followed until the cloud answers it. → { ok, text } | { ok: false, pending: true, text } (not answered in o.maxMs) */
  async function cmdFollow(runId, id, o) {
    o = o || {};
    /* NOWCMD (Paul 21 Nov 2026): followed by the page's one follower (charm-nest-cmd-now.js) until the cloud answers: its live step, its words; a failure in red with a toast (and Try again
       when the caller can send it again: o.resend), and 5 minutes with no answer said plainly in red, still followed in the background (pending: true, later: its answer when it comes) */
    const N = root.CNCmdNow;
    if (N && typeof N.follow === "function") {
      const f = await N.follow(runId, id, { api, onStep: t => safe(() => o.onStep && o.onStep(t)), gapMs: o.gapMs, maxMs: Math.min(+o.maxMs > 0 ? +o.maxMs : N.LATE_MS, N.LATE_MS), label: o.label, el: o.el, resend: o.resend, toast: o.toast });
      if (f.ok) return { ok: true, text: String(f.text || "Placed by the cloud") };
      if (f.late) return { ok: false, pending: true, text: f.text, later: f.later, retry: f.retry };
      return { ok: false, text: `the cloud did not place it: ${String(f.text || "not done").replace(/\.$/, "")}`, retry: f.retry, shown: true };
    }
    const t0 = Date.now(), gap = +o.gapMs > 0 ? +o.gapMs : 2500, max = +o.maxMs > 0 ? +o.maxMs : HAND_FOLLOW_MS;
    while (Date.now() - t0 < max) {
      await nap(gap);
      let list = null; try { list = await api({ op: "runCmdList", runId }); } catch (_) { continue; }
      const c = ((list && list.cmds) || []).find(x => x && x.id === id); if (!c) continue;
      if (c.state === "done") { safe(() => root.CharmNestRunFollow && root.CharmNestRunFollow.now("s3")); return { ok: true, text: String(c.text || "Placed by the cloud") }; }
      if (c.state === "failed" || c.state === "cancelled") return { ok: false, text: `the cloud did not place it: ${String(c.text || c.state).replace(/\.$/, "")}` };
      if (c.step) safe(() => o.onStep && o.onStep(`The cloud: ${c.step}`));
    }
    return { ok: false, pending: true, text: `the cloud has not answered in ${Math.round(max / 60000) || 1} minute(s); the request stays queued and is done when the cloud gets to it` };
  }
  async function handArt(metal, charms, o) {
    o = o || {};
    const list = (charms || []).filter(Boolean), say = t => safe(() => o.onStep && o.onStep(t));
    const fail = m => ({ ok: false, error: m, poolIds: [] });
    if (!list.length) return fail("no artwork to send");
    if (!M_LABEL[metal]) return fail(`pick the sheet's metal for it. Nothing was sent`);
    const by = who(); if (!by) return fail("nobody is signed in: sign in first, the artwork is recorded under your name. Nothing was sent");
    const runId = followedRun(); if (!runId) return fail("no run is open in the cloud: start the run, then add the artwork again. Nothing was sent");
    if (!(lane("cmd:placewaiting") && lane("nest"))) return fail("the cloud does not place added artwork yet (the owner turns on “cmd:placewaiting” and “nest” in Settings, Cloud carries). Nothing was sent");
    if (typeof o.fileOf !== "function" || typeof o.upload !== "function") return fail("the page is not ready: reload it, then add the artwork again. Nothing was sent");
    const groups = new Map(); for (const c of list) { const k = String(c.hash || c.id); if (!groups.has(k)) groups.set(k, []); groups.get(k).push(c); }
    const MM = 25.4 / 72, poolIds = [], names = [], errs = []; let gi = 0;
    for (const cs of groups.values()) {
      const c = cs[0], hash = String(c.hash || "").replace(/[^\w-]/g, ""), name = String(c.name || `${String(c.sourceName || "artwork").replace(/\.(ai|pdf)$/i, "")}-${String((+c.index || 0) + 1).padStart(2, "0")}`).slice(0, 80);
      if (!/^[\w-]{6,64}$/.test(hash) || !(+c.widthPt > 0 && +c.heightPt > 0 && +c.areaPt2 > 0)) { errs.push(`${name}: its outline was not traced, so the cloud cannot place it`); continue; }
      say(`Saving ${name} in the cloud (${++gi} of ${groups.size})…`);
      let path = c.cloud && c.cloud.aiPath ? String(c.cloud.aiPath) : "";
      if (!path) { try { const up = await o.upload(`charmnest/charms/${hash}.ai`, await o.fileOf(c)); path = (up && up.path) || ""; if (!path) throw new Error("the cloud gave no path"); } catch (e) { errs.push(`${name}: its file could not be saved in the cloud (${errOf(e)})`); continue; } }
      for (let left = cs.length; left > 0;) {
        const q = Math.min(99, left); left -= q;
        const body = { op: "customPiecePut", requestId: "hd" + Date.now().toString(36) + Math.random().toString(36).slice(2, 8), by, dev: devOr(), piece: { designId: "hand-" + hash, version: 0, designHash: null, geomHash: hash, customName: name, metal, quantity: q, orderId: null,
          widthMm: +(c.widthPt * MM).toFixed(3), heightMm: +(c.heightPt * MM).toFixed(3), aiPath: path, charmHash: c.hash || null, runId, day: o.day || null, cloudPlace: true, hand: true, widthPt: +c.widthPt, heightPt: +c.heightPt, areaPt2: +c.areaPt2 } };
        let r = null; try { r = await api(body); } catch (e) { if (e && (e.transient || +e.status >= 500)) { try { r = await api(body); } catch (e2) { r = { error: errOf(e2) }; } } else r = { error: errOf(e) }; }   // (the same request again: the server gives it the same number)
        if (!r || !r.ok || !Array.isArray(r.poolIds) || !r.poolIds.length) { errs.push(`${name}: not recorded in the cloud (${String((r && r.error) || "no answer").replace(/\.$/, "")})`); continue; }
        poolIds.push(...r.poolIds); names.push(name);
      }
    }
    if (!poolIds.length) return { ok: false, error: (errs.join("; ") || "nothing was recorded") + ". Nothing was sent", poolIds };
    const texts = []; let pending = false;
    for (let i = 0; i < poolIds.length; i += 80) {
      const ids = poolIds.slice(i, i + 80);
      let put = null; try { put = await api({ op: "runCmdPut", runId, dev: devOr(), by, cmd: { type: "placewaiting", ids, label: `Place the artwork added by hand: ${names[0]}${names.length > 1 ? ` and ${names.length - 1} more` : ""}`.slice(0, 150) } }); } catch (e) { put = { error: errOf(e) }; }
      if (!put || !put.ok || !put.id) { errs.push(`${ids.length} piece(s) are recorded in the cloud (${ids[0]}…) but the cloud was not asked to place them: ${String((put && put.error) || "no answer").replace(/\.$/, "")}`); continue; }
      say("Asked: the cloud places the artwork…");
      const putAgain = async () => { let p2 = null; try { p2 = await api({ op: "runCmdPut", runId, dev: devOr(), by, cmd: { type: "placewaiting", ids, label: `Place the artwork added by hand: ${names[0]}`.slice(0, 150) } }); } catch (e) { p2 = { error: errOf(e) }; } return p2 && p2.id ? { runId, id: p2.id } : { error: (p2 && p2.error) || "no answer" }; };   // (NOWCMD: Try again sends the same request)
      const f = await cmdFollow(runId, put.id, { onStep: o.onStep, gapMs: o.followMs, maxMs: o.maxMs, label: "Artwork added by hand", resend: putAgain });
      if (f.ok) texts.push(f.text); else if (f.pending) { pending = true; texts.push(f.text); } else errs.push(f.text);
    }
    return { ok: !!texts.length && !errs.length && !pending, pending, text: texts.join(" · "), error: errs.join(" · "), poolIds, runId };
  }
  async function charmPress(sh, c, change, o) {
    o = o || {}; change = change || {};
    const name = String((c && c.name) || "This charm"), by = who(), shName = sheetName(sh), to = M_LABEL[change.metal] || change.metal;
    const verb = change.metal ? `moved to ${to}` : change.excluded ? "excluded" : "included";
    const no = m => { toast(`${name}: not ${verb} — ${String(m).replace(/\.$/, "")}`, "bad", 10000); return { ok: false, error: m }; };
    if (!by) return no("sign in first: the change is saved under your name. Nothing was changed");
    if (!sh || !sh.sheetId) return no("this sheet is not in the cloud. Nothing was changed");
    const key = String((c && (c.poolId || c.id)) || ""); if (!key) return no("the piece has no name the cloud knows. Nothing was changed");
    if (change.metal && !(lane("cmd:placewaiting") && lane("nest"))) return no("the cloud does not place moved pieces yet (the owner turns on “cmd:placewaiting” and “nest” in Settings, Cloud carries). Nothing was changed");
    let r = null; try { r = await api({ op: "sheetLayoutPatch", id: sh.sheetId, by, dev: devOr(), charms: { [key]: change.metal ? { metal: String(change.metal) } : { excluded: !!change.excluded } } }); } catch (e) { r = { error: errOf(e) }; }
    if (!r || !r.ok) return no((r && r.error) || "the cloud did not answer. Nothing was changed");
    safe(() => root.CharmNestRunFollow && root.CharmNestRunFollow.now("s3"));
    if (!change.metal) { toast(change.excluded ? `${name}: excluded — saved in the cloud; the cloud nests ${shName} again without it` : `${name}: included — saved in the cloud; the cloud places it on ${shName} when it nests it`, "ok", 7000); return { ok: true, r }; }
    if (r.cmdError || !r.cmdId) { toast(`${name}: taken off ${shName}, but the cloud was not asked to place it on ${to}: ${String(r.cmdError || "no answer").replace(/\.$/, "")}. Move it again from Orders, or ask for help`, "bad", 12000); return { ok: false, r, error: r.cmdError || "no request" }; }
    toast(`${name}: taken off ${shName} — the cloud places it on ${to}…`, "ok", 5000);
    const f = cmdFollow(r.runId || followedRun(), r.cmdId, { gapMs: o.followMs, maxMs: o.maxMs, label: name }).then(x => { if (x.ok) toast(`${name}: ${x.text}`, "ok", 7000); else if (!root.CNCmdNow) toast(`${name}: ${x.text}`, "bad", 12000); return x; });   // (NOWCMD: a failure or a late answer is said by the follower, in red)
    if (o.wait) return Object.assign({ ok: true, r }, { follow: await f });
    return { ok: true, r, follow: f };
  }
  /* ── S2 (Paul 21 Nov 2026: "staff keep FULL use of every input, saved in the cloud, seen on every computer"): REVIEW ON A VIEWER ──
     1 · "Accept the new order" (an Order changed card, chg:<line>): the page's acceptChange made the line up again here (repool); on a viewer that is the cloud's work. It is now a
         person's press the cloud acts on: takeOffAsk { kind: changed, lineKeys } (the take-off lane, _charmNestTakeOff.js: the line's old pieces come off and it is made up again as the
         order reads now, or, when what it is made of did not change, it only stops waiting). Refused in words while the cloud's take-off lane is not on; nothing is done here.
     2 · Resolved: the decisions a person answered (Review.settled) came back from this browser's workspace, which a viewer does not keep. Each answer given here is sent to the cloud
         (reviewSettledPut), and the list Resolved draws is the cloud's (reviewSettledGet: at the start, when the feed names g:review-settled, when the tab is shown again, every
         ten minutes as a net), with the answers given here that the cloud has not confirmed yet. Mode "off": nothing here runs. */
  async function acceptChange(o) {
    o = o || {};
    const rid = String(o.rid || "").replace(/\D/g, ""), keys = [...new Set((Array.isArray(o.lineKeys) ? o.lineKeys : []).map(String).filter(k => rid && k.startsWith(rid + "_")))].slice(0, 50);
    const no = m => { toast(m, "bad", 9000); return { ok: false, error: m }; };
    if (!rid) return no("Which order? Nothing was changed");
    if (!lane("takeoff")) return no(`Order ${rid}: Accept the new order is not in the cloud yet (the cloud does not take pieces off in this build). Nothing was changed`);
    const name = String(o.name || who() || "").trim(); if (!name) return no("Sign in first: the answer is saved under your name. Nothing was changed");
    let r = null; try { r = await api({ op: "takeOffAsk", orderId: rid, kind: "changed", lineKeys: keys, note: "Accept the new order", by: name, dev: dev() }); } catch (e) { r = { error: (e && e.data && e.data.error) || (e && e.message) || String(e) }; }
    if (!r || r.error || !r.ok) return no(`Order ${rid}: not accepted — ${String((r && r.error) || "the cloud did not answer").replace(/\.$/, "")}. Nothing was changed`);
    toast(`Order ${rid}: accepted — the cloud makes ${keys.length > 1 ? "its pieces" : "the piece"} again as the order reads now`, "ok", 6000);
    safe(() => root.CharmNestRunFollow && root.CharmNestRunFollow.now("changed"));
    return { ok: true, queued: true, rid, askId: r.askId, via: "cloud" };
  }
  const SETTLED_NAME = "review-settled", SETTLED_FEED = "g:" + SETTLED_NAME, SETTLED_SHOWN = 200, SETTLED_TRIES = 4;
  const ST = { list: [], at: 0, reads: 0, puts: 0, fails: 0, pending: new Map(), started: false, reading: null, again: false, error: null };
  const sidOf = d => `${d && d.key}\u0001${d && d.t}`;
  /** The record as the cloud keeps it (charm-nest-review-core.js Settled, with the answer in words). */
  const settledRec = d => ({ key: String(d.key || ""), kind: String(d.kind || ""), why: String(d.why || "").slice(0, 300), answer: String(d.answer || "").slice(0, 300), by: String(d.by || "").slice(0, 80), t: +d.t || Date.now(),
    orders: (d.orders || []).map(String).slice(0, 20), lines: +d.lines || 0, pieces: +d.pieces || 0, rowKey: String(d.rowKey || (d.row && d.row.key) || ""), ...(d.how ? { how: String(d.how) } : {}) });
  /** An answer given on this page: to the cloud (tried again a few times; until then it stays in the list as this page's). */
  function settledPut(d) {
    if (!on() || !d || !d.key) return false;
    const id = sidOf(d); d.pendingCloud = true; ST.pending.set(id, d);
    let n = 0;
    const go = async () => {
      n++;
      let r = null; try { r = await api({ op: "reviewSettledPut", rec: settledRec(d), by: who() || d.by || "", dev: dev() }); } catch (e) { r = { error: (e && e.message) || String(e) }; }
      if (r && r.ok) { ST.puts++; ST.pending.delete(id); d.pendingCloud = false; d.cloud = true; return; }
      ST.fails++; ST.error = String((r && r.error) || "no answer").slice(0, 200);
      if (n < SETTLED_TRIES) setTimeout(() => go().catch(() => {}), 3000 * n);
      else toast(`The answer on order ${(d.orders || [])[0] || ""} was not saved in the cloud (${ST.error}): other computers do not list it under Resolved`, "bad", 9000);
    };
    go().catch(() => {});
    return true;
  }
  /** The cloud's list put into Review.settled (the same objects kept for the same record, so a card drawn is not drawn again), with the answers given here not confirmed yet. */
  function settledApply() {
    const R = root.Review; if (!on() || !R || typeof R.settled !== "function") return null;
    const arr = R.settled(); if (!Array.isArray(arr)) return null;
    const have = new Map(arr.map(d => [sidOf(d), d])), byKey = safe(() => root.B.orders.byKey, null), next = [];
    for (const c of ST.list) {
      const id = sidOf(c); let d = have.get(id) || ST.pending.get(id);
      if (!d) d = Object.assign({}, c, { row: null });
      else for (const f of ["kind", "why", "answer", "by", "orders", "lines", "pieces", "rowKey"]) if (c[f] !== undefined && c[f] !== "") d[f] = c[f];
      if (!d.row && c.rowKey && byKey && typeof byKey.get === "function") d.row = byKey.get(c.rowKey) || null;   // (the line still in the pull: its picture and its order)
      if (!d.pieces) d.pieces = d.lines || 0;
      d.cloud = true; next.push(d); have.delete(id); ST.pending.delete(id);
    }
    for (const d of ST.pending.values()) if (!next.includes(d)) next.push(d);
    next.sort((a, b) => (+b.t || 0) - (+a.t || 0));
    const same = next.length === arr.length && next.every((d, i) => d === arr[i]);
    if (!same) { arr.splice(0, arr.length, ...next.slice(0, SETTLED_SHOWN)); safe(() => R.render()); }
    return { size: arr.length, changed: !same };
  }
  async function settledPull(why) {
    if (!on()) return null;
    if (ST.reading) { ST.again = true; return ST.reading; }
    ST.reading = (async () => {
      try {
        const r = await api({ op: "reviewSettledGet" });
        if (!r || r.error || !Array.isArray(r.settled)) throw new Error((r && r.error) || "no answer");
        ST.list = r.settled.filter(x => x && x.key); ST.at = Date.now(); ST.reads++; ST.error = null;
        return settledApply();
      } catch (e) { ST.fails++; ST.error = String((e && e.message) || e).slice(0, 200); return null; }
      finally { ST.reading = null; if (ST.again) { ST.again = false; setTimeout(() => settledPull("again").catch(() => {}), 300); } }
    })();
    void why;
    return ST.reading;
  }
  function settledStart() {
    if (ST.started || !on()) return; ST.started = true;
    settledPull("start").catch(() => {});
    safe(() => { if (root.CNSync && typeof root.CNSync.follow === "function") root.CNSync.follow({ name: "review-settled", kinds: "g", wild: "g", active: () => on(), wants: k => String(k) === SETTLED_FEED, read: () => settledPull("feed").catch(() => {}) }); });
    safe(() => root.document.addEventListener("visibilitychange", () => { if (!root.document.hidden) settledPull("shown").catch(() => {}); }));
    // (the lines arrive from the cloud world after the list: a record whose line was not here yet gets its picture and its order when they come)
    safe(() => root.addEventListener("cn:world", () => { if (ST.list.length && R0().some(d => !d.row && d.rowKey)) settledApply(); }));
    setInterval(() => { if (!(root.document && root.document.hidden) && !safe(() => root.CNSync.live(), false)) settledPull("net").catch(() => {}); }, 600000);
  }
  const R0 = () => safe(() => root.Review.settled(), []) || [];
  readyP.then(() => { if (on()) setTimeout(settledStart, 0); });

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
  function asViewer() {
    const OH = root.OrderHold; if (!OH) return;
    if (typeof OH.release === "function" && !OH.release._viewer) S.ownRelease = OH.release;   // (ITEM 8: the page's own release, kept for an order this page holds)
    OH.release = Object.assign((rid, o) => release(rid, o), { _viewer: true });
    OH.run = (rid, o) => hold(rid, o);
    OH.plan = rid => holdPlan(rid);
    OH.resumeHolds = async () => 0;
  }
  readyP.then(() => { if (on()) asViewer(); });

  /* (VIEWAPPROVE 2, Paul 21 Nov 2026: "check if anything else is restricted from other computers"): the presses whose cloud path was built in CLOUD STEP 10 (a person's Cut Sheet:
     roseCutAsk, charm-nest-rose-ui.js record; a set's QR label and files made again: labelsAsk, charm-nest-flow.js) ask window.CharmNestCloudLanes(name, sheet or step), which this page
     build was to define and never did: on a viewer they ran here and the cloud refused them. It answers as lane() does, for the sheet's own run (the followed run when it names none). */
  root.CharmNestCloudLanes = (name, x) => safe(() => lane(String(name), (x && (x.runId || (x.sheet && x.sheet.runId))) || followedRun() || undefined), false);
  root.CNViewer = { mode, on, known, ready: () => readyP, boot, holdNew, draining: () => S.draining, cv: () => CV, lane, carrierOf, wait, pill, control, startRun, nest, release, hold, asViewer, holdPlan, takeOff, filesAsk, filesLane, filesGate, idleNow, read, keepSaves, keepResumes, handArt, charmPress, cmdFollow, acceptChange, settledPut, settledPull, settledApply, settledState: () => ({ size: ST.list.length, at: ST.at, reads: ST.reads, puts: ST.puts, fails: ST.fails, pending: ST.pending.size, error: ST.error }),
    nestOf, stopNest, sawSheet, nests: () => [...NESTS].map(([id, n]) => Object.assign({ sheetId: id }, n)),   // (S4)
    retry, ack, lastAct: () => S.lastAct || null,   // (S5: Try again on a take-off the cloud held or gave up on; Set aside on a cancelled order's notice)
    state: () => ({ known: S.known, assumed: S.assumed, mode: S.mode, at: S.at, fails: S.fails, draining: S.draining, idleAt: S.idleAt, idleWhy: S.idleWhy || "", cloudSteps: steps(), pending: (S.drive && S.drive.pending) || null }) };
  /* (ONSWITCH, Paul 20 Nov 2026) this page is a viewer: the computer's report says so (charm-nest-drive.js caps() → charm-nest-drift.js → Charm_Nest_Computers.drive), which is what
     "Cloud carries" reads for every computer open now. charm-nest-drive.js loads first in charm-nest-1.html; if it is not there yet, it is told as soon as it is (and its caps()
     also looks for window.CNViewer itself), so the order the two files load in never matters. */
  { let tries = 0; const tell = () => { const D = root.CharmNestDrive; if (D && typeof D.cap === "function") { safe(() => D.cap("viewer")); return; } if (++tries < 120) { const t = setTimeout(tell, 500); safe(() => t && typeof t.unref === "function" && t.unref()); } }; tell(); }
  // the first look as soon as the page can ask (CN.api exists once the page's script ran)
  const first = () => { if (root.CN && typeof root.CN.api === "function") read("start").catch(() => {}); else setTimeout(first, 300); };
  first();
})(typeof window !== "undefined" ? window : globalThis);
