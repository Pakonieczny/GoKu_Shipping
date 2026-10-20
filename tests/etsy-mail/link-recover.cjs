// LINKRECOVER (20 Oct 2026): "A message has waited 58 min for the inbox's Etsy helper to send it" and "8 new Etsy messages are waiting
// to be read" while the helper was heard every minute, with no way to clear it and nothing that recovered by itself.
// Offline: the real _etsyMailSendQueue / etsyMailDraftSend / _etsyMailLinkRecover / _etsyMailLinkHealth / etsyMailReapers / watchdog on
// the in-memory Firestore of tests/etsy-mail/_fakeFirestore.cjs, a virtual clock and the simulated extension of _queueHarness.cjs.
// Nothing reaches Etsy, Gmail, a customer or the network.
//
//   · the wedge itself: an old-style slot adopted over an EXPIRED holder stranded that holder "claimed" for ever, its slot "queued";
//     claimGate now refuses, and the books are made true again by the 3-minute upkeep by itself (and by the recovery);
//   · the recovery puts every message that was never clicked back on its way and NEVER re-sends one that may have gone;
//   · a send that heartbeats but never reaches Etsy's Send button is taken back, and its old tab can no longer click;
//   · the read side: a read the helper stops on every time is set aside after 3 recoveries instead of blocking the queue for ever,
//     a read that is still heartbeating is not taken away, and the recovery puts set-aside reads back;
//   · the run: one at a time, steps written as they finish, the line judged again; the link monitor starts it by itself, with back-off.
//
//   node tests/etsy-mail/link-recover.cjs
"use strict";
const path = require("path");
const { boot, reporter } = require("./_queueHarness.cjs");
const R = reporter(); const check = R.check; const say = s => process.stdout.write(s + "\n");
const fnDir = path.join(__dirname, "../../netlify/functions");
const MIN = 60000, HOUR = 60 * MIN;

(async () => {
  let h, t, r, ext;

  // ═══ 1. the wedge from the screenshot ═══
  say("The stranded holder: the helper is heard, a message waits for ever");
  h = boot({ seed: 41 });
  ext = new h.Ext("A");
  t = h.thread(801);
  r = await h.send(t, "Your charm ships tomorrow.");
  const X = r.body.sendId;
  check(h.item(X).state === "claimed" && h.slot(t).status === "queued" && h.lease().holderSendId === X, "(a message is handed to the helper: claimed, its slot says queued, it holds the lease)");
  h.clock.advance(3 * MIN);                                       // the helper did not claim it in time: the lease has expired, nobody has resolved it yet
  // an old-style slot (no queue id) in another conversation, which the extension tries to claim
  const t2 = h.thread(802);
  h.fake.poke("EtsyMail_Drafts/draft_" + t2, { draftId: "draft_" + t2, threadId: t2, status: "queued", text: "Old-style message", etsyConversationUrl: "https://www.etsy.com/your/conversations/802", queuedAt: new (require("./_fakeFirestore.cjs").Timestamp)(h.clock.now() - 5 * MIN) });
  const c = await h.call("claim", { draftId: "draft_" + t2, sessionId: "s_old", workerId: "A" });
  check(c.status === 409 && c.body.errorCode === "QUEUE_BUSY", "an old-style slot is NOT adopted over an expired holder any more (409 QUEUE_BUSY)");
  check(h.lease().holderSendId === X, "...so the expired holder is not overwritten and lost");

  // the state production was left in by the old adoption: X claimed, the lease on another (finished) message, X's slot still "queued"
  h.fake.poke("EtsyMail_SendQueueMeta/lease", Object.assign({}, h.lease(), { holderSendId: "q_gone", leaseUntilMs: h.clock.now() - 1000, holderThreadId: t2 }));
  h.fake.poke("EtsyMail_Drafts/draft_" + t2, Object.assign({}, h.slot(t2), { status: "draft" }));
  h.clock.advance(55 * MIN);
  const LH = require(path.join(fnDir, "_etsyMailLinkHealth.js"));
  const db = require(path.join(fnDir, "firebaseAdmin.js")).firestore();
  let res = LH.evaluate(await LH.gather(db), h.clock.now());
  const sendBefore = res.checks.find(x => x.id === "send");
  check(sendBefore.level === "down" && /waited/.test(sendBefore.text), "the light reproduces the report: Sending is red, 'a message has waited ...' (" + sendBefore.text.slice(0, 60) + ")");
  check(res.checks.find(x => x.id === "helper").level === "ok", "...while the helper is heard every minute");

  // the 3-minute upkeep repairs it by itself
  await h.Cron.handler({ headers: { "x-nf-event-source": "scheduled" } });
  check(h.item(X).state !== "claimed" || h.lease().holderSendId === X, "the 3-minute upkeep takes the stranded message back by itself (no button needed)");
  await ext.poll(); h.clock.advance(20000); await ext.poll(); h.clock.advance(20000);
  check(h.item(X).state === "confirmed", "...and the helper sends it: confirmed");
  check((h.sentToEtsy || []).filter(s => s.text === "Your charm ships tomorrow.").length === 1, "...exactly once on Etsy");
  res = LH.evaluate(await LH.gather(db), h.clock.now());
  check(res.checks.find(x => x.id === "send").level === "ok", "the light turns green again by itself");
  h.done();

  // ═══ 2. old-style slot is moved into the queue ═══
  say("An old-style 'queued' slot is taken into the queue and goes out");
  h = boot({ seed: 42 });
  ext = new h.Ext("A");
  t = h.thread(811);
  h.fake.poke("EtsyMail_Drafts/draft_" + t, { draftId: "draft_" + t, threadId: t, status: "queued", text: "Old-style hello", etsyConversationUrl: "https://www.etsy.com/your/conversations/811", sendOrigin: "manual", queuedAt: new (require("./_fakeFirestore.cjs").Timestamp)(h.clock.now() - 10 * MIN) });
  let rp = await h.Q.repair({ mode: "light" });
  check(rp.adopted === 1, "the recovery adopts it into the queue");
  await ext.poll(); h.clock.advance(20000);
  check((h.sentToEtsy || []).filter(s => s.text === "Old-style hello").length === 1, "...and it is sent, once");
  h.done();

  // ═══ 3. dead letters: never clicked -> sent; may have gone -> never re-sent ═══
  say("Dead letters: only what never reached Etsy's Send button goes again");
  h = boot({ seed: 43 });
  ext = new h.Ext("A");
  const ta = h.thread(821), tb = h.thread(822);
  const a = (await h.send(ta, "Never clicked, failed.")).body.sendId;
  const b = (await h.send(tb, "Clicked, not confirmed.")).body.sendId;
  h.fake.poke("EtsyMail_SendQueue/" + a, Object.assign({}, h.item(a), { state: "failed", open: true, stage: "pre_click", lastErrorCode: "MAX_ATTEMPTS", attempts: 4 }));
  h.fake.poke("EtsyMail_SendQueue/" + b, Object.assign({}, h.item(b), { state: "needs_attention", open: true, stage: "post_click", clickedAtMs: h.clock.now(), lastErrorCode: "STRANDED_POST_CLICK" }));
  h.fake.poke("EtsyMail_SendQueueMeta/lease", { leaseId: 5 });
  h.fake.poke("EtsyMail_Drafts/draft_" + ta, Object.assign({}, h.slot(ta), { status: "failed" }));
  h.fake.poke("EtsyMail_Drafts/draft_" + tb, Object.assign({}, h.slot(tb), { status: "sent_unverified" }));
  rp = await h.Q.repair({ mode: "manual", by: "Paul" });
  check(rp.revived === 1 && h.item(a).state !== "failed", "the failed message that never reached Send goes back in the queue");
  check(h.item(b).state === "needs_attention" && rp.attention >= 1, "the one that may have gone is NOT re-sent: a person decides");
  for (let i = 0; i < 4; i++) { await ext.poll(); h.clock.advance(20000); }
  check((h.sentToEtsy || []).filter(s => s.text === "Never clicked, failed.").length === 1, "...the revived one is sent once");
  check((h.sentToEtsy || []).filter(s => s.text === "Clicked, not confirmed.").length === 0, "...the maybe-sent one never goes to Etsy again");
  h.done();

  // ═══ 4. a tab that heartbeats for ever without reaching Send ═══
  say("A send that never reaches Etsy's Send button is taken back; its tab can no longer click");
  h = boot({ seed: 44 });
  t = h.thread(831);
  const s4 = (await h.send(t, "Stuck in a tab.")).body.sendId;
  const cl = await h.call("claim", { draftId: "draft_" + t, sessionId: "s_stuck", workerId: "A" });
  check(cl.status === 200 && h.item(s4).state === "sending", "(the helper claimed it: sending)");
  for (let i = 0; i < 80; i++) { h.clock.advance(5000); await h.call("heartbeat", { draftId: "draft_" + t, sessionId: "s_stuck" }); }
  check(h.item(s4).state === "sending" && h.lease().holderSendId === s4, "(after ~7 minutes of heartbeats it still holds the turn: nothing else could go)");
  rp = await h.Q.repair({ mode: "light" });
  check(rp.stuckSend === true && h.lease().holderSendId !== s4, "the recovery takes the turn back");
  const mc = await h.call("mark_clicked", { draftId: "draft_" + t, sessionId: "s_stuck" });
  check(mc.status !== 200, "the stuck tab's mark_clicked is refused (" + mc.status + "), so it can never click a second copy");
  ext = new h.Ext("B");
  for (let i = 0; i < 6; i++) { h.clock.advance(40000); await h.Cron.handler(); await ext.poll(); }   // (its retry waits out a short back-off; the upkeep hands it over)
  check((h.sentToEtsy || []).filter(s => s.text === "Stuck in a tab.").length === 1 && h.item(s4).state === "confirmed", "...a healthy tab sends it, once");
  h.done();

  // ═══ 5. the read side ═══
  say("Reads: a read the helper always stops on no longer blocks the others; a heartbeating read is left alone");
  h = boot({ seed: 45 });
  const { Timestamp } = require("./_fakeFirestore.cjs");
  const Reapers = require(path.join(fnDir, "etsyMailReapers.js"));
  const old = new Timestamp(h.clock.now() - 6 * MIN);
  h.fake.poke("EtsyMail_Jobs/gmail_poison", { jobId: "gmail_poison", jobType: "scrape", status: "claimed", threadId: "etsy_conv_900", attempts: 1, reaps: 3, claimedAt: old, createdAt: new Timestamp(h.clock.now() - HOUR), payload: { etsyConversationUrl: "https://www.etsy.com/your/conversations/900" }, lastError: "Reaped stuck claim after 300000ms (worker died?)" });
  h.fake.poke("EtsyMail_Jobs/gmail_long", { jobId: "gmail_long", jobType: "scrape", status: "claimed", threadId: "etsy_conv_901", attempts: 1, claimedAt: old, lastHeartbeatAt: new Timestamp(h.clock.now() - 20000), createdAt: new Timestamp(h.clock.now() - 10 * MIN), payload: { etsyConversationUrl: "https://www.etsy.com/your/conversations/901" } });
  h.fake.poke("EtsyMail_Jobs/gmail_new", { jobId: "gmail_new", jobType: "scrape", status: "claimed", threadId: "etsy_conv_902", attempts: 1, claimedAt: old, createdAt: new Timestamp(h.clock.now() - 8 * MIN), payload: { etsyConversationUrl: "https://www.etsy.com/your/conversations/902" } });
  await Reapers.reapStuckScrapeClaims();
  const job = id => h.fake.peek("EtsyMail_Jobs/" + id);
  check(job("gmail_poison").status === "failed" && /stopped 3 times/.test(job("gmail_poison").lastError), "a read reaped 3 times already is set aside (failed), instead of being claimed first for ever");
  check(job("gmail_long").status === "claimed", "a read whose helper still heartbeats is NOT taken away mid-read");
  check(job("gmail_new").status === "queued" && job("gmail_new").reaps === 1, "an ordinary stuck read is put back, its recoveries counted");
  let ev = await LH.gather(require(path.join(fnDir, "firebaseAdmin.js")).firestore());
  res = LH.evaluate(ev, h.clock.now());
  const rd = res.checks.find(x => x.id === "read");
  check(rd.level === "warn" && /set aside/.test(rd.text), "the light says a conversation was set aside (never silently): " + rd.text.slice(0, 70));
  h.done();

  // ═══ 6. the recovery run ═══
  say("The run: one at a time, each step reported, the line judged again");
  h = boot({ seed: 46 });
  const Rec = require(path.join(fnDir, "_etsyMailLinkRecover.js"));
  const fdb = require(path.join(fnDir, "firebaseAdmin.js")).firestore();
  h.fake.poke("EtsyMail_Config/gmailWatcher", { enabled: true });
  h.fake.poke("EtsyMail_Jobs/gmail_set_aside", { jobId: "gmail_set_aside", jobType: "scrape", status: "failed", reaps: 4, threadId: "etsy_conv_910", attempts: 1, updatedAt: new Timestamp(h.clock.now() - HOUR), createdAt: new Timestamp(h.clock.now() - 2 * HOUR), payload: { etsyConversationUrl: "https://www.etsy.com/your/conversations/910" }, lastError: "The Etsy helper stopped 3 times" });
  const kicks = [];
  const deps = {
    gmail: { getValidGmailAccessToken: async () => "tok", listMessages: async () => ({ messages: [] }) },
    kickGmail: async () => { kicks.push("gmail"); return { ok: true, status: 202 }; },
    orderLink: { reconcile: async () => ({ pulled: 2 }) },
    reapers: { reapStuckScrapeClaims: async () => ({ requeued: 0 }), reapDetectedThreadsWithoutLiveJobs: async () => ({ requeued: 0 }) },
    consistency: { recheck: async () => ({ ran: true, doc: { gaps: {} }, queued: 0 }) },
    sleep: async ms => { h.clock.advance(ms); }, watchMs: 30000,
    kick: async runId => { kicks.push(runId); return { ok: true, status: 202 }; }
  };
  const q1 = await Rec.request(fdb, { by: "Paul" }, deps);
  const q2 = await Rec.request(fdb, { by: "Anna" }, deps);
  check(q1.started && !q2.started && q2.run.runId === q1.run.runId, "a second press while a run goes joins it (one run at a time)");
  const out = await Rec.run(fdb, { runId: q1.run.runId }, deps);
  const st = await Rec.status(fdb);
  check(out.ok && st.state === "done" && st.steps.length >= 5, "the run finishes and lists its steps (" + st.steps.map(s => s.id + ":" + s.level).join(", ") + ")");
  check(kicks.includes("gmail"), "a Gmail pull was started (it resumes from the last email seen, so missed emails are pulled)");
  check(job("gmail_set_aside").status === "queued" && job("gmail_set_aside").attempts === 0, "the set-aside read is put back in line by a person's recovery");
  check(!!st.after && typeof st.summary === "string" && st.summary.length > 0, "the line is judged again and summed up: " + st.summary.slice(0, 80));
  ev = await LH.gather(fdb);
  res = LH.evaluate(ev, h.clock.now());
  check(res.checks.some(x => x.id === "recover" && x.level === "wait"), "the details show the last recovery, without it counting as a problem");
  h.done();

  // ═══ 7. by itself: the link monitor ═══
  say("The link monitor starts it by itself, and backs off while the same problem lasts");
  const RecP = require(path.join(fnDir, "_etsyMailLinkRecover.js"));
  const now = Date.UTC(2026, 9, 20, 16, 0, 0);
  const bad = { level: "down", incidentId: "1000", checks: [{ id: "send", level: "down", text: "A message has waited 58 min" }] };
  let d = RecP.shouldAuto(bad, null, now);
  check(d.go === true && d.n === 1, "a failing send starts a recovery at once");
  const rec1 = { state: "done", startedAtMs: now, autoIncident: "1000", autoCount: 1, autoAtMs: now };
  check(!RecP.shouldAuto(bad, rec1, now + 5 * MIN).go && RecP.shouldAuto(bad, rec1, now + 11 * MIN).go, "...then again after 10 minutes if it lasts");
  const rec3 = { state: "done", startedAtMs: now, autoIncident: "1000", autoCount: 3, autoAtMs: now };
  check(!RecP.shouldAuto(bad, rec3, now + 39 * MIN).go && RecP.shouldAuto(bad, rec3, now + 41 * MIN).go, "...the pause doubles (40 minutes after the 3rd)");
  const rec9 = { state: "done", startedAtMs: now, autoIncident: "1000", autoCount: 9, autoAtMs: now };
  check(RecP.shouldAuto(bad, rec9, now + 2 * HOUR + 1).go, "...and is never longer than 2 hours: it never gives up");
  check(!RecP.shouldAuto({ level: "warn", checks: [{ id: "helper", level: "warn" }, { id: "send", level: "wait" }] }, null, now).go, "an away helper (messages simply waiting) does not start one");
  check(!RecP.shouldAuto({ level: "down", checks: [{ id: "send", level: "down", text: "Sending is switched off in the inbox." }] }, null, now).go, "sending switched off by a person does not start one");
  check(!RecP.shouldAuto(bad, { state: "running", startedAtMs: now - MIN }, now).go, "a run already going is not doubled");

  h = boot({ seed: 47 });
  const WD = require(path.join(fnDir, "etsyMailLinkWatchdog.js"));
  const asked = [];
  const fakeRec = Object.assign({}, require(path.join(fnDir, "_etsyMailLinkRecover.js")), { request: async (dbx, o) => { asked.push(o); return { started: true, kicked: { ok: true } }; } });
  t = h.thread(841);
  await h.send(t, "Waiting while nobody takes it.");
  h.clock.advance(20 * MIN);
  const w = await WD.run({ now: h.clock.now(), reconcile: async () => ({}), recover: fakeRec });
  check(asked.length === 1 && asked[0].auto === true, "the 5-minute monitor started a recovery by itself (" + JSON.stringify(w.recover) + ")");
  h.done();

  R.finish("link-recover");
})().catch(e => { console.error(e); process.exit(1); });
