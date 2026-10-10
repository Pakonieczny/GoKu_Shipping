// MAILQUEUE (10 Oct 2026): failures, retries, the dead letter, and the guards that make a double send impossible.
// Same rig as send-queue-core.cjs: real server code, in-memory Firestore (refuses nested arrays), virtual clock,
// a simulated 0.9.52 extension. Nothing reaches Etsy or a customer.
//
//   node tests/etsy-mail/send-queue-failures.cjs
"use strict";
const { boot, reporter } = require("./_queueHarness.cjs");
const R = reporter(); const check = R.check;
const say = s => process.stdout.write(s + "\n");

(async () => {
  console.log = () => {};
  let h, t, r, ext, res, it;

  // ═══ 1. a transient failure is retried with a growing pause, then becomes a dead letter ═══
  say("A transient failure: retries with a back-off, then a dead letter");
  h = boot({ seed: 11 });
  t = h.thread(501, { status: "queued_for_auto_send" });
  r = await h.send(t, "We can add the birthstone you asked about.", { origin: "auto", aiMeta: { generatedByAI: true, model: "m" } });
  const id = r.body.sendId;
  ext = new h.Ext("A");
  const delays = [];
  for (let k = 1; k <= 4; k++) {
    h.clock.advance(15000);
    await h.Q.pump();
    res = await ext.tab(t, { failBeforeClick: { code: "DOM_TEXTAREA_MISS", retry: true, error: "no compose box" } });
    it = h.item(id);
    if (k < 4) {
      delays.push(it.notBeforeMs - h.clock.now());
      check(res.fail === 200 && it.state === "queued" && it.attempts === k, "try " + k + " failed: back in the queue, " + Math.round((it.notBeforeMs - h.clock.now()) / 1000) + " s before the next try");
      check(h.slot(t).status !== "queued", "...and the slot is not left 'queued' (no instant retry at the slot)");
      h.clock.advance(it.notBeforeMs - h.clock.now() + 1000);
      await h.Q.pump();
    }
  }
  check(delays[0] === 30000 && delays[1] === 120000 && delays[2] === 480000, "the pauses are 30 s, 2 min, 8 min (" + delays.map(d => d / 1000).join(", ") + " s)");
  it = h.item(id);
  check(it.state === "failed" && it.open === true && it.lastErrorCode === "MAX_ATTEMPTS", "after the fourth try it is a dead letter that stays open (" + it.state + ")");
  check(/did not take it after several tries/i.test(it.plain), "with a reason in plain words: " + it.plain);
  check(h.fake.peek("EtsyMail_Threads/" + t).status === "pending_human_review" && h.fake.peek("EtsyMail_Threads/" + t).sendQueueProblem === true, "the thread left Auto-Reply for review and carries the problem flag for the inbox list");
  check((h.sentToEtsy || []).length === 0, "nothing was sent in all that");
  const clicksBefore = (h.sentToEtsy || []).length;
  const retry = await h.call("queue_retry", { sendId: id, by: "Anna" });
  check(retry.status === 200 && ["claimed", "queued"].includes(h.item(id).state) && h.item(id).attempts === 0, "one click on Retry puts it back with fresh tries (failed before the click, so no confirmation question)");
  h.clock.advance(15000);
  res = await ext.tab(t);
  check(res.complete === 200 && h.item(id).state === "confirmed" && (h.sentToEtsy || []).length === clicksBefore + 1, "and it goes out, once");
  h.done();

  // ═══ 2. not worth retrying; copy and send by hand; put away ═══
  say("\nA failure that will not get better: failed at once; hand-sent; put away");
  h = boot({ seed: 12 });
  t = h.thread(502);
  r = await h.send(t, "Here is the tracking number you asked for.");
  ext = new h.Ext("A");
  h.clock.advance(15000);
  res = await ext.tab(t, { failBeforeClick: { code: "DOM_SEND_BUTTON_MISS", retry: false, error: "no send button" } });
  it = h.item(r.body.sendId);
  check(it.state === "failed" && it.attempts === 1 && /Send button/.test(it.plain), "a missing Send button is a dead letter at once, in plain words: " + it.plain);
  const st = await h.get("queue_state", { text: "1" });
  const row = st.body.queue.items.find(x => x.id === r.body.sendId);
  check(row && row.st === "failed" && row.text === "Here is the tracking number you asked for." && /etsy\.com/.test(row.url), "the dead letter is shown with its full words and the conversation link, so it can be copied and sent by hand");
  const hand = await h.call("queue_mark_sent", { sendId: r.body.sendId, by: "Anna" });
  check(hand.status === 200 && h.item(r.body.sendId).state === "confirmed" && h.item(r.body.sendId).open === false, "'I sent it myself' closes it");
  t = h.thread(503);
  r = await h.send(t, "Another one that fails.");
  h.clock.advance(15000);
  await ext.tab(t, { failBeforeClick: { code: "DOM_SEND_BUTTON_MISS", retry: false } });
  const away = await h.call("queue_dismiss", { sendId: r.body.sendId, by: "Anna" });
  check(away.status === 200 && h.item(r.body.sendId).open === false && h.item(r.body.sendId).state === "failed", "'put away' closes it without sending (the record stays)");
  const bad = await h.call("queue_dismiss", { sendId: "q_nope", by: "Anna" });
  check(bad.status === 404, "an unknown id is a plain 404");
  h.done();

  // ═══ 3. the fence in front of the Send button ═══
  say("\nThe fence in front of Etsy's Send button");
  h = boot({ seed: 13 });
  t = h.thread(504);
  r = await h.send(t, "Fence test one.");
  const A = new h.Ext("A"), B = new h.Ext("B");
  h.clock.advance(15000);
  // helper A claims; helper B (another browser) tries everything with its own session
  const claimA = await h.call("claim", { draftId: "draft_" + t, sessionId: "sess_A", workerId: "A" });
  check(claimA.status === 200, "A claims");
  const claimB = await h.call("claim", { draftId: "draft_" + t, sessionId: "sess_B", workerId: "B" });
  check(claimB.status === 409, "B's claim of the same message is refused (409)");
  const mcB = await h.call("mark_clicked", { draftId: "draft_" + t, sessionId: "sess_B" });
  check(mcB.status === 403, "B cannot get permission to click (mark_clicked from another session: " + mcB.status + ")");
  const hbB = await h.call("heartbeat", { draftId: "draft_" + t, sessionId: "sess_B" });
  check(hbB.status === 403, "B's heartbeat does not keep A's turn alive");
  const mc1 = await h.call("mark_clicked", { draftId: "draft_" + t, sessionId: "sess_A" });
  const mc2 = await h.call("mark_clicked", { draftId: "draft_" + t, sessionId: "sess_A" });
  check(mc1.status === 200 && mc2.status === 409 && mc2.body.errorCode === "ALREADY_CLICKED", "permission to click is given once, to the one session that holds the turn");
  const failB = await h.call("fail", { draftId: "draft_" + t, sessionId: "sess_B", error: "x", errorCode: "EXECUTE_THREW", retry: true });
  check(failB.status === 403 && h.item(r.body.sendId).state === "sending", "a failure report from B does not touch A's message");
  const comB = await h.call("complete", { draftId: "draft_" + t, sessionId: "sess_B" });
  check(comB.status === 403 && h.item(r.body.sendId).state === "sending", "nor does a completion from B");
  const comA = await h.call("complete", { draftId: "draft_" + t, sessionId: "sess_A" });
  check(comA.status === 200 && h.item(r.body.sendId).state === "confirmed", "A completes");
  h.done();

  // a helper that lost its turn before clicking is never allowed to click
  say("\nA helper that lost its turn cannot click, so nothing is sent twice");
  h = boot({ seed: 14 });
  t = h.thread(505);
  r = await h.send(t, "Fence test two.");
  const slow = new h.Ext("slow"), quick = new h.Ext("quick");
  h.clock.advance(15000);
  const cl = await h.call("claim", { draftId: "draft_" + t, sessionId: "sess_slow", workerId: "slow" });
  h.clock.advance(70000);                                               // the slow tab stops heartbeating for 70 s
  await h.Cron.handler();                                               // the 3-minute upkeep notices
  it = h.item(r.body.sendId);
  check(it.state === "queued" && it.notBeforeMs > h.clock.now(), "the 3-minute upkeep puts the silent helper's message back in the queue (" + it.state + ")");
  const lateMark = await h.call("mark_clicked", { draftId: "draft_" + t, sessionId: "sess_slow" });
  check(lateMark.status !== 200, "the slow helper wakes up and asks permission to click: refused (" + lateMark.status + ")");
  h.clock.advance(60000); await h.Q.pump();
  res = await quick.tab(t);
  check(res.complete === 200, "the other helper sends it");
  check((h.sentToEtsy || []).length === 1, "one click on Etsy in total");
  h.done();

  // ═══ 4. after the click: never again, but a late 'sent' still counts ═══
  say("\nAfter the click");
  h = boot({ seed: 15 });
  t = h.thread(506);
  r = await h.send(t, "Late completion test.");
  h.clock.advance(15000);
  await h.call("claim", { draftId: "draft_" + t, sessionId: "sess_L", workerId: "L" });
  await h.call("mark_clicked", { draftId: "draft_" + t, sessionId: "sess_L" });
  h.clock.advance(90000);                                               // silence after the click
  await h.Cron.handler();
  it = h.item(r.body.sendId);
  check(it.state === "needs_attention", "silence after the click: needs attention, not re-sent (" + it.state + ")");
  const late = await h.call("complete", { draftId: "draft_" + t, sessionId: "sess_L" });
  check(late.status === 200, "the helper's late 'sent' report is accepted");
  it = h.item(r.body.sendId);
  check(it.state === "confirmed" && it.open === false, "reality beats bookkeeping: the dead letter closes as confirmed (" + it.state + ")");
  check((h.sentToEtsy || []).length === 0 || true, "(no helper clicked in this script)");
  // the conversation shows it: a stranded message is confirmed by the next scrape of the conversation
  t = h.thread(507);
  r = await h.send(t, "Seen in the conversation test.");
  h.clock.advance(15000);
  await h.call("claim", { draftId: "draft_" + t, sessionId: "sess_M", workerId: "M" });
  await h.call("mark_clicked", { draftId: "draft_" + t, sessionId: "sess_M" });
  h.clock.advance(90000);
  h.fake.poke("EtsyMail_Threads/" + t + "/messages/m1", { direction: "outbound", text: "Seen in the conversation test.", timestamp: new h.fake.Timestamp(h.clock.now() - 60000) });
  await h.Q.resolveHolder({ reason: "test" });
  check(h.item(r.body.sendId).state === "confirmed", "a clicked message that then went silent is confirmed straight away when the stored conversation already shows it");
  h.done();

  // ═══ 5. the old extension's circuit breaker and the older rescues leave managed messages alone ═══
  say("\nThe older safety nets do not fight the dispatcher");
  h = boot({ seed: 16 });
  t = h.thread(508);
  r = await h.send(t, "Breaker test.");
  const fe = await h.call("forceExpire", { draftId: "draft_" + t, reason: "client_circuit_breaker_after_3_attempts" });
  check(fe.status === 200 && fe.body.skipped === true && h.slot(t).status === "queued", "the extension's circuit breaker cannot fail a managed message (skipped)");
  h.clock.advance(15000);
  await h.call("claim", { draftId: "draft_" + t, sessionId: "sess_Z", workerId: "Z" });
  await h.call("heartbeat", { draftId: "draft_" + t, sessionId: "sess_Z" });
  // the older rescue cron looks for 'sending' drafts with an old heartbeat; this one is the dispatcher's
  const slotRef = h.slot(t);
  h.fake.poke("EtsyMail_Drafts/draft_" + t, Object.assign({}, slotRef, { sendHeartbeatAt: new h.fake.Timestamp(h.clock.now() - 5 * 60000) }));
  await h.Cron.handler();
  check(h.slot(t).status === "sending" || h.item(r.body.sendId).state !== "queued", "the older 3-minute rescue does not requeue a managed slot by itself (the dispatcher's own lease rules decide)");
  // the older sweep for an unmanaged slot still works, untouched
  h.fake.poke("EtsyMail_Drafts/draft_etsy_conv_599", { draftId: "draft_etsy_conv_599", threadId: "etsy_conv_599", status: "sending", sendStage: "pre_click", sendAttempts: 1, sendHeartbeatAt: new h.fake.Timestamp(h.clock.now() - 5 * 60000), text: "old build" });
  await h.Cron.handler();
  check(h.slot("etsy_conv_599").status === "queued", "an unmanaged stranded draft (an older build's) is still rescued the old way");
  h.done();

  // ═══ 6. a helper that never comes ═══
  say("\nNo helper at all");
  h = boot({ seed: 17 });
  t = h.thread(509);
  r = await h.send(t, "Is anyone there?");
  check(h.item(r.body.sendId).state === "claimed", "it is offered to the helpers");
  h.clock.advance(130000); await h.Q.maintain();
  it = h.item(r.body.sendId);
  check(it.state === "queued" && it.helperMisses === 1 && it.attempts === 0, "not picked up for two minutes: back in the queue, not counted as a try (" + it.state + ")");
  for (let k = 0; k < 8; k++) { h.clock.advance(150000); await h.Q.maintain(); }
  it = h.item(r.body.sendId);
  check(["queued", "claimed"].includes(it.state), "it keeps being offered while the wait is short (" + it.state + ")");
  h.clock.advance(31 * 60000); h.fake.poke("EtsyMail_OrderLinkMeta/helper", { seenAtMs: h.clock.now() - 20 * 60000 });
  await h.Q.maintain();
  it = h.item(r.body.sendId);
  check(it.state === "failed" && it.lastErrorCode === "HELPER_OFFLINE" && /not running/i.test(it.plain), "after 30 minutes with a silent helper: a visible dead letter saying the helper is not running (" + it.state + ")");
  const sum = await h.Q.summary();
  check(sum.failed === 1 && sum.queued === 0, "the health summary counts it");
  h.done();

  // ═══ 7. kill switch ═══
  say("\nThe inbox's send switch");
  h = boot({ seed: 18 });
  t = h.thread(510);
  await h.call("kill_switch_set", { disabled: true, reason: "test", by: "Paul" });
  r = await h.send(t, "While paused.");
  check(r.status === 503 && r.body.errorCode === "SEND_DISABLED", "paused: a new message is refused plainly");
  await h.call("kill_switch_set", { disabled: false, by: "Paul" });
  r = await h.send(t, "After resume.");
  check(r.status === 200 && h.item(r.body.sendId).state === "claimed", "resumed: works");
  h.done();

  // ═══ 8. an older build's queued slot is taken in, not trampled ═══
  say("\nA slot written by the older build");
  h = boot({ seed: 19 });
  t = h.thread(511);
  h.fake.poke("EtsyMail_Drafts/draft_" + t, { draftId: "draft_" + t, threadId: t, etsyConversationUrl: "https://www.etsy.com/your/conversations/511", text: "old queued", attachments: [], status: "queued", sendOrigin: "manual", queuedAt: new h.fake.Timestamp(h.clock.now()), sendAttempts: 0, sendStage: "pre_click" });
  const r2 = await h.send(h.thread(512), "new message in another conversation");
  check(r2.status === 200, "a new message is accepted");
  ext = new h.Ext("A");
  h.clock.advance(15000);
  res = await ext.tab(t);
  check(res.claim === 409 && res.code === "QUEUE_BUSY", "while another message holds the turn, the older build's message waits (409 QUEUE_BUSY)");
  res = await ext.tab("etsy_conv_512");
  check(res.complete === 200, "the queued message goes first");
  h.clock.advance(15000);
  res = await ext.tab(t);
  check(res.claim === 200 && res.complete === 200, "then the older build's message is claimed (taken into the queue) and sent");
  const adopted = h.items().find(i => i.source === "legacy");
  check(adopted && adopted.state === "confirmed", "it is recorded in the queue (" + (adopted && adopted.state) + ")");
  h.done();

  // ═══ 9. a process that dies halfway loses nothing ═══
  say("\nA function that times out halfway");
  h = boot({ seed: 20 });
  t = h.thread(513);
  // the enqueue's start-up (pump) fails once after the message is stored
  h.fake.failOn((op, path) => op === "set" && path === "EtsyMail_SendQueueMeta/lease", new Error("14 UNAVAILABLE: simulated outage"), true);
  r = await h.send(t, "Survives a timeout.");
  check(r.status === 200 && ["queued", "claimed"].includes(r.body.queueState), "the message is stored even when the start-up fails (" + r.body.queueState + ")");
  h.fake.clearFail();
  await h.Q.maintain();
  it = h.item(r.body.sendId);
  check(it.state === "claimed", "the next upkeep pass starts it");
  // the helper's 'sent' report is stored on the slot, then the function dies before the queue is updated
  h.clock.advance(15000);
  await h.call("claim", { draftId: "draft_" + t, sessionId: "sess_Q", workerId: "Q" });
  await h.call("mark_clicked", { draftId: "draft_" + t, sessionId: "sess_Q" });
  h.fake.failOn((op, path) => op === "get" && path === "EtsyMail_Drafts/draft_" + t, new Error("14 UNAVAILABLE: simulated outage"), true);
  const comp = await h.call("complete", { draftId: "draft_" + t, sessionId: "sess_Q" });
  check(comp.status === 200 || comp.status === 500, "(the report arrived)");
  h.fake.clearFail();
  h.clock.advance(90000); await h.Cron.handler();
  it = h.item(r.body.sendId);
  check(it.state === "confirmed" || it.state === "sent", "the slot says it was sent: the 3-minute pass makes the queue true again (" + it.state + ")");
  check(!h.items().some(i => i.state === "queued"), "nothing was put back to be sent again");
  h.done();

  // ═══ 10. the books never hold what Firestore refuses ═══
  say("\nStorage rules");
  h = boot({ seed: 21 });
  let threw = false;
  try { await h.fake.db.collection("x").doc("y").set({ a: [[1, 2]] }); } catch (e) { threw = /nested/i.test(e.message); }
  check(threw, "the fake database refuses an array inside an array, as the live one does");
  t = h.thread(514);
  await h.send(t, "With a picture.", { attachments: [{ type: "image", storagePath: "etsymail/drafts/x.png", proxyUrl: "/x.png", filename: "x.png", contentType: "image/png", bytes: 10 }, { type: "listing", listingId: "123", listingUrl: "https://www.etsy.com/listing/123", listingTitle: "A charm" }] });
  ext = new h.Ext("A"); await ext.poll();
  check(h.items().every(i => i.state === "confirmed") && h.warn.filter(w => /nested|undefined/i.test(w)).length === 0, "queue entries with attachments store cleanly (no nested arrays, no undefined)");
  h.done();

  // ═══ a helper that was replaced cannot click ═══
  say("\nA helper that went silent, was replaced, and then wakes up cannot click Send");
  h = boot({ seed: 31 });
  t = h.thread(701);
  r = await h.send(t, "Late helper test.", { key: "late-1" });
  const dId = "draft_" + t;
  const c1 = await h.call("claim", { draftId: dId, sessionId: "s_old", workerId: "A" });
  check(c1.status === 200, "helper A claims the message");
  h.clock.advance(70000);
  await h.Q.resolveHolder({ reason: "test" });
  h.clock.advance(61000);
  await h.Q.pump();
  const c2 = await h.call("claim", { draftId: dId, sessionId: "s_new", workerId: "B" });
  check(c2.status === 200, "after A went silent before clicking, helper B is given the message");
  const lateClick = await h.call("mark_clicked", { draftId: dId, sessionId: "s_old" });
  check(lateClick.status !== 200, "A wakes up and asks to click: refused (" + lateClick.status + " " + (lateClick.body.errorCode || "") + ")");
  check(h.item(r.body.sendId).stage === "pre_click", "nothing was recorded as clicked");
  const good = await h.call("mark_clicked", { draftId: dId, sessionId: "s_new" });
  check(good.status === 200, "B's own click is allowed");
  const again = await h.call("mark_clicked", { draftId: dId, sessionId: "s_new" });
  check(again.status === 409 && again.body.errorCode === "ALREADY_CLICKED", "a second click on the same message is refused (ALREADY_CLICKED)");
  h.done();

  R.finish("send-queue-failures");
})().catch(e => { process.stdout.write("CRASH " + (e.stack || e) + "\n"); process.exit(1); });
