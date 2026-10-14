// The Etsy helper's computer is shut down for hours (14 Oct 2026). Offline: the real etsyMailDraftSend / _etsyMailSendQueue on an
// in-memory Firestore, a virtual clock and a simulated extension (tests/etsy-mail/_queueHarness.cjs). Nothing reaches Etsy or a customer.
//
//   · while the helper is silent, messages are SAVED in the queue and nothing is handed to it: no dead letters, no tries burned,
//     no churn on the draft slot; pages are told "offline" (wait: "offline") so they can say "delayed", calmly;
//   · the queue is looked at again on every upkeep pass; when the helper is heard again it must stay heard (settle) before the first
//     message goes, and a helper that flaps gets nothing;
//   · the backlog goes out one at a time, in order, and while MORE than two wait the pause after each send is random, 7 to 17 s;
//   · dead letters of the old "helper offline" kind (nothing was sent) go back in the queue by themselves, unless the conversation
//     already shows the words, they are too old, or the Send button may have been clicked;
//   · a message waits for days, not for ever (QUEUED_EXPIRED after a week).
//
//   node tests/etsy-mail/send-queue-outage.cjs
"use strict";
const { boot, reporter } = require("./_queueHarness.cjs");
const R = reporter(); const check = R.check; const say = s => process.stdout.write(s + "\n");
const MIN = 60000, HOUR = 60 * MIN;
/** a small seeded generator, so the "random" pauses are the same on every run */
const seeded = seed => () => { seed |= 0; seed = (seed + 0x6D2B79F5) | 0; let t = Math.imul(seed ^ (seed >>> 15), 1 | seed); t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t; return ((t ^ (t >>> 14)) >>> 0) / 4294967296; };

(async () => {
  let h, t, r, ext;
  // ═══ 1. the computer goes off: messages are saved, nothing is handed over, nothing dies ═══
  say("The helper's computer is off: messages wait, safe");
  h = boot({ seed: 31 });
  t = h.thread(701);
  ext = new h.Ext("A");
  r = await h.send(t, "First, while the helper is here.");
  await ext.poll(); h.clock.advance(20000);
  check(h.item(r.body.sendId).state === "confirmed", "(the helper is up: an ordinary message goes out at once)");
  h.helperUp = false;
  h.clock.advance(6 * MIN);                                       // the helper has been silent for more than 5 minutes
  const N = 12;                                                   // a night's backlog
  const threads = Array.from({ length: N }, (_, i) => h.thread(702 + i));
  const sent = [];
  for (let i = 0; i < threads.length; i++) { const x = await h.send(threads[i], "Message " + (i + 1) + " for the buyer"); sent.push(x.body.sendId); h.clock.advance(2000); }
  check(sent.length === N && sent.every(id => h.item(id).state === "queued"), "twelve messages sent while the helper is silent are all just queued: nothing is handed to the helper");
  check(threads.every(th => h.slot(th).status !== "queued"), "...and none of them is written into a draft slot as 'queued' (the helper would see nothing to open)");
  check(h.lease().linkState === "holding", "the queue knows the helper is away (lease.linkState = holding)");
  check(threads.every(th => h.fake.peek("EtsyMail_Threads/" + th).sendQueueState === "queued"), "each conversation still carries its 'a message is on its way' flag, so the inbox shows it");
  let view = (await h.get("queue_state", { threadId: threads[0], open: "1" })).body.queue;
  check(view.offline === true && view.items.length === 1 && view.items[0].st === "queued" && view.items[0].wait === "offline", "a page asking about the conversation is told: queued, wait 'offline'");
  check(view.items[0].at > 0 && view.items[0].tp === "Message 1 for the buyer", "...with the time the message was written (the page's clock counts from there)");
  const revBefore = h.rev();
  // four hours of the 3-minute upkeep with nobody there
  for (let i = 0; i < 80; i++) { h.clock.advance(3 * MIN); await h.Q.maintain(); }
  check(sent.every(id => h.item(id).state === "queued" && h.item(id).attempts === 0 && !h.item(id).helperMisses), "four hours later: all twelve still queued, no try used, no 'helper did not pick it up' miss counted");
  check(h.items().filter(i => i.state === "failed" || i.state === "needs_attention").length === 0, "no dead letters");
  check(threads.every(th => h.slot(th).status !== "queued" && h.slot(th).status !== "failed"), "the draft slots were never touched by the waiting");
  check(h.rev() - revBefore <= 1, "the idle waiting wrote next to nothing (revision moved " + (h.rev() - revBefore) + ")");
  const sum = await h.Q.summary();
  check(sum.offline === true && sum.held === N && sum.failed === 0 && sum.needsAttention === 0, "the health summary: offline, 12 held, 0 failed");
  check(h.warn.filter(w => !/^ERROR .*no network/.test(w)).length === 0, "no warnings or errors (" + h.warn.slice(0, 2).join(" | ") + ")");

  // ═══ 2. the computer comes back: heard twice, then they go, one by one, in order ═══
  say("\nThe helper is back: really back, then in order");
  h.helperUp = true;
  await ext.poll();                                               // its first sign of life
  h.clock.advance(MIN); await h.Q.maintain();
  check(sent.every(id => h.item(id).state === "queued"), "heard once after the silence: still nothing handed over (reconnecting)");
  check(h.lease().linkState === "holding" && h.lease().linkBackSeenMs > 0, "the first sighting is remembered");
  view = (await h.get("queue_state", { threadId: threads[0], open: "1" })).body.queue;
  check(view.items[0].wait === "offline", "while reconnecting the page still says delayed");
  h.clock.advance(MIN); await ext.poll(); h.clock.advance(MIN); await h.Q.maintain();
  check(h.lease().linkState === "online" && h.lease().holderSendId, "heard again a minute later: settled, the first message is handed over");
  check(h.slot(threads[0]).status === "queued" || h.item(sent[0]).state === "claimed", "the OLDEST message is the one (" + h.item(sent[0]).state + ")");
  h.Q._setRandom(seeded(7));
  const pauses = [], order = [];
  for (let guard = 0; guard < 800 && sent.some(id => h.item(id).state !== "confirmed"); guard++) {
    await h.Q.pump();
    const before = (h.sentToEtsy || []).length;
    await ext.poll();
    if ((h.sentToEtsy || []).length > before) {
      order.push(h.sentToEtsy[h.sentToEtsy.length - 1].text);
      await h.Q.pump();                                           // (the pump after a release is what sets the wide pause)
      const L = h.lease(); if (L.paceUntilMs) pauses.push(L.paceUntilMs - L.lastReleaseAtMs);
    }
    h.clock.advance(3000);
  }
  check(sent.every(id => h.item(id).state === "confirmed"), "all twelve went out");
  check(order.join("|") === Array.from({ length: N }, (_, i) => "Message " + (i + 1) + " for the buyer").join("|"), "...in the order they were written");
  check((h.sentToEtsy || []).length === N + 1 && new Set(h.sentToEtsy.map(x => x.text)).size === N + 1, "each exactly once on Etsy (" + (N + 1) + " clicks in all, none twice)");
  // on Etsy itself: typing takes 9 s and the confirmation 1.5 s in the simulated helper, so two clicks are never closer than 10.5 s + the pause
  const clicks = h.sentToEtsy.map(x => x.at), gaps = clicks.slice(1).map((c, i) => c - clicks[i] - 10500);
  check(gaps.slice(1, N).every(g => g >= 7000 - 1), "the helper was really held back: the time between clicks never dropped under the 7 s pause (shortest " + (Math.min(...gaps.slice(1, N)) / 1000).toFixed(1) + " s)");
  const wide = pauses.slice(0, N - 1);                            // after every send but the last one a message still waited
  check(wide.length === N - 1 && wide.every(p => p >= 7000 && p <= 17000), "every pause while the backlog drained is between 7 and 17 s: " + wide.map(p => (p / 1000).toFixed(1)).join(", ") + " s");
  check(new Set(wide.map(p => Math.round(p / 100))).size >= 8, "...and they are random, not one fixed number");
  check(wide.some(p => p < 9000) && wide.some(p => p > 15000), "...across the whole 7 to 17 s range (the old pause was only 9 to 15 s)");
  check(h.Q.bigGapFor(h.Q.DEFAULTS, () => 0) === 7000 && h.Q.bigGapFor(h.Q.DEFAULTS, () => 0.999999) === 17000 && h.Q.bigGapFor(h.Q.DEFAULTS, () => 0.5) === 12000, "(the rule itself: 7 s at one end, 17 s at the other, 12 s in the middle)");
  check(h.lease().bigBatch === true || !h.lease().holderSendId, "(the long-queue mode lasts as long as the backlog)");
  await h.Q.pump();
  check(h.lease().bigBatch === undefined && h.lease().linkState === undefined, "once the queue is empty the long-queue mode and the outage state are cleared");
  h.done();

  // ═══ 3. two messages are not a long queue: the usual pause ═══
  say("\nTwo waiting is not a long queue");
  h = boot({ seed: 32 }); h.Q._setRandom(seeded(9));
  ext = new h.Ext("A");
  const a = h.thread(711), b = h.thread(712);
  const ra = await h.send(a, "Alpha"); const rb = await h.send(b, "Bravo");
  await ext.poll();
  await h.Q.pump();
  let L = h.lease();
  check(h.item(ra.body.sendId).state === "confirmed" && L.bigBatch !== true, "one waiting behind the first: no long-queue mode");
  check(L.paceUntilMs - L.lastReleaseAtMs >= 9000 && L.paceUntilMs - L.lastReleaseAtMs <= 15000, "the pause is the usual 9 to 15 s (" + (L.paceUntilMs - L.lastReleaseAtMs) + " ms)");
  h.done();

  // ═══ 4. a helper that flaps gets nothing ═══
  say("\nA helper that comes and goes");
  h = boot({ seed: 33 });
  ext = new h.Ext("A");
  t = h.thread(721);
  h.clock.advance(1000);
  h.helperUp = false; h.clock.advance(10 * MIN);
  r = await h.send(t, "Waiting for a steady helper");
  check(h.item(r.body.sendId).state === "queued" && h.lease().linkState === "holding", "queued and held");
  await ext.poll(); h.clock.advance(MIN); await h.Q.maintain();
  check(h.item(r.body.sendId).state === "queued" && h.lease().linkBackSeenMs > 0, "it came back for one check-in: remembered, nothing handed over");
  h.clock.advance(9 * MIN); await h.Q.maintain();               // and is gone again
  check(h.item(r.body.sendId).state === "queued" && !h.lease().linkBackSeenMs && h.lease().linkState === "holding", "gone again before it settled: the settling starts over");
  await ext.poll(); h.clock.advance(MIN); await h.Q.maintain();   // back: the first sighting again
  check(h.item(r.body.sendId).state === "queued", "back again: only the first sighting, still nothing handed over");
  h.clock.advance(MIN); await ext.poll(); h.clock.advance(5000); await h.Q.maintain();
  check(["claimed", "sending"].includes(h.item(r.body.sendId).state), "heard again a minute later: now it is handed over (" + h.item(r.body.sendId).state + ")");
  h.done();

  // ═══ 5. the ordinary case is untouched ═══
  say("\nA helper that is there, or never heard from, is not held");
  h = boot({ seed: 34 });
  t = h.thread(731);
  r = await h.send(t, "Nobody has ever checked in");
  check(h.item(r.body.sendId).state === "claimed", "a helper never heard from is not 'away': the message is offered as before");
  h.done();
  h = boot({ seed: 35 });
  t = h.thread(732);
  h.clock.advance(MIN);
  h.helperUp = false; h.clock.advance(4 * MIN);                   // silent for 4 minutes only
  r = await h.send(t, "A short gap is no outage");
  check(h.item(r.body.sendId).state === "claimed" && h.lease().linkState === undefined, "silent for less than 5 minutes: not held (" + h.item(r.body.sendId).state + ")");
  h.done();

  // ═══ 6. dead letters of the old kind come back by themselves, carefully ═══
  say("\nOld 'helper offline' dead letters go back in the queue");
  h = boot({ seed: 36 });
  ext = new h.Ext("A");
  const mk = (n, over) => {
    const th = h.thread(n);
    const id = "q_old" + n, now = h.clock.now();
    h.fake.poke("EtsyMail_SendQueue/" + id, Object.assign({
      sendId: id, idemKey: "k" + n, derived: true, threadId: th, draftId: "draft_" + th, conversationUrl: "https://www.etsy.com/your/conversations/" + n,
      text: "Old message " + n, attachments: [], source: "inbox", origin: "manual", priority: 0, createdBy: "Anna", createdAtMs: now - 2 * HOUR, epoch: 0,
      state: "failed", open: true, attempts: 0, notBeforeMs: 0, stage: "pre_click", helperMisses: 0, sessions: [], events: [], updatedAtMs: now - HOUR,
      lastErrorCode: "HELPER_OFFLINE", lastError: "x", plain: "The Etsy helper (the Chrome extension) is not running or not picking messages up."
    }, over || {}));
    return { th, id };
  };
  h.clock.advance(MIN);
  const fresh = mk(741), gone = mk(742), tooOld = mk(743, { createdAtMs: h.clock.now() - 80 * HOUR }), clicked = mk(744, { stage: "post_click", clickedAtMs: h.clock.now() - HOUR }), other = mk(745, { lastErrorCode: "MAX_ATTEMPTS" });
  // 742: someone already sent those words by hand: the conversation shows them
  h.fake.poke("EtsyMail_Threads/" + gone.th + "/messages/m_hand", { id: "m_hand", direction: "outbound", text: "Old message 742", timestamp: new h.fake.Timestamp(h.clock.now() - HOUR) });
  h.fake.poke("EtsyMail_SendQueueMeta/rev", { n: 1, atMs: h.clock.now() });
  const sum6 = await h.Q.summary();
  check(sum6.reviving === 2, "the summary knows two of them are about to be put back (" + sum6.reviving + ")");
  await h.Q.maintain();
  check(["queued", "claimed"].includes(h.item(fresh.id).state), "a young one, never sent: back in the queue (" + h.item(fresh.id).state + ")");
  check(h.item(gone.id).state === "confirmed", "one whose words are already in the conversation is settled as sent, not sent again (" + h.item(gone.id).state + ")");
  check(h.item(tooOld.id).state === "failed" && h.item(tooOld.id).open === true, "one older than 72 hours is left for a person");
  check(h.item(clicked.id).state === "failed" && h.item(clicked.id).open === true, "one whose Send button may have been clicked is left for a person");
  check(h.item(other.id).state === "failed" && h.item(other.id).open === true, "a failure of another kind is left alone");
  check(sent_once(h, "Old message 742") === 0, "nothing was sent for the hand-sent one");
  h.done();

  // ═══ 7. people keep writing: replies are generated and sent while the helper is away; each joins the queue, in order ═══
  say("\nWriting goes on while the helper is away");
  h = boot({ seed: 37 });
  const admin = require(require("path").join(__dirname, "../../netlify/functions/firebaseAdmin.js"));
  const inUse = th => admin.firestore().runTransaction(async tx => {
    const sn = await tx.get(admin.firestore().collection("EtsyMail_Drafts").doc("draft_" + th));
    return h.Q.slotInUse(tx, sn.exists ? sn.data() : null, th);
  });
  ext = new h.Ext("A");
  h.clock.advance(MIN);
  h.helperUp = false; h.clock.advance(10 * MIN);
  const t1 = h.thread(751), t2 = h.thread(752), t3 = h.thread(753);
  const r1 = await h.send(t1, "First reply to this customer");
  check(r1.status === 200 && h.item(r1.body.sendId).state === "queued" && h.slot(t1).queueWaiting === true, "a reply is sent while the helper is away: accepted, queued");
  check((await inUse(t1)) === false, "its conversation's draft slot is NOT in use (nothing was handed over), so an AI draft or a link message may be written there");
  const r2 = await h.send(t1, "Second reply, written afterwards");
  check(r2.status === 200 && !r2.body.deduped && h.item(r2.body.sendId).state === "queued" && r2.body.sendId !== r1.body.sendId, "a second reply in the SAME conversation is accepted too (it queues behind the first)");
  const r3 = await h.send(t2, "A reply in another conversation");
  const r4 = await h.send(t3, "And one more");
  check(r3.status === 200 && r4.status === 200, "replies in other conversations are accepted");
  const auto = await h.send(t1, "An automatic reply on top of a person's", { origin: "auto", aiMeta: { generatedByAI: true, model: "m" } });
  check(auto.status === 409 && auto.body.errorCode === "DRAFT_BUSY", "an AUTOMATIC reply is still never queued on top of a person's reply in the same conversation (409, as before)");
  let view7 = (await h.get("queue_state", { threadId: t2, open: "1" })).body.queue;
  check(view7.held === 4 && view7.items.length === 1, "the page is told how many emails wait in all (" + view7.held + "), whatever the conversation it asks about");
  // the helper comes back and settles; then everything goes, one at a time, each conversation in order
  h.helperUp = true; await ext.poll(); h.clock.advance(MIN); await h.Q.maintain();
  h.clock.advance(MIN); await ext.poll();
  h.clock.advance(5000); await h.Q.maintain();
  check(["claimed", "sending"].includes(h.item(r1.body.sendId).state), "the first reply is handed over (" + h.item(r1.body.sendId).state + ")");
  check((await inUse(t1)) === "queued", "now that it is in flight the slot IS in use: nothing may be written over it");
  const marks = h.Q.queueMarksOf({ queueWaiting: true, queueWaitingOrigin: "manual", queueWaitingSendId: "q_x", queueRecentFp: "f", queueRecentSendId: "q_y", queueRecentAtMs: 5, text: "x" });
  check(marks.queueWaiting === true && marks.queueWaitingOrigin === "manual" && marks.queueRecentFp === "f" && marks.text === undefined && Object.keys(h.Q.queueMarksOf({ text: "x" })).length === 0, "a writer that replaces the slot keeps the queue's marks on it (and only those)");
  const sent7 = [];
  for (let g = 0; g < 300 && [r1, r2, r3, r4].some(r => h.item(r.body.sendId).state !== "confirmed"); g++) {
    await h.Q.pump(); const b = (h.sentToEtsy || []).length; await ext.poll();
    if ((h.sentToEtsy || []).length > b) sent7.push(h.sentToEtsy[h.sentToEtsy.length - 1].text);
    h.clock.advance(3000);
  }
  check(sent7.length === 4 && sent7.indexOf("First reply to this customer") < sent7.indexOf("Second reply, written afterwards"), "all four replies went out, once each, the two in one conversation in the order written");
  check(sent7[0] === "First reply to this customer", "and the oldest message went first");
  h.done();

  // ═══ 8. a page that shows "waiting for the helper" is told the moment the helper is judged away ═══
  say("\nThe page is told when the helper goes away");
  h = boot({ seed: 38 });
  ext = new h.Ext("A");
  h.clock.advance(MIN);
  const t8 = h.thread(761);
  const r8 = await h.send(t8, "Sent while the helper was still there");
  let v8 = (await h.get("queue_state", { threadId: t8, open: "1" })).body.queue;
  check(v8.items[0].wait !== "offline" && v8.held === 0, "the helper is there: not delayed, none held");
  h.helperUp = false;
  h.clock.advance(3 * MIN); await h.Q.maintain();                 // not picked up: back in the queue (the helper has not been silent for 5 minutes yet)
  h.clock.advance(3 * MIN);
  const before8 = h.rev();
  h.clock.advance(MIN); await h.Q.maintain();                     // silent for 7 minutes: the queue calls it away
  check(h.lease().linkState === "holding" && h.rev() > before8, "when the queue first judges the helper away the revision moves (" + before8 + " -> " + h.rev() + "), so an open page asks again");
  v8 = (await h.get("queue_state", { threadId: t8, open: "1", n: String(before8) })).body.queue;
  check(!v8.unchanged && v8.items[0].wait === "offline" && v8.held === 1, "and the page's next look says delayed, 1 email held");
  h.done();

  // the writers that put an AI draft, a sales-agent draft or a link message in a conversation's slot all ask the queue first
  const fsx = require("fs"), pth = require("path");
  for (const f of ["etsyMailDraftReply.js", "etsyMailSalesAgent-background.js", "etsyMailListingCreator-background.js"]) {
    const src = fsx.readFileSync(pth.join(__dirname, "../../netlify/functions", f), "utf8");
    check(/_etsyMailSendQueue"\)\.slotInUse\(tx,/.test(src) && !/\.queueWaiting === true\) return/.test(src), f + " asks the queue whether the slot is really in use (and no longer refuses for any waiting reply)");
  }

  R.finish("send-queue-outage");
})().catch(e => { console.error(e); process.exit(1); });

function sent_once(h, text) { return (h.sentToEtsy || []).filter(x => x.text === text).length; }
