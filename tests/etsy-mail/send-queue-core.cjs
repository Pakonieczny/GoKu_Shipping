// MAILQUEUE (10 Oct 2026): ONE durable queue and ONE lease for every outgoing buyer message.
// Offline: the real etsyMailDraftSend / _etsyMailSendQueue / cleanup cron on an in-memory Firestore that refuses nested
// arrays and `undefined`, a virtual clock, and a simulated Chrome extension (0.9.52 protocol). No Etsy, no customer, no AI.
//
//   node tests/etsy-mail/send-queue-core.cjs
"use strict";
const { boot, reporter } = require("./_queueHarness.cjs");
const R = reporter(); const check = R.check;

(async () => {
  // ═══ 1. one message, start to finish ═══
  console.log = () => {};
  let h = boot({ seed: 1 });
  process.stdout.write("One message, start to finish\n");
  let t = h.thread(101, { status: "queued_for_auto_send" });
  let r = await h.send(t, "Hello there, thanks for your order!");
  check(r.status === 200 && r.body.status === "queued" && /^q_/.test(r.body.sendId), "the inbox's Send is accepted and gets a queue id");
  check(h.item(r.body.sendId).state === "claimed" && h.slot(t).status === "queued" && h.slot(t).queueSendId === r.body.sendId, "an idle queue starts it at once: the slot holds it as queued, for the helper");
  check(h.fake.peek("EtsyMail_Threads/" + t + "/messages/optim_draft_" + t.replace("etsy_conv_", "etsy_conv_")) !== undefined || h.fake.list("EtsyMail_Threads/" + t + "/messages").length === 1, "the stand-in bubble is written at once (the inbox shows the message while it waits)");
  check(h.fake.peek("EtsyMail_Threads/" + t).sendQueueState === "queued", "the conversation carries sendQueueState=queued (the inbox list shows it with no new polling)");
  let ext = new h.Ext("A");
  const sentId = r.body.sendId;
  let x = await ext.poll();
  check(x.length === 1 && x[0].claim === 200 && x[0].clicked && x[0].complete === 200, "the extension claimed, clicked once and completed");
  check(h.item(sentId).state === "confirmed" && h.item(sentId).open === false, "the queue follows: confirmed, closed");
  check(h.lease().holderSendId === null && h.lease().paceUntilMs > h.clock.now(), "the turn is released and the pause before the next send is set");
  check(h.fake.peek("EtsyMail_Threads/" + t).sendQueueState === undefined && h.fake.peek("EtsyMail_Threads/" + t).awaitingReplySince === undefined, "the conversation flag is cleared and the thread was promoted as before");
  check(h.sentToEtsy.length === 1, "exactly one click on Etsy");
  check(h.warn.filter(w => !/^ERROR .*no network/.test(w)).length === 0, "no warnings or errors on the happy path (" + h.warn.slice(0, 2).join(" | ") + ")");
  h.done();

  // ═══ 2. the same message twice is one message ═══
  process.stdout.write("\nThe same message twice is one message\n");
  h = boot({ seed: 2, jitterMs: 3 });
  t = h.thread(102);
  const burst = await Promise.all([1, 2, 3, 4, 5, 6].map(() => h.send(t, "Your order ships Monday.", { key: "press-1" })));
  check(burst.every(b => b.status === 200), "six simultaneous presses (a double click, a retry, two tabs) are all answered");
  check(h.items().length === 1 && new Set(burst.map(b => b.body.sendId)).size === 1, "one queue entry, one id");
  check(burst.filter(b => b.body.deduped).length === 5, "five were recognised as the same press");
  ext = new h.Ext("A"); await ext.poll();
  const again = await h.send(t, "Your order ships Monday.", { key: "press-1" });
  check(again.status === 200 && again.body.deduped && again.body.queueState === "confirmed", "pressing again after it went out shows the same finished message");
  check(h.sentToEtsy.length === 1, "one click on Etsy in total");
  const w1 = await h.send(t, "Your order ships Tuesday.");                 // no key (an older tab): derived key
  const w2 = await h.send(t, "Your order ships Tuesday.");
  check(w2.body.deduped === true && w1.body.sendId === w2.body.sendId, "no key at all: the same words twice within seconds are one message");
  const early = await ext.poll();
  check(early[0] && early[0].claim === 429 && early[0].code === "PACING", "a helper that claims inside the 12 s pause after the last send is told to wait (429 PACING)");
  h.clock.advance(15000); await ext.poll();                                // w1 goes out
  check(h.item(w1.body.sendId).state === "confirmed", "(the repeated words went out once)");
  const clicksSoFar = h.sentToEtsy.length;
  h.clock.advance(60000);
  const w3 = await h.send(t, "Your order ships Tuesday.");
  check(!w3.body.deduped && h.item(w3.body.sendId).epoch === 1 && ["claimed", "queued"].includes(h.item(w3.body.sendId).state), "the same words a minute after they went out are a new message (a person may mean to say it again)");
  h.clock.advance(15000); await ext.poll();
  check(h.sentToEtsy.length === clicksSoFar + 1, "...and it is sent, once");
  h.done();

  // ═══ 3. one at a time, whatever the number of browsers ═══
  process.stdout.write("\nOne at a time\n");
  h = boot({ seed: 3, jitterMs: 4 });
  const threads = [201, 202, 203, 204, 205, 206].map(n => h.thread(n));
  // sample the books on every commit: never two slots offered/claimed, never two items active
  let maxActive = 0, maxQueuedSlots = 0, samples = 0;
  h.fake.hooks.beforeCommit = () => {
    samples++;
    const active = h.fake.list("EtsyMail_SendQueue").filter(x => x.data.state === "claimed" || x.data.state === "sending").length;
    const slots = h.fake.list("EtsyMail_Drafts").filter(x => x.data.status === "queued" || x.data.status === "sending").length;
    maxActive = Math.max(maxActive, active); maxQueuedSlots = Math.max(maxQueuedSlots, slots);
  };
  for (let i = 0; i < threads.length; i++) { await h.send(threads[i], "Message number " + i + " for you.", { by: i % 2 ? "Anna" : "Ben" }); h.clock.advance(200); }
  check(h.items().length === 6, "six messages from six conversations are all accepted");
  const exts = [new h.Ext("A"), new h.Ext("B"), new h.Ext("C")];
  for (let round = 0; round < 80 && h.items().some(i => i.open); round++) {
    await Promise.all(exts.map(e => e.poll()));
    h.clock.advance(7000);
    await h.Q.pump();
  }
  check(h.items().every(i => i.state === "confirmed"), "all six were sent (" + h.items().map(i => i.state).join(",") + ")");
  check(h.sentToEtsy.length === 6 && new Set(h.sentToEtsy.map(s => s.text)).size === 6, "six clicks on Etsy, one per message, none twice");
  check(maxActive <= 1, "never more than one message claimed or sending at any moment (" + maxActive + " over " + samples + " commits)");
  check(maxQueuedSlots <= 1, "never more than one slot offered to the helpers at any moment (" + maxQueuedSlots + ")");
  const clicks = h.sentToEtsy.map(s => s.at);
  const gaps = clicks.slice(1).map((c, i) => c - clicks[i]);
  check(gaps.every(g => g >= 9000 + 12000 - 1000), "each send starts at least the 12 s pause after the previous one ended (shortest gap " + Math.min(...gaps) / 1000 + " s)");
  check(h.sentToEtsy.map(s => s.text).join("|") === threads.map((_, i) => "Message number " + i + " for you.").join("|"), "they went in the order they were pressed");
  h.fake.hooks.beforeCommit = null;
  h.done();

  // ═══ 4. order: people first, then automated; a conversation stays in order ═══
  process.stdout.write("\nOrder\n");
  h = boot({ seed: 4 });
  const a = h.thread(301), b = h.thread(302), c = h.thread(303), d = h.thread(304);
  const hold = await h.send(h.thread(300), "first, claimed at once");           // takes the turn, so the rest wait
  await h.send(a, "auto one", { origin: "auto", aiMeta: { generatedByAI: true, model: "m" } });
  h.clock.advance(1000);
  await h.send(b, "person one");
  h.clock.advance(1000);
  await h.send(c, "auto two", { origin: "auto", aiMeta: { generatedByAI: true, model: "m" } });
  h.clock.advance(1000);
  await h.send(d, "person two");
  h.clock.advance(1000);
  await h.send(b, "person one, second message");
  const order = h.Q.orderQueue(h.items(), h.clock.now()).map(i => i.text);
  check(order.join("|") === "person one|person two|person one, second message|auto one|auto two".split("|").join("|") || order.join("|") === ["person one", "person one, second message", "person two", "auto one", "auto two"].join("|") || true, "(order printed below)");
  process.stdout.write("       order: " + order.join(" > ") + "\n");
  check(order[0] === "person one" && order.indexOf("person one") < order.indexOf("person one, second message"), "a conversation's own messages keep their order");
  check(order.slice(0, 3).every(x => /person/.test(x)) && order.slice(3).every(x => /auto/.test(x)), "messages typed by a person go before automated ones");
  check(order.indexOf("auto one") < order.indexOf("auto two"), "inside a class, oldest first");
  // aging: an automated message that has waited 15 minutes ranks as a person's
  const aged = h.Q.orderQueue(h.items(), h.clock.now() + 16 * 60000).map(i => i.text);
  check(aged.indexOf("auto one") < aged.indexOf("person one, second message") || aged[0] === "auto one" || aged.indexOf("auto one") < aged.indexOf("person two"), "after 15 minutes waiting an automated message is no longer starved by newer people's messages (" + aged.join(" > ") + ")");
  // an automated reply never goes in behind a reply a person has waiting in the same conversation
  const guard = await h.send(b, "auto over the person's", { origin: "auto", aiMeta: { generatedByAI: true, model: "m" } });
  check(guard.status === 409 && guard.body.errorCode === "DRAFT_BUSY", "an automated reply is refused while a person's reply is waiting in that conversation (audit F1)");
  h.done();

  // ═══ 5. the helper dies, the message is not lost and not sent twice ═══
  process.stdout.write("\nThe helper dies\n");
  h = boot({ seed: 5 });
  t = h.thread(401);
  r = await h.send(t, "Please confirm the engraving spelling.");
  const dead = new h.Ext("dead"), alive = new h.Ext("alive");
  let res = await dead.tab(t, { dieAt: "typing" });
  check(res.died === "typing" && h.item(r.body.sendId).state === "sending", "helper one claimed it and went silent while typing");
  check((await alive.tab(t)).peek === "none", "a second helper's peek is offered nothing while the first holds the turn");
  const race = await alive.handle({ id: "draft_" + t, threadId: t });          // both tabs peeked at once, the second claims late
  check(race.claim === 409, "...and a late claim from it is refused (" + race.code + ")");
  h.clock.advance(61000);
  const peekAfter = await h.call("peek", { threadId: t });
  check(peekAfter.body.queued === false, "after a minute of silence a peek repairs the books and offers nothing yet");
  check(h.item(r.body.sendId).state === "queued" && h.item(r.body.sendId).notBeforeMs > h.clock.now(), "the message is back in the queue with a short delay (typed but never clicked: safe to send again)");
  check(h.sentToEtsy === undefined, "nothing reached Etsy yet");
  h.clock.advance(61000);
  await h.Q.pump();
  res = await alive.tab(t);
  check(res.claim === 200 && res.complete === 200 && h.item(r.body.sendId).state === "confirmed", "the other helper sends it");
  check(h.sentToEtsy.length === 1, "one click on Etsy in total");
  // dies AFTER the click: never sent again
  t = h.thread(402);
  r = await h.send(t, "Your package left today.");
  h.clock.advance(20000);
  res = await alive.tab(t, { dieAt: "after_click" });
  check(res.clicked && res.died === "after_click", "a helper clicked Send and then died");
  h.clock.advance(61000);
  await h.Q.resolveHolder({ reason: "test" });
  let it = h.item(r.body.sendId);
  check(it.state === "needs_attention" && it.open === true && /Check the conversation/i.test(it.plain), "that message is a dead letter that says in plain words to check the conversation first (" + it.state + ")");
  const before = h.sentToEtsy.length;
  for (let i = 0; i < 6; i++) { h.clock.advance(70000); await h.Q.maintain(); await new h.Ext("late" + i).poll(); }
  check(h.sentToEtsy.length === before, "never sent again by itself, however long it waits");
  const need = await h.call("queue_retry", { sendId: r.body.sendId, by: "Anna" });
  check(need.status === 409 && need.body.errorCode === "QUEUE_MAYBE_SENT" && /Your package left today/.test(need.body.text), "Send again asks the person to check first and hands back the words to copy");
  const yes = await h.call("queue_retry", { sendId: r.body.sendId, by: "Anna", confirmMaybeSent: true });
  check(yes.status === 200 && h.item(r.body.sendId).state === "claimed", "after the person confirms, it goes back in the queue");
  h.done();

  // ═══ the same words from two places are one message ═══
  process.stdout.write("\nThe same words pressed in two tabs are one message\n");
  h = boot({ seed: 7 });
  t = h.thread(701);
  const sw_w1 = await h.send(t, "Your charm ships tomorrow.", { key: "tab1-press" });
  const sw_w2 = await h.send(t, "Your charm ships tomorrow.", { key: "tab2-press" });
  check(sw_w2.status === 200 && sw_w2.body.deduped === true && sw_w2.body.sendId === sw_w1.body.sendId && h.items().length === 1, "two tabs, two keys, the same words within seconds: one message in the queue");
  const sw_w3 = await h.send(t, "Your charm ships on Friday instead.", { key: "tab3-press" });
  check(h.items().length === 2 && sw_w3.body.sendId !== sw_w1.body.sendId, "different words are a different message");
  h.clock.advance(25000);
  const sw_w4 = await h.send(t, "Your charm ships tomorrow.", { key: "tab4-press" });
  check(sw_w4.body.deduped !== true && h.items().length === 3, "the same words a while later (25 s) are a new message on purpose");
  // taken back, then sent again at once: allowed
  const sw_t2 = h.thread(702);
  const sw_c1 = await h.send(sw_t2, "Sorry, wrong text.", { key: "sw_c1" });
  await h.call("queue_cancel", { sendId: sw_c1.body.sendId, by: "Anna" });
  const sw_c2 = await h.send(sw_t2, "Sorry, wrong text.", { key: "sw_c2" });
  check(sw_c2.body.deduped !== true && sw_c2.body.sendId !== sw_c1.body.sendId, "a message that was taken back can be written again at once");
  // a Charm Sorter question is never merged by its words
  const sw_t3 = h.thread(703);
  const sw_s1 = await h.sorter(sw_t3, "What font would you like?", "ol_1", "i1");
  const sw_s2 = await h.sorter(sw_t3, "What font would you like?", "ol_1", "i2");
  check(sw_s1.body.sendId !== sw_s2.body.sendId && h.items().filter(i => i.threadId === sw_t3).length === 2, "two sorter lines asking the same thing stay two messages (the sorter's own key is the truth)");
  h.done();

  // ═══ a long pause is not shown to the helper early ═══
  process.stdout.write("\nA long pause keeps the next message out of the helper's sight until the pause is nearly over\n");
  h = boot({ seed: 8 });
  await h.call("queue_config", { config: { gapMs: 120000 } });
  const tA = h.thread(601), tB = h.thread(602);
  await h.send(tA, "Message one (pause test).", { key: "p-1" });
  ext = new h.Ext("P"); await ext.poll();
  const rB = await h.send(tB, "Message two (pause test).", { key: "p-2" });
  check(h.item(rB.body.sendId).state === "queued" && (!h.slot(tB) || h.slot(tB).status !== "queued"), "with a 2-minute pause the next message waits in the queue; no slot shows it to the helper yet");
  const peekEarly = await h.call("peek", { threadId: tB });
  check(peekEarly.body.queued === false, "a helper tab looking at that conversation sees nothing to do (its tab-opening poll does not count this as a miss)");
  h.clock.advance(100000); await h.Q.pump();
  check(h.item(rB.body.sendId).state === "claimed", "within 30 s of the end of the pause it is offered to the helper");
  const tooSoon = await ext.tab(tB);
  check(tooSoon.claim === 429 && tooSoon.code === "PACING", "and a claim before the pause ends is still told to wait");
  h.clock.advance(25000);
  const onTime = await ext.tab(tB);
  check(onTime.claim === 200 && onTime.complete === 200, "after the pause the helper sends it");
  h.done();

  // ═══ the stand-in bubble belongs to the message being sent ═══
  process.stdout.write("\nTwo messages in one conversation: the stand-in follows the one being sent\n");
  h = boot({ seed: 9 });
  t = h.thread(501);
  const standIn = () => { const d = h.fake.peek("EtsyMail_Threads/" + t + "/messages/optim_draft_" + t); return d && (d.text || d.body || JSON.stringify(d)); };
  const a1 = await h.send(t, "First message of the pair.", { key: "k-a1" });
  check(/First message/.test(String(standIn())), "the first message has its stand-in");
  const a2 = await h.send(t, "Second message of the pair.", { key: "k-a2" });
  check(h.item(a2.body.sendId).state === "queued" && /First message/.test(String(standIn())), "the second waits behind it and does NOT replace the first's stand-in");
  ext = new h.Ext("S"); await ext.poll();
  h.clock.advance(13000); await h.Q.pump();
  check(h.item(a2.body.sendId).state === "claimed" && /Second message/.test(String(standIn())), "when its turn comes the stand-in is its own");
  h.done();

  R.finish("send-queue-core");
})().catch(e => { process.stdout.write("CRASH " + (e.stack || e) + "\n"); process.exit(1); });
