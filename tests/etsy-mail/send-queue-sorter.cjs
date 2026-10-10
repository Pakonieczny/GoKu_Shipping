// MAILQUEUE (10 Oct 2026): the Charm Sorter's questions to customers go through the same single queue as the inbox's
// replies. Real _etsyMailOrderLink + etsyMailDraftSend + dispatcher on the in-memory Firestore; a simulated extension.
//   node tests/etsy-mail/send-queue-sorter.cjs
"use strict";
const { boot, reporter } = require("./_queueHarness.cjs");
const R = reporter(); const check = R.check;
const say = s => process.stdout.write(s + "\n");

(async () => {
  console.log = () => {};
  const station = { id: "st1", name: "Bo" };
  let h, t, ext, res;
  const setup = o => {
    h = boot(Object.assign({ seed: 31 }, o || {}));
    t = h.thread(901, { etsyOrderId: "900", customerName: "Dana", etsyUsername: "dana", buyerUserId: "77" });
    ext = new h.Ext("A");
  };
  const eng = id => h.fake.peek("EtsyMail_OrderLinks/" + id);
  const ask = async (text, clientId, extra) => h.OL.ask(station, Object.assign({ receiptId: "900", text, clientId }, extra || {}));
  const outbox = () => { const e = h.fake.list("EtsyMail_OrderLinks")[0]; return e ? e.data.outbox : []; };
  const item = id => outbox().find(x => x.id === id);

  say("A sorter question goes through the queue");
  setup();
  // the inbox had an AI draft in this conversation's reply box; it must come back afterwards
  h.fake.poke("EtsyMail_Drafts/draft_" + t, { draftId: "draft_" + t, threadId: t, text: "AI draft awaiting review", status: "draft", generatedByAI: true, attachments: [], createdBy: "AI" });
  let v = await ask("Which font do you want on the back?", "c1");
  check(item("c1") && item("c1").status === "queued" && /^q_/.test(item("c1").qid), "the question is accepted and queued in the sorter's outbox with its queue id");
  const q1 = h.item(item("c1").qid);
  check(q1 && q1.orderLink && q1.orderLink.i === "c1" && q1.idemKey === "sorter:" + q1.orderLink.e + ":c1", "the queue entry carries the sorter's key: the same question is the same entry");
  check(h.slot(t).orderLink && h.slot(t).text === "Which font do you want on the back?" && h.slot(t).orderLinkParked.had.text === "AI draft awaiting review", "the slot carries the question and the inbox's draft is parked, not lost");
  h.clock.advance(15000);
  res = await ext.tab(t);
  check(res.complete === 200, "the extension sends it");
  check(item("c1").status === "sent" && item("c1").confirmedAtMs > 0, "the sorter shows it sent");
  check(h.slot(t).text === "AI draft awaiting review" && h.slot(t).status === "draft" && !h.slot(t).orderLink, "and the inbox's draft is back in the reply box, as it was");
  check(h.OL._internal.summary(eng(v.id)).pending === 0, "nothing is pending");

  say("\nSeveral at once: one queue, in order, with positions");
  setup({ seed: 32 });
  await ask("First question?", "a1");
  await ask("Second question?", "a2");
  const inbox = await h.send(t, "A reply typed in the inbox.");
  await ask("Third question?", "a3");
  check(["a1", "a2", "a3"].every(i => item(i).status === "queued"), "three questions are all queued (nothing waits on the inbox any more)");
  const order = h.Q.orderQueue(h.items(), h.clock.now()).map(i => i.text);
  check(order.join("|") === ["Second question?", "A reply typed in the inbox.", "Third question?"].join("|"), "one conversation, strict order, the first already holds the turn (" + order.join(" > ") + ")");
  const sync = await h.OL.sync({ n: -1, since: 0, qn: null, qo: true });
  const qi = sync.queue.items;
  check(qi.length === 3 && qi.find(x => x.ol === "a1").st === "claimed" && qi.find(x => x.ol === "a2").pos === 2 && qi.find(x => x.ol === "a3").pos === 4, "the sorter's sync carries each question's place in the queue (a1 first, a2 second, a3 fourth behind the inbox reply)");
  check(!JSON.stringify(sync.queue).includes("A reply typed in the inbox"), "and never the text of the inbox's own replies");
  const again = await h.OL.sync({ n: sync.n, since: sync.v, qn: sync.queue.n, qo: true });
  check(again.queue && again.queue.unchanged === true, "asking again with the same revision costs one document read and says 'unchanged'");
  // let them all go
  for (let k = 0; k < 12 && h.items().some(i => i.open && ["queued", "claimed", "sending"].includes(i.state)); k++) { h.clock.advance(16000); await h.Q.pump(); await ext.poll(); }
  check(["a1", "a2", "a3"].every(i => item(i).status === "sent") && h.items().every(i => i.state === "confirmed"), "they all go, one at a time, in order");
  check(h.sentToEtsy.map(s => s.text).join("|") === ["First question?", "Second question?", "A reply typed in the inbox.", "Third question?"].join("|"), "exactly in the order asked (" + h.sentToEtsy.map(s => s.text.slice(0, 6)).join(",") + ")");

  say("\nFailure, retry, and a message that may have gone");
  setup({ seed: 33 });
  await ask("Does the spelling look right?", "f1");
  h.clock.advance(15000);
  await ext.tab(t, { failBeforeClick: { code: "DOM_SEND_BUTTON_MISS", retry: false } });
  check(item("f1").status === "failed" && /Send button/.test(item("f1").error), "a failure shows in the sorter in plain words: " + item("f1").error);
  const eid = h.fake.list("EtsyMail_OrderLinks")[0].id;
  await h.OL.retry({ engagementId: eid, itemId: "f1" }, station);
  check(item("f1").status === "queued" && h.item(item("f1").qid).epoch === 1, "Try again puts the same queue entry back (not a second message)");
  h.clock.advance(15000);
  await ext.tab(t);
  check(item("f1").status === "sent", "and it goes");
  await ask("Is the second ear the same?", "f2");
  h.clock.advance(15000);
  await ext.tab(t, { dieAt: "after_click" });
  h.clock.advance(90000); await h.Q.maintain();
  check(item("f2").status === "attention" && /Check the conversation/i.test(item("f2").error), "a click with no answer is 'attention', never 'failed': " + item("f2").error);
  let thrown = null;
  try { await h.OL.retry({ engagementId: eid, itemId: "f2" }, station); } catch (e) { thrown = e; }
  check(thrown && thrown.status === 409 && thrown.code === "MAYBE_SENT", "Try again asks the person to look at Etsy first");
  await h.OL.retry({ engagementId: eid, itemId: "f2", confirmMaybeSent: true }, station);
  check(item("f2").status === "queued", "after they confirm, it is queued again");
  await h.OL.cancel({ engagementId: eid, itemId: "f2" });
  check(item("f2").status === "cancelled" && h.item(item("f2").qid).state === "cancelled", "a queued question can be taken back, in the sorter and in the queue");
  await ask("A third thing?", "f3");
  ext.dead = false;                                                     // the browser is back
  h.clock.advance(15000);
  await ext.tab(t, { dieAt: "after_click" });
  h.clock.advance(90000); await h.Q.maintain();
  await h.OL.markSent({ engagementId: eid, itemId: "f3" }, station);
  check(item("f3").status === "sent" && h.item(item("f3").qid).state === "confirmed", "'I sent it myself' closes it in both places");
  h.done();

  say("\nThe customer's reply, the scrape, and an older build's message in flight");
  setup({ seed: 34 });
  await ask("Any engraving change?", "g1");
  h.clock.advance(15000); await ext.tab(t);
  const e0 = h.fake.list("EtsyMail_OrderLinks")[0].data;
  await h.OL.onThreadMessages(t, { orderLinkIds: [e0.id] }, [{ id: "m9", direction: "outbound", tsMs: h.clock.now(), text: "Any engraving change?", senderName: "Shop" }]);
  check(item("g1").msgId === "m9" && item("g1").deliveredAtMs > 0, "the scrape finds our message on Etsy: Delivered");
  // an older build queued a sorter question: no queue entry exists; the old settle path still closes it
  h.fake.poke("EtsyMail_OrderLinkMeta/bell", { n: 5, inflight: { "x~old": { e: e0.id, i: "old", t: t, d: "draft_" + t, at: h.clock.now() } } });
  const ee = h.fake.peek("EtsyMail_OrderLinks/" + e0.id);
  h.fake.poke("EtsyMail_OrderLinks/" + e0.id, Object.assign({}, ee, { outbox: ee.outbox.concat([{ id: "old", text: "From the older build", by: "Bo", atMs: h.clock.now(), status: "queued" }]) }));
  h.fake.poke("EtsyMail_Drafts/draft_" + t, { draftId: "draft_" + t, threadId: t, text: "From the older build", status: "sent", sentAt: new h.fake.Timestamp(h.clock.now()), orderLink: { e: e0.id, i: "old" }, orderLinkParked: { none: true }, attachments: [] });
  await h.OL.sync({ n: -1, since: 0 });
  check(item("old").status === "sent", "an in-flight question from the older build is settled by the old path (" + item("old").status + ")");
  h.done();

  // ═══ the sandbox can never enqueue, claim or lease in the real queue ═══
  say("\nThe sandbox never touches the real queue");
  setup({ seed: 35 });
  const realMsg = await h.send(t, "A real message already in the queue.", { key: "real-1" });
  const realQueueBefore = () => JSON.stringify({ items: h.fake.list("EtsyMail_SendQueue").length, meta: h.fake.list("EtsyMail_SendQueueMeta").map(x => x.id + ":" + JSON.stringify(x.data)).sort(), slots: h.fake.list("EtsyMail_Drafts").length });
  const before = realQueueBefore();
  // (1) a sandbox question through the sorter's own door: simulated, sent at once in the sandbox, never queued
  const sbv = await h.OL.ask(station, { receiptId: "900", text: "Sandbox: which font?", clientId: "sb1", sandbox: true });
  const sbItem = (h.fake.list("EtsyMail_OrderLinks").map(x => x.data).find(e => e.sandbox) || { outbox: [] }).outbox.find(x => x.id === "sb1");
  check(sbItem && sbItem.status === "sent" && !sbItem.qid, "a sandbox question is simulated as sent in the sorter and has no queue id");
  check(realQueueBefore() === before, "...and nothing was written to the real queue, its lease or revision, or any draft slot");
  // (2) the dispatcher's door refuses a sandbox request outright, whoever calls it: by flag, by sandbox engagement id, by key
  for (const [what, extra] of [["flag", { sandbox: true }], ["engagement id", { orderLink: { e: "olsb_900_order_x", i: "q1" } }], ["sorter key", { idempotencyKey: "sorter:olsb_900_order_x:q1" }]]) {
    const r = await h.call("enqueue", Object.assign({ threadId: t, etsyConversationUrl: "https://www.etsy.com/your/conversations/901", text: "Must never go (" + what + ")", employeeName: "Bo", sendOrigin: "manual", allowSendWithoutPendingTracking: true }, extra));
    check(r.status === 403 && r.body.errorCode === "SANDBOX_NEVER_SENDS", "enqueue by " + what + " is refused (403 SANDBOX_NEVER_SENDS)");
  }
  const direct = await h.Q.submit({ threadId: t, conversationUrl: "u", text: "direct", orderLink: { e: "olsb_1_x", i: "i" } });
  check(direct && direct.sandboxRefused === true, "the queue's own submit() refuses a sandbox request too");
  check(realQueueBefore() === before, "...and the real queue, lease, revision and draft slots are exactly as they were");
  // (3) the worst case: a sandbox engagement that wrongly carries a conversation. dispatch / retry / cancel / sent never reach the queue
  h.fake.poke("EtsyMail_OrderLinks/olsb_900_order_zz", { id: "olsb_900_order_zz", receiptId: "900", sandbox: true, status: "open", threadId: t, scope: "order", createdAtMs: h.clock.now(), v: 1,
    outbox: [{ id: "w1", text: "stuck sandbox message", by: "Bo", atMs: h.clock.now(), status: "waiting" }, { id: "w2", text: "failed sandbox message", by: "Bo", atMs: h.clock.now(), status: "failed", qid: "q_fake" }] });
  await h.OL.sync({ n: -1, since: 0 });
  await h.OL.retry({ engagementId: "olsb_900_order_zz", itemId: "w2", sandbox: true }, station).catch(() => null);
  await h.OL.cancel({ engagementId: "olsb_900_order_zz", itemId: "w2", sandbox: true }).catch(() => null);
  await h.OL.markSent({ engagementId: "olsb_900_order_zz", itemId: "w1", sandbox: true }, station).catch(() => null);
  check(realQueueBefore() === before, "even a sandbox engagement that carries a conversation never reaches the real queue through sync, retry, cancel or sent");
  const realId = realMsg.body.sendId;
  const sbOps = [];
  for (const op of ["queue_cancel", "queue_retry", "queue_mark_sent", "queue_dismiss"]) sbOps.push((await h.call(op, { sendId: realId || "q_none", sandbox: true })).status);
  const sbState = await h.get("queue_state", { sandbox: "true", threadId: t });
  check(sbOps.every(c => c === 403) && sbState.status === 200 && sbState.body.queue.items.length === 0 && sbState.body.queue.sandbox === true, "a sandbox page can neither act on the real queue (queue_cancel/retry/mark_sent/dismiss: 403) nor read it (queue_state: empty)");
  check(realQueueBefore() === before, "...with nothing changed in the real queue");
  // (4) the sync a sandbox page gets has no queue part
  h.fake.poke("EtsyMail_OrderLinkMeta/bell", { n: 9, inflight: { "a~b": { e: "ol_x", i: "b", t: t, d: "draft_" + t, at: h.clock.now() } } });
  const sbSync = await h.OL.sync({ n: -1, since: 0, sandbox: true, qo: true });
  check(sbSync.queue === null, "a sandbox page's sync carries no queue part, even when it asks (qo) and a real message is in flight");
  h.done();

  R.finish("send-queue-sorter");
})().catch(e => { process.stdout.write("CRASH " + (e.stack || e) + "\n"); process.exit(1); });
