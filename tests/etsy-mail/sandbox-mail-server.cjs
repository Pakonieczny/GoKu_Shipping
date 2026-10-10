// SANDMAIL (10 Oct 2026): the Charm Sorter's customer email works in the Sandbox as it does on the real side, with one exception that
// stays: nothing typed in the sandbox is ever delivered to a customer. The REAL etsyMailOrderLink endpoint, _etsyMailOrderLink.js and
// etsySandbox.js run on the faithful in-memory Firestore (refuses nested arrays), the sandbox playing a pulled set whose receipts
// carry their real buyer ids; the inbox holds real conversations, a real send queue and a shared reply box. A virtual clock moves the
// sandbox's send states. No network, no Etsy call, no paid AI call, nothing sent.
//
//   1. the buyer's real history, read only: threads, photos, none, a failed lookup, no order loaded, another buyer's thread refused
//   2. a rehearsal question walks Queued (nth) -> Sending -> Sent on the sandbox's own clock, one at a time, with a queue view
//      the real queue never sees; it can be taken back while it waits and not once it is being sent
//   3. the customer's side: a reply now, a reply played for later, unread/preview/counts, read, resolve, reopen, language
//   4. nothing of the real inbox changes (byte for byte) and the real queue is never read by a sandbox page; the worlds never mix
//   5. after a wipe and a new start the leftovers are dropped quietly
//   node tests/etsy-mail/sandbox-mail-server.cjs
"use strict";
const { boot, reporter, ORDERS } = require("./_sandboxMailRig.cjs");
const T = reporter();
const R = boot();
const real = Date.now;
let skew = 0;
Date.now = () => real() + skew;
const sb = { sandbox: true };
const bellOf = () => R.fake.peek("EtsyMail_OrderLinkMeta/bell") || {};
const qr = r => (r.body.queue ? r.body.queue.items.map(v => `${v.st}${v.pos ? ":" + v.pos : ""}`) : null);

(async () => {
  const before = R.realSnap();
  const sandboxDocs = () => R.fake.list("EtsyMail_OrderLinks").filter(x => /^olsb_/.test(x.id) && x.data.sandbox === true).map(x => x.id);

  console.log("The buyer's real history, read only");
  let r = await R.via("history_info", Object.assign({ receiptId: ORDERS.withThreads }, sb));
  T.check(r.status === 200 && r.body.why === "ok" && r.body.total === 4 && r.body.threads.length === 2 && r.body.exact, "a pulled order whose buyer has threads: the real count (4 messages in 2 conversations)");
  T.check(!r.body.threads.some(t => t.threadId === "etsy_conv_113"), "another buyer's conversation is never in it");
  const h = await R.via("history", Object.assign({ receiptId: ORDERS.withThreads, threadId: "etsy_conv_111", limit: 200 }, sb));
  T.check(h.status === 200 && h.body.messages.length === 3 && h.body.messages[2].images.length === 1 && /etsystatic/.test(h.body.messages[2].images[0].src), "Pull all messages: the pages come back oldest first, the photo with them");
  r = await R.via("history", Object.assign({ receiptId: ORDERS.withThreads, threadId: "etsy_conv_113" }, sb));
  T.check(r.status === 404, "a conversation that is not that buyer's is refused");
  r = await R.via("history_info", Object.assign({ receiptId: ORDERS.noThreads }, sb));
  T.check(r.status === 200 && r.body.why === "none" && r.body.total === 0 && r.body.threads.length === 0, "a known buyer with no threads: an honest 'none' (looked and found nothing), not an error");
  r = await R.via("history_info", Object.assign({ receiptId: ORDERS.unknown }, sb));
  T.check(r.status === 200 && r.body.why === "no_buyer", "an order nothing knows: 'no_buyer' (the sorter says: Sandbox: no order loaded)");
  R.fake.failOn((op, p) => p === "Charm_Sandbox/stream", new Error("14 UNAVAILABLE: the sandbox store is down"), false);
  r = await R.via("history_info", Object.assign({ receiptId: "4170000003" }, sb));
  T.check(r.status === 200 && r.body.why === "lookup_failed" && /sandbox's order lookup/.test(r.body.reason), "a lookup that fails: 'lookup_failed' with the reason, never 'no messages': " + r.body.reason);
  R.fake.clearFail();
  skew += 21000;   // (a failed answer is kept 15 s, a 'no buyer' one 20 s)
  r = await R.via("history_info", Object.assign({ receiptId: "4170000003" }, sb));
  T.check(r.body.why === "none", "and it asks again once the pause is over: the buyer is found, nothing in the inbox ('none')");
  r = await R.via("history_info", Object.assign({ receiptId: ORDERS.unknown }, sb));
  T.check(R.diff(before, R.realSnap()).length === 0 && R.counts.etsy === 0, "the lookups wrote nothing and made no Etsy call");

  console.log("Writing: the same visible states, in the sandbox only");
  const ask = (text, clientId, extra) => R.via("ask", Object.assign({ receiptId: ORDERS.withThreads, scope: "order", text, orderNumber: ORDERS.withThreads, buyerName: "Ada Byrne", clientId }, extra || {}, sb));
  let a = await ask("Which font would you like?", "c1");
  const eid = a.body.id;
  T.check(a.status === 200 && /^olsb_/.test(eid) && a.body.link === "sandbox" && !a.body.threadId, "a question writes a sandbox engagement (olsb_), linked to no real conversation");
  T.check(a.body.messages.length === 1 && a.body.messages[0].status === "queued" && a.body.pending === 1, "it appears in the sandbox thread, queued (not already 'sent')");
  T.check(JSON.stringify(qr(a)) === '["queued:1"]', "the answer carries the sandbox's queue view: Queued (1st)");
  let s = await R.via("sync", Object.assign({ n: -1, since: 0, full: true }, sb));
  let since = s.body.v, n = s.body.n;
  T.check(JSON.stringify(qr(s)) === '["queued:1"]' && s.body.changes.some(c => c.id === eid && c.pending === 1), "a sandbox sync says the same, and the badge counts it as on its way");
  const b = await ask("And the size?", "c2", { engagementId: eid });
  T.check(JSON.stringify(qr(b)) === '["queued:1","queued:2"]', "a second message waits behind the first: Queued (1st), Queued (2nd) (one at a time)");
  skew += 2600;
  s = await R.via("sync", Object.assign({ n, since, full: false }, sb));
  T.check(JSON.stringify(qr(s)) === '["sending","queued:2"]', "when its turn comes the first is Sending and the second is Queued (2nd) behind it: " + JSON.stringify(qr(s)));
  let c = await R.via("cancel", Object.assign({ engagementId: eid, itemId: "c1" }, sb));
  T.check(c.status === 409 && /Too late/.test(c.body.error), "a message that is being sent cannot be taken back: " + c.body.error);
  c = await R.via("cancel", Object.assign({ engagementId: eid, itemId: "c2" }, sb));
  T.check(c.status === 200 && !c.body.messages.some(m => m.itemId === "c2"), "one that still waits can: it leaves the thread");
  skew += 5000;
  let t = await R.via("thread", Object.assign({ engagementId: eid }, sb));
  T.check(t.status === 200 && t.body.messages.length === 1 && t.body.messages[0].status === "sent" && t.body.messages[0].sentAtMs > 0, "after its time it is Sent (with the time it was sent)");
  T.check(t.body.pending === 0 && Object.keys(bellOf().sbFlight || {}).length === 0, "nothing is left on its way, and the bell's sandbox list is empty again");
  s = await R.via("sync", Object.assign({ n: -1, since: 0, full: true }, sb));
  T.check(s.body.queue && s.body.queue.items.length === 0 || s.body.queue === null, "the queue view is empty (a finished message leaves it)");
  since = s.body.v; n = s.body.n;
  T.check(R.diff(before, R.realSnap()).length === 0, "the real inbox is byte-for-byte as it was: the real queue, the shared reply box, the conversations, the jobs (" + Object.keys(before).length + " documents)");
  const q0 = (R.fake.stats.byColl.EtsyMail_SendQueue || {}).reads || 0;
  R.fake.poke("EtsyMail_OrderLinkMeta/bell", Object.assign({}, bellOf(), { inflight: { "ol_x~i1": { e: "ol_x", i: "i1", t: "etsy_conv_111", d: "draft_etsy_conv_111", at: Date.now() } } }));
  await R.via("sync", Object.assign({ n: -1, since: 0, full: true }, sb));
  T.check(((R.fake.stats.byColl.EtsyMail_SendQueue || {}).reads || 0) === q0, "a sandbox page does not even read the real send queue (the real upkeep is the real pages' and the cron's)");
  const rs = await R.via("sync", { n: -1, since: 0, full: true, sandbox: false });
  T.check(rs.status === 200 && !rs.body.changes.some(x => /^olsb_/.test(x.id)), "a real page's sync never lists a sandbox engagement");
  { const b0 = Object.assign({}, bellOf()); delete b0.inflight; R.fake.poke("EtsyMail_OrderLinkMeta/bell", b0); }

  console.log("Receiving: the customer's side");
  let sim = await R.via("simulate", Object.assign({ engagementId: eid, text: "Yes please, Anna & Tom" }, sb));
  T.check(sim.status === 200 && sim.body.unread === 1 && sim.body.inboundCount === 1 && sim.body.lastInboundPreview === "Yes please, Anna & Tom" && sim.body.messages.some(m => m.side === "customer"), "a reply played now: unread 1, the preview, the customer's row in the thread");
  s = await R.via("sync", Object.assign({ n, since, full: false }, sb));
  T.check(s.body.changes.some(x => x.id === eid && x.unread === 1), "the next sync carries it to every sorter tab (the bell rang)");
  since = s.body.v; n = s.body.n;
  sim = await R.via("simulate", Object.assign({ engagementId: eid, text: "Gold, please", delayMs: 10000 }, sb));
  T.check(sim.body.unread === 1 && !sim.body.messages.some(m => /Gold/.test(m.text || "")), "a reply played for later is not there yet");
  s = await R.via("sync", Object.assign({ n, since, full: false }, sb));
  T.check(!s.body.changes.some(x => x.unread === 2), "and a sync before its time brings nothing");
  skew += 11000;
  s = await R.via("sync", Object.assign({ n, since, full: false }, sb));
  const ch = s.body.changes.find(x => x.id === eid);
  T.check(ch && ch.unread === 2 && ch.lastInboundPreview === "Gold, please", "when its time comes it arrives by itself, as a real one does: unread 2, the new preview");
  since = s.body.v; n = s.body.n;
  T.check(Object.keys(bellOf().sbFlight || {}).length === 0, "nothing is left on the bell's sandbox list");
  let x = await R.via("read", Object.assign({ engagementId: eid }, sb));
  s = await R.via("sync", Object.assign({ n: bellOf().n - 1, since, full: false }, sb));
  T.check(x.status === 200 && s.body.changes.some(y => y.id === eid && y.unread === 0), "read clears the unread count");
  since = s.body.v;
  x = await R.via("resolve", Object.assign({ engagementId: eid }, sb));
  T.check(x.status === 200 && x.body.status === "resolved", "resolve");
  x = await R.via("simulate", Object.assign({ engagementId: eid, text: "late" }, sb));
  x = await R.via("reopen", Object.assign({ engagementId: eid }, sb));
  T.check(x.status === 200 && x.body.status === "open", "reopen");
  x = await R.via("lang", Object.assign({ engagementId: eid, lang: "uk" }, sb));
  T.check(x.status === 200 && R.fake.peek("EtsyMail_OrderLinks/" + eid).lang === "uk", "language is kept on the sandbox question");
  x = await R.via("link_url", Object.assign({ engagementId: eid, url: "https://www.etsy.com/your/conversations/111" }, sb));
  T.check(x.status === 404, "a sandbox question cannot be pointed at a real conversation (404)");
  const REAL_E = "ol_" + ORDERS.withThreads + "_o_x1", realBefore = JSON.stringify(R.fake.peek("EtsyMail_OrderLinks/" + REAL_E));
  x = await R.via("simulate", { engagementId: REAL_E, sandbox: false });
  T.check(x.status === 400 && /Only a sandbox/.test(x.body.error), "the customer's side cannot be played on a real engagement");
  x = await R.via("simulate", { engagementId: REAL_E, sandbox: true });
  T.check(x.status === 404 && JSON.stringify(R.fake.peek("EtsyMail_OrderLinks/" + REAL_E)) === realBefore, "nor from a sandbox page (404), and the real engagement is untouched");
  const doc = R.fake.peek("EtsyMail_OrderLinks/" + eid);
  T.check(doc.sandbox === true && !doc.threadId, "the sandbox engagement stays sandbox:true and unlinked");

  console.log("The worlds never mix");
  const so = await R.via("order", Object.assign({ receiptId: ORDERS.withThreads, scope: "order" }, sb));
  const ro = await R.via("order", { receiptId: ORDERS.withThreads, scope: "order", sandbox: false });
  T.check(so.body.engagements.length === 1 && so.body.engagements[0].id === eid && ro.body.engagements.length === 1 && ro.body.engagements[0].id === REAL_E, "the same order number: the sandbox shows only its own engagement, the real side only its own");

  console.log("A wipe and a new start");
  const stray = await ask("one more, then the wipe", "c3", { engagementId: eid });
  T.check(stray.status === 200 && Object.keys(bellOf().sbFlight || {}).length === 1, "(a message is on its way when the sandbox is reset)");
  for (const id of sandboxDocs()) await R.fake.db.doc("EtsyMail_OrderLinks/" + id).delete();   // what the wipe does: only olsb_ documents that say sandbox:true
  R.point();
  s = await R.via("sync", Object.assign({ n: -1, since: 0, full: true }, sb));
  T.check(s.status === 200 && s.body.changes.length === 0 && (!s.body.queue || s.body.queue.items.length === 0), "a sandbox sync after the wipe shows nothing of the old run");
  T.check(Object.keys(bellOf().sbFlight || {}).length === 0, "the leftovers of the old run are dropped from the bell, quietly");
  const fresh = await ask("A new run, a new question", "n1");
  T.check(fresh.status === 200 && fresh.body.id !== eid && fresh.body.messages.length === 1, "the new run starts clean");
  T.check(R.diff(before, R.realSnap()).length === 0, "and the real inbox is still exactly as it was");

  T.check(R.counts.etsy === 0 && R.counts.network === 0, "no Etsy call and no network call were made");
  const bad = R.warn.filter(w => /ERROR|sandbox clock/.test(w) && !/the sandbox store is down/.test(w));   // that one is the failed lookup the test caused on purpose
  T.check(!bad.length, "no server error was logged (but the failed lookup the test caused on purpose)" + (bad.length ? ": " + bad.slice(-2).join(" | ") : ""));
  Date.now = real; R.done();
  T.finish("sandbox-mail-server");
})().catch(e => { Date.now = real; R.done(); console.error(e); process.exit(1); });
