// MAILDIAG (10 Oct 2026): the sorter's Customer tab counts and pulls "this buyer's messages" from the inbox's own copy of the
// buyer's conversations (ops history_info / history). For a SANDBOX order the buyer used to come only from the sandbox's own
// copy of the order, so when that copy was gone (the old snapshot stopped being a source with the 250-newest pull, a wipe, a
// pull that has not run yet: the sorter keeps showing the order meanwhile) the answer was an empty "No messages with this
// buyer in the inbox yet". Now the inbox's own records of the same Etsy number name the buyer. Drives the REAL module over
// the cost meter's in-memory Firestore. No network, no Etsy call (the test fails on any), no write, no real data.
//
//   node tests/etsy-mail/history-sandbox-buyer.cjs
"use strict";
const assert = require("assert"), path = require("path"), Module = require("module");
const meter = require("../cost/meter.cjs");
const m = meter.create();
m.install();
let etsyCalls = 0;
{ const prev = Module._load;
  Module._load = function (req, ...rest) {
    if (req === "node-fetch") return async url => { etsyCalls++; throw new Error("the test allows no network call: " + url); };
    if (/(^|[\\/])_etsyApiMeter(\.js)?$/.test(req)) return { bump: () => ({ failNet() {}, fromHttp() {} }), bumpSimple: () => {}, wrapHandler: fn => fn, flushNow: async () => {} };
    if (/(^|[\\/])_etsyMailEtsy(\.js)?$/.test(req)) return { getShopReceiptFull: async id => { etsyCalls++; throw new Error("the test allows no Etsy call: " + id); } };
    return prev.call(this, req, ...rest);
  };
}
process.env.ETSYMAIL_EXTENSION_SECRET = "s3cret-for-test";
const link = require(path.join(__dirname, "../../netlify/functions/_etsyMailOrderLink.js"));

const RID = "4174973193";            // the order of Paul's screenshot (a real shop order the sandbox shows)
const msg = (i, dir) => ({ direction: dir, text: "m" + i, timestamp: meter.Timestamp.fromMillis(1790000000000 + i * 60000) });
const seed = {
  // the buyer's two conversations (one names this order, one is another order of the same buyer) and a stranger's
  "EtsyMail_Threads/etsy_conv_111": { etsyOrderId: RID, buyerUserId: "7001", status: "etsy_scraped", updatedAt: meter.Timestamp.fromMillis(1790000500000) },
  "EtsyMail_Threads/etsy_conv_112": { etsyOrderId: "999", buyerUserId: "7001", status: "archived", updatedAt: meter.Timestamp.fromMillis(1789000000000) },
  "EtsyMail_Threads/etsy_conv_113": { etsyOrderId: "555", buyerUserId: "8002", status: "archived", updatedAt: meter.Timestamp.fromMillis(1789000000000) },
  "EtsyMail_Threads/etsy_conv_111/messages/a1": msg(1, "inbound"), "EtsyMail_Threads/etsy_conv_111/messages/a2": msg(2, "outbound"),
  "EtsyMail_Threads/etsy_conv_112/messages/b1": msg(3, "inbound"),
  "EtsyMail_Threads/etsy_conv_113/messages/c1": msg(4, "inbound"),
  // the shop's stored copy of another order of the same buyer (the receipts mirror) and a buyer cache entry
  "EtsyMail_Receipts/777": { buyer_user_id: 7001 },
  "EtsyMail_Receipts/778": { raw: { buyer_user_id: 7001 } },
  "EtsyMail_OrderLinkBuyers/779": { buyerUserId: "7001", atMs: Date.now() },
  "EtsyMail_OrderLinkBuyers/780": { buyerUserId: null, atMs: Date.now() },
  // what the live sandbox holds now: no order pointer, only an old stream document
  "Charm_Sandbox/stream": { on: true, v: 2, seed: 42069871, tick: 103, simStart: 1790998200000, simNow: 1791060000000, stepMs: 600000, min: 2, max: 5, speed: 50, total: 0 }
};

(async () => {
  m.db.seed(seed);
  const info = async (receiptId, sandbox) => link.historyInfo({ receiptId, sandbox });
  const w0 = m.snapshot();

  // 1. the live state: no sandbox copy of the order, but the inbox's conversation names it -> the buyer's two conversations
  let r = await info(RID, true);
  assert.deepStrictEqual(r.threads.map(t => t.threadId).sort(), ["etsy_conv_111", "etsy_conv_112"], "sandbox order named by a conversation: that buyer's conversations");
  assert.strictEqual(r.total, 3, "2 + 1 messages (never the stranger's)");
  assert.ok(r.exact);

  // 2. no conversation names it, the receipts mirror knows the buyer (both shapes of the mirror's field)
  for (const id of ["777", "778"]) { r = await info(id, true); assert.strictEqual(r.total, 3, "mirror " + id); }

  // 3. the buyer cache knows it; a cached "no buyer" stays no buyer
  r = await info("779", true); assert.strictEqual(r.total, 3, "buyer cache");
  r = await info("780", true); assert.strictEqual(r.total, 0, "cached unknown buyer");

  // 4. an order nothing knows: honestly empty, no Etsy call
  r = await info("123456789", true); assert.strictEqual(r.total, 0); assert.deepStrictEqual(r.threads, []);

  // 5. the history pages work for the same sandbox order, and never leak another buyer's conversation
  const page = await link.history({ receiptId: RID, sandbox: true, threadId: "etsy_conv_111", limit: 200 });
  assert.deepStrictEqual(page.messages.map(x => x.text), ["m1", "m2"]);
  await assert.rejects(() => link.history({ receiptId: RID, sandbox: true, threadId: "etsy_conv_113" }), /not this buyer's/);

  // 6. a sandbox copy of the order, when there is one, still wins (the pulled orders carry buyer_user_id): a pointer plus its file
  m.db.seed({ "Charm_Sandbox/current": { source: "etsy-pull", path: "charmnest/sandbox/orders-pull/x.json", count: 1, at: Date.now(), startId: "s1" } });
  m.db.seed({ "Charm_Sandbox/stream": null });
  await m._storage.bucket().file("charmnest/sandbox/orders-pull/x.json").save(JSON.stringify({ receipts: [{ receipt_id: 4242, buyer_user_id: 8002, is_paid: true }] }));
  r = await info("4242", true);
  assert.deepStrictEqual(r.threads.map(t => t.threadId), ["etsy_conv_113"], "the sandbox's own copy names the buyer");

  // 7. the real (non-sandbox) path is the one it always was
  r = await info(RID, false); assert.strictEqual(r.total, 3, "production order, by its conversation");

  // nothing was written and Etsy was never called
  const d = m.since(w0);
  assert.strictEqual(d.writes, 0, "no write"); assert.strictEqual(d.deletes, 0, "no delete");
  assert.strictEqual(etsyCalls, 0, "no Etsy call");
  console.log("history-sandbox-buyer: OK (reads " + d.reads + ", writes " + d.writes + ", Etsy calls " + etsyCalls + ")");
})().catch(e => { console.error("FAIL:", e && e.stack || e); process.exit(1); });
