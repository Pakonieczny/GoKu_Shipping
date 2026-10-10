"use strict";
/* The offline rig for the sandbox's customer email (SANDMAIL, 10 Oct 2026).
 *
 *   - the REAL etsyMailOrderLink endpoint, _etsyMailOrderLink.js and etsySandbox.js (with the order stream playing a pulled set
 *     of receipts that carry their real buyer ids), on the faithful in-memory Firestore of tests/etsy-mail/_fakeFirestore.cjs
 *     (refuses nested arrays and undefined, enforces read-before-write and retries optimistic conflicts) and an in-memory
 *     Storage holding the pulled file;
 *   - the inbox's real data as the inbox keeps it: conversations with messages (one with a photo), a receipts mirror, the
 *     send queue and the shared reply boxes, all of which a sandbox session must leave byte-for-byte alone;
 *   - nothing reaches Etsy, a customer, the AI or the network: node-fetch and fetch refuse, the Etsy module is a stub that
 *     counts (and refuses) its calls, and the translation call is a canned stand-in that counts.
 */
const path = require("path"), crypto = require("crypto"), http = require("http"), fs = require("fs"), Module = require("module");
const { createFake, install } = require("./_fakeFirestore.cjs");
const fnDir = path.join(__dirname, "../../netlify/functions");
const root = path.join(__dirname, "../..");

const KEY = "station-key-for-the-test-" + "x".repeat(24);
// the pulled set: a real shop's newest orders, each with its real buyer id (Etsy's receipts carry buyer_user_id)
const ORDERS = {
  withThreads: "4170000001",   // the buyer has two conversations in the inbox (one with a photo)
  noThreads: "4170000002",     // the buyer is known, the inbox holds nothing of theirs
  unknown: "4170000099"        // a number nothing knows
};

function boot(opts = {}) {
  opts = opts || {};
  process.env.ETSYMAIL_EXTENSION_SECRET = "test-secret-not-real";
  delete process.env.EDIT_PASSCODE;
  delete process.env.ANTHROPIC_API_KEY;
  const fake = createFake({ seed: opts.seed || 5 });
  const inst = install(fake);
  const blobs = new Map();
  fake.admin.storage = () => ({ bucket: () => ({ file: p => ({
    download: async () => { if (!blobs.has(p)) throw new Error("no such file " + p); return [Buffer.from(blobs.get(p))]; },
    save: async buf => { blobs.set(p, Buffer.from(buf)); }, delete: async () => { blobs.delete(p); }
  }), getFiles: async () => [[], null] }) });
  const counts = { etsy: 0, ai: 0, network: 0 };
  // translation: a canned stand-in for the AI (counts its calls); everything else on the network refuses
  const realLoad = Module._load;
  Module._load = function (req, ...rest) {
    if (req === "node-fetch") return async (url, init) => {
      if (/api\.anthropic\.com/.test(String(url))) {
        counts.ai++;
        const sent = JSON.parse(init.body).messages[0].content;
        const items = JSON.parse(sent).messages;
        return { ok: true, status: 200, json: async () => ({ stop_reason: "end_turn", content: [{ type: "text", text: JSON.stringify({ translations: items.map(x => ({ id: x.id, text: "[translated] " + x.text, from: "uk", same: false })) }) }] }) };
      }
      counts.network++; throw new Error("the test allows no network call: " + url);
    };
    return realLoad.call(this, req, ...rest);
  };
  process.env.ANTHROPIC_API_KEY = "test-key-not-real";
  const realNow = Date.now;
  const warn = [], realWarn = console.warn, realError = console.error;
  console.warn = (...a) => { warn.push(a.map(String).join(" ")); };
  console.error = (...a) => { warn.push("ERROR " + a.map(String).join(" ")); };
  // a fresh copy of the server modules, with Etsy and the AI context stubbed
  for (const k of Object.keys(require.cache)) if (k.startsWith(fnDir)) delete require.cache[k];
  const stub = (rel, exports) => { const p = require.resolve(path.join(fnDir, rel)); require.cache[p] = { id: p, filename: p, loaded: true, exports }; };
  stub("_etsyMailLearning.js", { recordOutcome: async () => {} });
  stub("_etsyMailAnthropic.js", { fetchClassificationContext: async () => { counts.ai++; throw new Error("no AI in this test"); } });
  stub("_etsyMailEtsy.js", { getShopReceiptFull: async id => { counts.etsy++; throw new Error("the test allows no Etsy call: " + id); }, getValidEtsyAccessToken: async () => { counts.etsy++; throw new Error("no Etsy token in this test"); } });
  const endpoint = require(path.join(fnDir, "etsyMailOrderLink.js"));
  const OL = require(path.join(fnDir, "_etsyMailOrderLink.js"));
  const Sandbox = require(path.join(fnDir, "etsySandbox.js"));

  const T = Date.UTC(2026, 9, 10, 12, 0, 0) / 1000;   // 2026-10-10 12:00 UTC (the pulled orders were made just before)
  const receipt = (id, buyer, name, extra) => Object.assign({
    receipt_id: Number(id), buyer_user_id: buyer, name, is_paid: true, was_paid: true, is_shipped: false, status: "paid",
    create_timestamp: T - 3600, created_timestamp: T - 3600, expected_ship_date: T + 5 * 86400,
    transactions: [{ transaction_id: Number(id) + 1, title: "Charm", quantity: 1, sku: "SKU" }]
  }, extra || {});
  const PATH = "charmnest/sandbox/orders-pull/test.json";
  blobs.set(PATH, JSON.stringify({ at: T * 1000, count: 3, source: "etsy-pull", receipts: [
    receipt(ORDERS.withThreads, 7001, "Ada Byrne", { create_timestamp: T - 7200, created_timestamp: T - 7200 }),
    receipt(ORDERS.noThreads, 7002, "Bo Lindqvist", { create_timestamp: T - 3600, created_timestamp: T - 3600 }),
    receipt("4170000003", 7003, "Cleo Real")
  ] }));
  const ms = v => new fake.Timestamp(v);
  const NOW = realNow();
  const point = () => {
    fake.poke("Charm_Sandbox/current", { path: PATH, source: "etsy-pull", startId: "start-1", count: 3, open: 3, at: T * 1000, pulledAt: NOW });
    fake.poke("Charm_Sandbox/stream", { on: true, v: 2, seed: 7, tick: 300, simStart: (T - 86400) * 1000, simNow: NOW, stepMs: 600000, min: 2, max: 5, speed: 50, total: 3, snapshotPath: PATH });
  };
  point();
  const msg = (n, dir, text, extra) => Object.assign({ direction: dir, text, senderName: dir === "inbound" ? "Ada Byrne" : "Shop", timestamp: ms(NOW - 86400e3 * 5 + n * 3600e3) }, extra || {});
  // the real inbox: Ada has two conversations (one names order 4170000001 and holds a photo); a stranger has one too
  const inbox = {
    "EtsyMail_Threads/etsy_conv_111": { threadId: "etsy_conv_111", customerName: "Ada Byrne", etsyOrderId: ORDERS.withThreads, buyerUserId: "7001", status: "etsy_scraped", lastInboundAt: ms(NOW - 86400e3 * 4), updatedAt: ms(NOW - 86400e3 * 4), etsyConversationUrl: "https://www.etsy.com/your/conversations/111" },
    "EtsyMail_Threads/etsy_conv_111/messages/a1": msg(1, "inbound", "Hi, can the back say Anna & Tom?"),
    "EtsyMail_Threads/etsy_conv_111/messages/a2": msg(2, "outbound", "Of course, in which font?"),
    "EtsyMail_Threads/etsy_conv_111/messages/a3": msg(3, "inbound", "Here is the handwriting I like", { imageUrls: ["https://i.etsystatic.com/1/r/il/abc/1/il_794xN.1.jpg"], thumbnailUrls: ["https://i.etsystatic.com/1/r/il/abc/1/il_340x270.1.jpg"] }),
    "EtsyMail_Threads/etsy_conv_112": { threadId: "etsy_conv_112", customerName: "Ada Byrne", etsyOrderId: "4160000555", buyerUserId: "7001", status: "archived", lastInboundAt: ms(NOW - 86400e3 * 30), updatedAt: ms(NOW - 86400e3 * 30) },
    "EtsyMail_Threads/etsy_conv_112/messages/b1": msg(-200, "inbound", "Do you ship to Canada?"),
    "EtsyMail_Threads/etsy_conv_113": { threadId: "etsy_conv_113", customerName: "Stranger", etsyOrderId: "4150000001", buyerUserId: "8008", status: "archived", updatedAt: ms(NOW - 86400e3 * 40) },
    "EtsyMail_Threads/etsy_conv_113/messages/c1": msg(-300, "inbound", "A stranger's private words"),
    "EtsyMail_Receipts/4170000001": { receiptId: "4170000001", buyer_user_id: 7001, name: "Ada Byrne" },
    "EtsyMail_Drafts/draft_etsy_conv_111": { draftId: "draft_etsy_conv_111", threadId: "etsy_conv_111", status: "draft", text: "A half-written reply by the shop", createdBy: "Anna" },
    "EtsyMail_SendQueue/q_real1": { sendId: "q_real1", threadId: "etsy_conv_111", state: "queued", open: true, text: "Real message waiting in the real queue", createdAtMs: NOW - 60e3, updatedAtMs: NOW - 60e3 },
    "EtsyMail_SendQueueMeta/lease": { holderSendId: null, paceUntilMs: 0 },
    "EtsyMail_SendQueueMeta/rev": { n: 4 },
    "EtsyMail_Jobs/job1": { jobType: "scrape", status: "queued", threadId: "etsy_conv_111" },
    "EtsyMail_OrderLinkMeta/helper": { seenAtMs: NOW - 20e3 },
    "EtsyMail_Config/global": { sendDisabled: false },
    "EtsyMail_Config/gmailWatcher": { enabled: true }, "EtsyMail_Config/scrapeHealth": { consecutiveBad: 0, lastOkAtMs: NOW - 60e3 },
    "EtsyMail_Config/linkHealth": { atMs: NOW - 30e3, level: "ok", checks: [] }, "EtsyMail_Config/gmailSyncState": { lastSyncCompletedAt: ms(NOW - 60e3) },
    "EtsyMail_OrderLinkMeta/bell": { n: 1, reconcileAtMs: NOW - 60e3 },
    // a REAL question of the shop's on the very same order number the sandbox rehearses (it must never show in, or be touched by, the sandbox)
    [`EtsyMail_OrderLinks/ol_${ORDERS.withThreads}_o_x1`]: { id: `ol_${ORDERS.withThreads}_o_x1`, receiptId: ORDERS.withThreads, scope: "order", sandbox: false, status: "open", threadId: "etsy_conv_111", link: "thread",
      createdAtMs: NOW - 9e5, v: 1, outbox: [{ id: "i1", text: "Your order ships Friday.", status: "queued", atMs: NOW - 60e3 }], seen: [], sim: [], unread: 1, inboundCount: 1 }
  };
  for (const [p, d] of Object.entries(inbox)) fake.poke(p, d);
  fake.poke("EtsyMail_OrderLinkStations/" + crypto.createHash("sha256").update(KEY).digest("hex"), { username: "paul", displayName: "Paul", createdAtMs: NOW - 1e7, lastSeenAtMs: NOW });
  fake.poke("EtsyMail_Operators/paul", { username: "paul", displayName: "Paul", role: "owner" });

  const parse = r => { let b = {}; try { b = JSON.parse(r.body || "{}"); } catch (_) { b = { raw: r.body }; } return { status: r.statusCode, body: b }; };
  const via = async (op, body, o) => parse(await endpoint.handler({ httpMethod: "POST", headers: { "x-mail-station": (o && o.key) || KEY }, body: JSON.stringify(Object.assign({ op }, body || {})) }));
  const snap = prefix => { const out = {}; for (const [coll] of prefix.map(c => [c])) for (const x of fake.list(coll)) out[coll + "/" + x.id] = JSON.stringify(x.data); return out; };
  // everything of the real inbox a sandbox session must leave exactly as it was
  const REAL_COLLS = ["EtsyMail_Threads", "EtsyMail_Drafts", "EtsyMail_SendQueue", "EtsyMail_SendQueueMeta", "EtsyMail_Jobs", "EtsyMail_Receipts", "EtsyMail_Config", "EtsyMail_Operators", "EtsyMail_Customers"];
  const realSnap = () => {
    const out = snap(REAL_COLLS);
    for (const id of ["etsy_conv_111", "etsy_conv_112", "etsy_conv_113"]) for (const x of fake.list(`EtsyMail_Threads/${id}/messages`)) out[`EtsyMail_Threads/${id}/messages/${x.id}`] = JSON.stringify(x.data);
    for (const x of fake.list("EtsyMail_OrderLinks")) if (!(x.data.sandbox === true && /^olsb_/.test(x.id))) out["EtsyMail_OrderLinks/" + x.id] = JSON.stringify(x.data);
    return out;
  };
  const diff = (a, b) => { const bad = []; for (const k of new Set([...Object.keys(a), ...Object.keys(b)])) if (a[k] !== b[k]) bad.push(k); return bad; };

  /** The browser side: a tiny server for the sorter harness page, the real charm-nest-mail.js and the real endpoint. */
  function serve(htmlFor) {
    return new Promise(resolve => {
      const srv = http.createServer((req, res) => {
        const u = new URL(req.url, "http://x");
        if (u.pathname === "/h.html") { res.writeHead(200, { "Content-Type": "text/html" }); return res.end(htmlFor(u.searchParams)); }
        if (u.pathname === "/send-flight.js") { res.writeHead(200, { "Content-Type": "text/javascript", "Cache-Control": "no-store" }); return res.end(fs.readFileSync(path.join(root, "send-flight.js"))); }
        if (u.pathname === "/charm-nest-mail.js") { res.writeHead(200, { "Content-Type": "text/javascript", "Cache-Control": "no-store" }); return res.end(fs.readFileSync(path.join(root, "charm-nest-mail.js"))); }
        if (u.pathname.endsWith("/etsyMailOrderLink")) {
          let raw = ""; req.on("data", c => raw += c); req.on("end", async () => {
            const r = await endpoint.handler({ httpMethod: "POST", headers: { "x-mail-station": req.headers["x-mail-station"] || "" }, body: raw });
            if (serve.seen) { try { const b = JSON.parse(raw); serve.seen.push({ op: b.op, body: b, status: r.statusCode, res: r.body }); } catch (_) {} }
            res.writeHead(r.statusCode, { "Content-Type": "application/json" }); res.end(r.body);
          });
          return;
        }
        res.writeHead(404); res.end();
      }).listen(0, "127.0.0.1", () => resolve(srv));
    });
  }
  serve.seen = null;

  return {
    fake, OL, Sandbox, endpoint, via, counts, warn, blobs, point, KEY, ORDERS, NOW, realSnap, diff, serve, parse,
    done() { Date.now = realNow; console.warn = realWarn; console.error = realError; Module._load = realLoad; inst.restore(); }
  };
}

function reporter() {
  const fails = [];
  const out = { check(ok, what) { if (!ok) fails.push(what); process.stdout.write((ok ? "  ok   " : "  FAIL ") + what + "\n"); }, fails };
  out.finish = name => {
    if (fails.length) { process.stdout.write("\n" + fails.length + " FAILED in " + name + ":\n - " + fails.join("\n - ") + "\n"); process.exit(1); }
    process.stdout.write("\n" + name + ": all checks passed\n");
  };
  return out;
}

module.exports = { boot, reporter, KEY, ORDERS };
