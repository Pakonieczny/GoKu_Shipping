// GUARD TEST: nothing the Charm Sorter's sandbox does (a rehearsal question, its replies, its Customer tab, its reset and wipe)
// can change a real customer's mail, and no request can act on an engagement of the other world.
// Companion of tests/charm-nest/sandbox-wipe-guard.cjs (which proves the wipe against the whole database); this one is about the
// mail collections only. Offline: the in-memory Firestore of tests/charm-nest/_sandboxFakes.cjs (refuses nested arrays, logs every
// write), the REAL etsyMailOrderLink endpoint, _etsyMailOrderLink.js and charmNestLibrary's sandboxReset. Etsy and the sandbox's
// order emulation are stubs that count their calls (none are made). No network, no paid AI, nothing is sent.
//
//   1. production's mail is seeded: threads with messages, queued reply boxes, scrape jobs, customers, receipts, config, helper
//      check-in, the change counter, a real engagement with a message waiting, and two mislabelled lookalikes (an olsb_ document
//      that says sandbox:false, an ol_ document that says sandbox:true);
//   2. every sandbox op of the order-link door is driven (ask, order, thread, sync, history_info, history, retry, cancel, copied,
//      sent, read, resolve, reopen, lang, simulate, health): each may write only sandbox engagements (olsb_…) and the change
//      counter; none calls Etsy;
//   3. every op that names an engagement by id refuses, writing nothing, when the request says it is in the other world;
//   4. the real sandboxReset wipes the sandbox's engagements and nothing else: every production mail document, and both
//      lookalikes, are byte-identical afterwards.
//   node tests/etsy-mail/mail-isolation-guard.cjs
"use strict";
const path = require("path"), assert = require("assert"), crypto = require("crypto");
process.env.CHARM_NEST_DELETE_CODE = "guard-test-delete-code";
const Fk = require("../charm-nest/_sandboxFakes.cjs"); Fk.install();
const { store, writes, TS, canon } = Fk;
const fnDir = path.join(__dirname, "../../netlify/functions");
const Families = require("../../charm-nest-sandbox-families.js");
const warn = [], realWarn = console.warn, realError = console.error;
console.warn = (...a) => { warn.push(a.map(String).join(" ")); }; console.error = (...a) => { warn.push("ERROR " + a.map(String).join(" ")); };

// Etsy and the sandbox's order emulation: stubs that count (the sandbox must never reach Etsy)
const etsy = { calls: 0 }, sbox = { calls: 0 };
const stub = (rel, exports) => { const p = require.resolve(path.join(fnDir, rel)); require.cache[p] = { id: p, filename: p, loaded: true, exports }; };
stub("_etsyMailEtsy.js", { getShopReceiptFull: async () => { etsy.calls++; return { buyer_user_id: "7001" }; } });
stub("etsySandbox.js", { serve: async () => { sbox.calls++; return { statusCode: 404, body: "{}" }; } });
process.env.ETSYMAIL_EXTENSION_SECRET = "test-secret-not-real";

const endpoint = require(path.join(fnDir, "etsyMailOrderLink.js"));
const lib = require(path.join(fnDir, "charmNestLibrary.js"));

const fails = [];
const check = (ok, what) => { if (!ok) fails.push(what); console.log((ok ? "  ok   " : "  FAIL ") + what); };

const KEY = "station-key-" + "x".repeat(40), NOW = Fk.realNow();
const RID_P = "4170000101", RID_S = "4170000202", RID_S2 = "4170000303";   // a production order; the order the sandbox plays (a real number)
const T1 = "etsy_conv_111", T2 = "etsy_conv_222";

// ── 1 · production's mail ──
const PROD = {
  [`EtsyMail_Threads/${T1}`]: { customerName: "Ada Byrne", etsyOrderId: RID_P, buyerUserId: "7001", status: "etsy_scraped", lastInboundAt: new TS(NOW - 3600e3), messageCount: 3 },
  [`EtsyMail_Threads/${T1}/messages/m1`]: { direction: "inbound", text: "Hi, which chain length?", timestamp: new TS(NOW - 7200e3) },
  [`EtsyMail_Threads/${T1}/messages/m2`]: { direction: "outbound", text: "18 inches", timestamp: new TS(NOW - 5400e3) },
  [`EtsyMail_Threads/${T1}/messages/m3`]: { direction: "inbound", text: "Thanks!", timestamp: new TS(NOW - 3600e3) },
  [`EtsyMail_Threads/${T2}`]: { customerName: "Cleo Real", etsyOrderId: RID_S, buyerUserId: "9001", status: "etsy_scraped", lastInboundAt: new TS(NOW - 86400e3), messageCount: 2 },
  [`EtsyMail_Threads/${T2}/messages/n1`]: { direction: "inbound", text: "Is my order on its way?", timestamp: new TS(NOW - 90000e3) },
  [`EtsyMail_Threads/${T2}/messages/n2`]: { direction: "outbound", text: "Yes, tomorrow.", timestamp: new TS(NOW - 86400e3) },
  [`EtsyMail_Drafts/draft_${T1}`]: { status: "queued", text: "Your order ships Friday.", queuedAt: new TS(NOW - 60e3), orderLink: { e: `ol_${RID_P}_o_x1`, i: "i1" } },
  "EtsyMail_Jobs/job1": { jobType: "scrape", status: "queued", threadId: T1, createdAt: new TS(NOW - 120e3) },
  "EtsyMail_Customers/c1": { name: "Ada Byrne", etsyUserId: "7001" },
  [`EtsyMail_Receipts/${RID_P}`]: { receiptId: RID_P, buyerUserId: "7001", name: "Ada Byrne" },
  [`EtsyMail_Receipts/${RID_S}`]: { receiptId: RID_S, buyerUserId: "9001", name: "Cleo Real" },
  "EtsyMail_OrderLinkBuyers/9001": { buyerUserId: "9001", at: NOW - 1e6 },
  "EtsyMail_Translations/t1": { text: "hola", lang: "es", out: "hello" },
  "EtsyMail_Config/gmailWatcher": { enabled: true }, "EtsyMail_Config/scrapeHealth": { consecutiveBad: 0, lastOkAtMs: NOW - 60e3 },
  "EtsyMail_Config/linkHealth": { atMs: NOW - 30e3, level: "ok", checks: [] }, "EtsyMail_Config/global": { sendDisabled: false },
  "EtsyMail_OrderLinkMeta/helper": { seenAtMs: NOW - 20e3 },
  [`EtsyMail_OrderLinks/ol_${RID_P}_o_x1`]: { id: `ol_${RID_P}_o_x1`, receiptId: RID_P, scope: "order", sandbox: false, status: "open", threadId: T1, link: "thread", createdAtMs: NOW - 9e5, v: 1,
    outbox: [{ id: "i1", text: "Your order ships Friday.", status: "queued", atMs: NOW - 60e3 }, { id: "i2", text: "failed one", status: "failed", atMs: NOW - 90e3 }, { id: "i3", text: "by hand", status: "manual", atMs: NOW - 80e3 }], seen: [], sim: [], unread: 1, inboundCount: 1 },
  // lookalikes that must survive everything the sandbox does: the wipe needs the id prefix AND the flag
  "EtsyMail_OrderLinks/olsb_odd1": { id: "olsb_odd1", receiptId: RID_P, scope: "order", sandbox: false, status: "open", threadId: T1, outbox: [], seen: [], sim: [], createdAtMs: NOW - 8e5, v: 2 },
  "EtsyMail_OrderLinks/ol_odd2": { id: "ol_odd2", receiptId: RID_P, scope: "order", sandbox: true, status: "open", threadId: null, outbox: [], seen: [], sim: [], createdAtMs: NOW - 8e5, v: 3 }
};
for (const [k, v] of Object.entries(PROD)) store.set(k, v);
store.set("EtsyMail_OrderLinkStations/" + crypto.createHash("sha256").update(KEY).digest("hex"), { username: "paul", displayName: "Paul", createdAtMs: NOW - 1e7 });
store.set("EtsyMail_Operators/paul", { username: "paul", displayName: "Paul", role: "owner" });
const SBX = (id, outbox) => ({ id, receiptId: RID_S, scope: "order", sandbox: true, status: "open", threadId: null, link: "sandbox", createdAtMs: NOW - 5e5, v: 10, outbox, seen: [], sim: [], unread: 0, inboundCount: 0, customer: { name: "Cleo Real" } });
store.set("EtsyMail_OrderLinks/olsb_seed1", SBX("olsb_seed1", [{ id: "f1", text: "failed", status: "failed", atMs: NOW - 4e5 }, { id: "m1", text: "manual", status: "manual", atMs: NOW - 3e5 }, { id: "n1", text: "new", status: "new", atMs: NOW - 2e5 }]));

const prodPaths = Object.keys(PROD);
const snap = () => new Map([...store].map(([k, v]) => [k, canon(v)]));
const prodBefore = new Map(prodPaths.map(k => [k, canon(store.get(k))]));
const lookalikes = ["EtsyMail_OrderLinks/olsb_odd1", "EtsyMail_OrderLinks/ol_odd2"];
const prodIntact = () => prodPaths.filter(k => !store.has(k) || canon(store.get(k)) !== prodBefore.get(k));

const via = async (op, body) => {
  const n = writes.length;
  const r = await endpoint.handler({ httpMethod: "POST", headers: { "x-mail-station": KEY }, body: JSON.stringify(Object.assign({ op }, body)) });
  return { status: r.statusCode, body: JSON.parse(r.body || "{}"), wrote: writes.slice(n).map(w => w.path) };
};
// (the station's own "last seen" stamp is the sorter's connection record, not a customer's mail)
const allowedForSandbox = p => /^EtsyMail_OrderLinks\/olsb_/.test(p) || /^EtsyMail_OrderLinkMeta\//.test(p) || /^EtsyMail_OrderLinkStations\//.test(p);

(async () => {
  console.log("The registry");
  const fam = Families.server(), mailFam = fam.filter(f => /^EtsyMail/.test(f.collection || f.key || ""));
  check(mailFam.length === 1 && mailFam[0].key === "EtsyMail_OrderLinks" && mailFam[0].idPrefix === "olsb_" && mailFam[0].flag && mailFam[0].flag.sandbox === true, "the only mail family the wipe knows is EtsyMail_OrderLinks: id olsb_… AND sandbox:true");
  for (const p of ["EtsyMail_Threads/t", `EtsyMail_Threads/${T1}/messages/m1`, "EtsyMail_Drafts/draft_x", "EtsyMail_Jobs/j", "EtsyMail_Customers/c", "EtsyMail_Receipts/r", "EtsyMail_OrderLinkBuyers/b", "EtsyMail_Translations/t", "EtsyMail_OrderLinks/ol_real"]) {
    check(Families.ownerOf("firestore", p) === null, `a sandbox write to ${p.split("/").slice(0, 1)} is unclaimed by the registry, so the wipe guard would fail it`);
  }
  check(["EtsyMail_Config/x", "EtsyMail_OrderLinkMeta/bell"].every(p => (Families.ownerOf("firestore", p) || {}).kind === "protected"), "config and the change counter are on the keep list (never deleted)");

  console.log("The sandbox's own mail ops");
  const sb = { sandbox: true };
  let r = await via("ask", Object.assign({ receiptId: RID_S2, scope: "order", text: "Which chain length would you like?", orderNumber: RID_S2, buyerName: "Cleo Real", clientId: "c1" }, sb));
  check(r.status === 200 && r.wrote.length > 0 && r.wrote.every(allowedForSandbox), "ask: a rehearsal question writes only its own olsb_ engagement and the counter (" + [...new Set(r.wrote)].join(", ") + ")");
  const mine = r.body.id; check(/^olsb_/.test(mine || ""), "its engagement is olsb_…: " + mine);
  check(r.body.sandbox === true && !r.body.threadId, "it is linked to no real conversation");
  r = await via("ask", Object.assign({ receiptId: RID_P, scope: "order", text: "second", clientId: "c2", engagementId: `ol_${RID_P}_o_x1`, newQuestion: true }, sb));
  check(r.status === 200 && /^olsb_/.test(r.body.id || "") && r.wrote.every(allowedForSandbox), "ask from the sandbox for a real order, naming a real engagement: a sandbox one is made, the real one is not touched (" + r.body.id + ")");
  check(r.body.threadId === null && r.body.link === "sandbox", "and it is not linked to the real order's real conversation");
  const ops = [
    ["order", Object.assign({ receiptId: RID_S, scope: "order" }, sb)],
    ["order", Object.assign({ receiptId: RID_P, scope: "order" }, sb)],
    ["thread", Object.assign({ engagementId: mine }, sb)],
    ["sync", Object.assign({ n: -1, since: 0, full: true }, sb)],
    ["history_info", Object.assign({ receiptId: RID_S }, sb)],
    ["cancel", Object.assign({ engagementId: "olsb_seed1", itemId: "n1" }, sb)],   // (before retry: a sandbox retry "sends" everything unsent)
    ["retry", Object.assign({ engagementId: "olsb_seed1", itemId: "f1" }, sb)],
    ["copied", Object.assign({ engagementId: "olsb_seed1", itemId: "m1" }, sb)],
    ["sent", Object.assign({ engagementId: "olsb_seed1", itemId: "m1" }, sb)],
    ["read", Object.assign({ engagementId: "olsb_seed1" }, sb)],
    ["resolve", Object.assign({ engagementId: "olsb_seed1" }, sb)],
    ["reopen", Object.assign({ engagementId: "olsb_seed1" }, sb)],
    ["lang", Object.assign({ engagementId: "olsb_seed1", lang: "uk" }, sb)],
    ["simulate", Object.assign({ engagementId: "olsb_seed1", text: "Yes please" }, sb)],
    ["health", Object.assign({ fresh: true }, sb)]
  ];
  for (const [op, body] of ops) {
    r = await via(op, body);
    check(r.status === 200 && r.wrote.every(allowedForSandbox), `${op}: answered, wrote only sandbox engagements/counter (${[...new Set(r.wrote)].join(", ") || "nothing"})`);
  }
  const info = await via("history_info", Object.assign({ receiptId: RID_S }, sb));
  const tid = info.body.threads && info.body.threads[0] && info.body.threads[0].threadId;
  check(info.status === 200 && tid === T2 && info.wrote.length === 0, "history_info: the sandbox order's real buyer's history is READ (by design, Paul's 25 Sep rule), never written");
  r = await via("history", Object.assign({ receiptId: RID_S, threadId: T2 }, sb));
  check(r.status === 200 && r.body.messages.length === 2 && r.wrote.length === 0, "history: its messages are read, writing nothing");
  r = await via("history", Object.assign({ receiptId: RID_S, threadId: T1 }, sb));
  check(r.status === 404, "history cannot read a conversation that is not that buyer's (another customer's thread is refused)");
  check(etsy.calls === 0 && sbox.calls >= 0, `no Etsy call was made by any sandbox op (Etsy calls ${etsy.calls})`);
  check(prodIntact().length === 0, "after all of it every production mail document is byte-identical" + (prodIntact().length ? ": " + prodIntact().join(", ") : ""));

  console.log("An engagement of the other world is refused");
  const REAL = `ol_${RID_P}_o_x1`;
  for (const op of ["retry", "cancel", "copied", "sent", "read", "resolve", "reopen", "lang", "link_url", "simulate"]) {
    const body = { engagementId: REAL, itemId: "i2", lang: "uk", url: "https://www.etsy.com/your/conversations/111", text: "x" };
    let a = await via(op, Object.assign({}, body, { sandbox: true }));
    check(a.status === 404 && a.wrote.length === 0, `${op}: a sandbox page naming a real engagement is refused (404), nothing written`);
    a = await via(op, Object.assign({}, body, { engagementId: "olsb_seed1", sandbox: false }));
    check(a.status === 404 && a.wrote.length === 0, `${op}: a real page naming a sandbox engagement is refused (404), nothing written`);
  }
  const seedBefore = canon(store.get("EtsyMail_OrderLinks/olsb_seed1"));
  let a = await via("lang", { engagementId: REAL, lang: "de", sandbox: false });
  check(a.status === 200 && a.wrote.every(p => p === `EtsyMail_OrderLinks/${REAL}` || /OrderLinkMeta/.test(p)), "the same op in its own world still works (a real page, a real engagement)");
  prodBefore.set(`EtsyMail_OrderLinks/${REAL}`, canon(store.get(`EtsyMail_OrderLinks/${REAL}`)));   // (that op was meant to change it)
  check(canon(store.get("EtsyMail_OrderLinks/olsb_seed1")) === seedBefore, "and it did not touch the sandbox engagement");
  a = await via("lang", { engagementId: REAL, lang: "uk" });
  check(a.status === 200, "a page that does not say its world (an older page) is not judged");
  prodBefore.set(`EtsyMail_OrderLinks/${REAL}`, canon(store.get(`EtsyMail_OrderLinks/${REAL}`)));

  console.log("The wipe");
  const sandboxMade = [...store.keys()].filter(k => /^EtsyMail_OrderLinks\/olsb_/.test(k) && (store.get(k) || {}).sandbox === true);
  check(sandboxMade.length >= 3, `the sandbox holds its engagements before the wipe (${sandboxMade.length})`);
  const live = async b => { const r = await lib.handler({ httpMethod: "POST", headers: {}, body: JSON.stringify(b), queryStringParameters: {} }); return { status: r.statusCode, body: JSON.parse(r.body || "{}") }; };
  let w = await live({ op: "sandboxReset", sandbox: true }), calls = 1;
  while (w.body.more && calls < 100) { w = await live({ op: "sandboxReset", sandbox: true }); calls++; }
  check(w.status === 200 && w.body.ok === true && w.body.more === false, "the real sandboxReset ran to the end: " + JSON.stringify(w.body).slice(0, 120));
  const left = [...store.keys()].filter(k => /^EtsyMail_OrderLinks\/olsb_/.test(k) && (store.get(k) || {}).sandbox === true);
  check(left.length === 0, "every sandbox engagement is gone (left: " + left.join(", ") + ")");
  const gone = prodIntact();
  check(gone.length === 0, `every production mail document is byte-identical after the wipe (${prodPaths.length} checked)` + (gone.length ? ": " + gone.join(", ") : ""));
  check(lookalikes.every(k => store.has(k)), "the lookalikes survived: olsb_ saying sandbox:false, ol_ saying sandbox:true");
  check([...store.keys()].some(k => k.startsWith(`EtsyMail_Threads/${T1}/messages/`)) && store.has(`EtsyMail_Drafts/draft_${T1}`) && store.has("EtsyMail_Jobs/job1"), "threads, messages, the queued reply box and the scrape job are all still there");

  console.warn = realWarn; console.error = realError;
  console.log(fails.length ? `\nFAILED: ${fails.length}\n- ` + fails.join("\n- ") : "\nmail-isolation-guard OK");
  process.exit(fails.length ? 1 : 0);
})().catch(e => { console.warn = realWarn; console.error = realError; console.error(e); console.log(warn.slice(-5).join("\n")); process.exit(1); });
