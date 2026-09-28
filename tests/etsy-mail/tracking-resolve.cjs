// Tests for netlify/functions/_etsyMailTrackingResolve.js and the Send
// safety net in etsyMailDraftSend.js: a reply with two different tracking
// numbers keeps only the real one (or each one, when all are real packages
// the reply is about).
//
//   node tests/etsy-mail/tracking-resolve.cjs
//
// No network and no paid AI: the model is a stub, Firestore is an in-memory
// fake, and node-fetch is replaced by a stub that refuses every request.
"use strict";
const assert = require("node:assert/strict");
const path = require("node:path");
const Module = require("node:module");

delete process.env.ANTHROPIC_API_KEY;   // the default model call can never run

// ── In-memory Firestore ──────────────────────────────────────────────────
const store = new Map();   // "coll/id" (or "coll/id/sub/id") → data
const writes = [];
function docRef(p) {
  return {
    path: p, id: p.split("/").pop(),
    get: async () => snapOf(p),
    set: async (data, opts) => setDoc(p, data, opts),
    collection: (sub) => collRef(p + "/" + sub)
  };
}
function snapOf(p) {
  const d = store.get(p);
  return { id: p.split("/").pop(), exists: d !== undefined, data: () => (d === undefined ? undefined : JSON.parse(JSON.stringify(d))) };
}
function setDoc(p, data, opts) {
  writes.push({ path: p, data });
  const clean = JSON.parse(JSON.stringify(data, (k, v) => (v && v.__fv ? undefined : v)));
  store.set(p, opts && opts.merge ? { ...(store.get(p) || {}), ...clean } : clean);
}
function collRef(p) {
  const q = { filters: [], order: null, lim: 1e9 };
  const api = {
    doc: (id) => docRef(p + "/" + id),
    where: (f, op, v) => { q.filters.push([f, v]); return api; },
    orderBy: (f, dir) => { q.order = [f, dir]; return api; },
    limit: (n) => { q.lim = n; return api; },
    get: async () => {
      let docs = [...store.entries()].filter(([k]) => k.startsWith(p + "/") && !k.slice(p.length + 1).includes("/"))
        .map(([k, d]) => ({ id: k.split("/").pop(), data: () => JSON.parse(JSON.stringify(d)), raw: d }))
        .filter(x => q.filters.every(([f, v]) => String(x.raw[f]) === String(v)));
      if (q.order) docs.sort((a, b) => (q.order[1] === "desc" ? -1 : 1) * ((a.raw[q.order[0]] || 0) - (b.raw[q.order[0]] || 0)));
      docs = docs.slice(0, q.lim);
      return { empty: !docs.length, size: docs.length, docs, forEach: (fn) => docs.forEach(fn) };
    }
  };
  return api;
}
const fakeDb = {
  collection: (c) => collRef(c),
  getAll: async (...refs) => refs.map(r => snapOf(r.path)),
  runTransaction: async (fn) => fn({ get: async (r) => snapOf(r.path), set: (r, d, o) => setDoc(r.path, d, o) })
};
const firestore = () => fakeDb;
firestore.FieldValue = { serverTimestamp: () => ({ __fv: "ts" }), delete: () => ({ __fv: "del" }) };
firestore.Timestamp = { fromMillis: (ms) => ({ toMillis: () => ms }) };
const fakeAdmin = { firestore, storage: () => ({ bucket: () => ({}) }) };

let fetchCalls = 0;
const realLoad = Module._load;
Module._load = function (req, parent, ...rest) {
  if (/(^|\/)firebaseAdmin(\.js)?$/.test(req) || req === "firebase-admin") return fakeAdmin;
  if (req === "node-fetch") return async () => { fetchCalls++; throw new Error("no network in this test"); };
  return realLoad.call(this, req, parent, ...rest);
};
const FN = path.resolve(__dirname, "../../netlify/functions");
const T = require(path.join(FN, "_etsyMailTrackingResolve.js"));
const S = require(path.join(FN, "etsyMailDraftSend.js"));

// ── Fixtures (made-up numbers in real shapes) ────────────────────────────
const A = "9214490362894300000011";            // USPS tracking numbers
const B = "9214490362894300000028";
const C = "9214490362894300000035";
const barcode = (c) => "420941115678" + c;      // "420" + ZIP+4 + number
const LX1 = "LX000000001NL", LX2 = "LX000000002NL";
const at = (d) => d + "T12:00:00-04:00";
const cache = {
  voided : { status: "In Transit", statusKey: "in_transit", carrier: "usps", events: [
    { at: at("2026-08-31"), title: "Postage refunded" }, { at: at("2026-08-28"), title: "Postage Purchased" }] },
  label  : { status: "Pre-Shipment", statusKey: "pre_shipment", carrier: "usps", events: [{ at: at("2026-08-20"), title: "Postage Purchased" }] },
  moving : { status: "In Transit", statusKey: "in_transit", carrier: "usps", events: [
    { at: at("2026-08-22"), title: "Postage Purchased" }, { at: at("2026-08-23"), title: "Accepted at USPS Origin Facility", location: "Buffalo, NY" }] },
  moving2: { status: "In Transit", statusKey: "in_transit", carrier: "usps", events: [
    { at: at("2026-08-22"), title: "Postage Purchased" }, { at: at("2026-08-24"), title: "Arrived at USPS Regional Facility", location: "Albany, NY" }] },
  deliveredApr: { status: "Delivered", statusKey: "delivered", carrier: "chitchats", events: [
    { at: at("2026-04-01"), title: "Postage Purchased" }, { at: at("2026-04-14"), title: "The item has been delivered successfully", status: "delivered" }] },
  deliveredMay: { status: "Delivered", statusKey: "delivered", carrier: "chitchats", events: [
    { at: at("2026-05-01"), title: "Postage Purchased" }, { at: at("2026-05-11"), title: "The item has been delivered successfully", status: "delivered" }] }
};
const ship = (code, receiptId, day) => ({ code, receiptId, carrier: "USPS", shippedAt: Date.parse(at(day)), orderedAt: Date.parse(at("2026-08-01")) });
const noModel = () => { throw new Error("the AI must not be asked here"); };

let passed = 0;
async function test(name, fn) {
  await fn();
  passed++;
  console.log("ok -", name);
}

(async () => {
  await test("the same number twice, also as a 420 barcode, is left alone", async () => {
    const text = "Tracking: https://tools.usps.com/go/TrackConfirmAction?tLabels=" + barcode(A) + " (" + A + ")";
    assert.deepEqual(T.codesInText(text), [A]);
    assert.equal(T.normCode(barcode(A)), A);
    const res = await T.resolveTrackingConflict({ imageCodes: [barcode(A), A], textCodes: T.codesInText(text), askModel: noModel });
    assert.equal(res, null);
  });

  await test("numbers are read in every form, and order numbers are not numbers", () => {
    const grouped = A.replace(/(\d{4})(?=\d)/g, "$1 ");
    assert.deepEqual(T.codesInText("Your number is " + grouped + "."), [A]);
    assert.deepEqual(T.codesInText("Order 4151190949 shipped; call 1-800-275-8777."), []);
    assert.deepEqual(T.codesInText(A + " 2026"), [A]);
    assert.deepEqual(T.codesInText("Track " + LX1.toLowerCase() + " or https://www.ups.com/track?tracknum=1Z999AA10123456784").sort(), ["1Z999AA10123456784", LX1].sort());
    assert.deepEqual(T.codesInText("ref 9214-4903-6289-4300-0000-11", [A]), [A]);
  });

  await test("a number on the buyer's order beats one on no order", async () => {
    const res = await T.resolveTrackingConflict({
      imageCodes: [A], textCodes: [B], shipments: [ship(A, "111", "2026-08-22")], cache: { [A]: cache.moving }, askModel: noModel
    });
    assert.equal(res.method, "rules");
    assert.deepEqual(res.kept, [A]);
    assert.deepEqual(res.dropped.map(d => d.code), [B]);
    assert.match(res.dropped[0].reason, /not on any/);
  });

  await test("a replaced label: the older label on the same order that was never scanned goes", async () => {
    const res = await T.resolveTrackingConflict({
      imageCodes: [A, B], shipments: [ship(A, "111", "2026-08-20"), ship(B, "111", "2026-08-22")],
      cache: { [A]: cache.label, [B]: cache.moving }, askModel: noModel
    });
    assert.equal(res.method, "rules");
    assert.deepEqual(res.kept, [B]);
    assert.equal(res.dropped[0].code, A);
    assert.match(res.dropped[0].reason, /replaced label/);
    assert.match(res.keptReason, /usps scanned/i);
  });

  await test("a scanned label beats a never-scanned one bought before it, across orders", async () => {
    const res = await T.resolveTrackingConflict({
      imageCodes: [A], textCodes: [B], shipments: [ship(A, "111", "2026-08-20"), ship(B, "222", "2026-08-22")],
      cache: { [A]: cache.label, [B]: cache.moving }, askModel: noModel
    });
    assert.deepEqual(res.kept, [B]);
    assert.match(res.dropped[0].reason, /never scanned/);
  });

  await test("a cancelled label (postage refunded) loses to the number the sender typed", async () => {
    const res = await T.resolveTrackingConflict({
      imageCodes: [barcode(A)], textCodes: [B], typedCodes: [B], shipments: [ship(barcode(A), "111", "2026-08-28")],
      cache: { [A]: cache.voided }, askModel: noModel
    });
    assert.equal(res.method, "rules");
    assert.deepEqual(res.kept, [B]);
    assert.deepEqual(res.dropped, [{ code: A, reason: "label cancelled, postage refunded" }]);
    assert.equal(T.describeResolution(res), "Kept tracking 9214…0028 (the number typed into the reply); removed 9214…0011 (label cancelled, postage refunded)");
  });

  await test("two delivered orders: the AI is asked once and its pick is used", async () => {
    const calls = [];
    const askModel = async ({ system, user }) => {
      calls.push(user);
      assert.match(system, /JSON only/);
      return 'Sure: {"keep":["' + LX2 + '"],"keep_reason":"the package she asked about","drop":[{"code":"' + LX1 + '","reason":"other order, already delivered"}],"why":"She asks about the May parcel."}';
    };
    const res = await T.resolveTrackingConflict({
      imageCodes: [LX2, LX1], shipments: [{ code: LX1, receiptId: "401", shippedAt: at("2026-04-01") }, { code: LX2, receiptId: "404", shippedAt: at("2026-05-01") }],
      cache: { [LX1]: cache.deliveredApr, [LX2]: cache.deliveredMay },
      customerText: "The necklace shipped May 1 still hasn't come.", draftText: "I've pulled the tracking for your package.", askModel
    });
    assert.equal(calls.length, 1);
    assert.match(calls[0], new RegExp(LX1 + "[\\s\\S]*on order 401"));
    assert.match(calls[0], /still hasn't come/);
    assert.equal(res.method, "ai");
    assert.deepEqual(res.kept, [LX2]);
    assert.deepEqual(res.dropped, [{ code: LX1, reason: "other order, already delivered" }]);
    assert.equal(res.keptReason, "the package she asked about");
  });

  const ambiguous = { imageCodes: [A], textCodes: [B], typedCodes: [B], shipments: [ship(A, "111", "2026-08-14")], cache: { [A]: cache.moving } };
  await test("a model failure falls back to the ranking (the sender's number first)", async () => {
    const res = await T.resolveTrackingConflict({ ...ambiguous, askModel: async () => { throw new Error("overloaded"); } });
    assert.equal(res.method, "fallback");
    assert.deepEqual(res.kept, [B]);
    assert.match(res.why, /overloaded/);
    // Without a typed number: the one on the order, then scanned, then newest.
    const res2 = await T.resolveTrackingConflict({ ...ambiguous, typedCodes: [], history: [{ direction: "outbound", text: "New label: " + B }], askModel: async () => { throw new Error("x"); } });
    assert.deepEqual(res2.kept, [A]);
  });

  await test("a model that never answers times out and falls back", async () => {
    const res = await T.resolveTrackingConflict({ ...ambiguous, timeoutMs: 30, askModel: () => new Promise(() => {}) });
    assert.equal(res.method, "fallback");
    assert.match(res.why, /timed out/);
  });

  await test("a bad model answer is rejected", async () => {
    for (const answer of [
      "I think the second one.",
      '{"keep":["9214490362894399999999"],"drop":[]}',
      '{"keep":["' + A + '"],"drop":[{"code":"' + A + '","reason":"x"}]}',
      '{"keep":[],"drop":[{"code":"' + A + '","reason":"x"}]}'
    ]) {
      const res = await T.resolveTrackingConflict({ ...ambiguous, askModel: async () => answer });
      assert.equal(res.method, "fallback", answer);
      assert.equal(res.kept.length, 1);
    }
  });

  await test("a customer asking about two orders: the AI may keep both", async () => {
    const res = await T.resolveTrackingConflict({
      imageCodes: [A, C], shipments: [ship(A, "111", "2026-08-22"), ship(C, "333", "2026-08-23")],
      cache: { [A]: cache.moving, [C]: cache.moving2 },
      askModel: async () => '{"keep":["' + A + '","' + C + '"],"drop":[],"why":"She asked about both orders."}'
    });
    assert.equal(res.method, "ai");
    assert.deepEqual(res.kept, [A, C]);
    assert.deepEqual(res.dropped, []);
  });

  await test("one order sent in two boxes, both scanned: both kept without asking the AI", async () => {
    const res = await T.resolveTrackingConflict({
      imageCodes: [A, B], shipments: [ship(A, "111", "2026-08-22"), ship(B, "111", "2026-08-22")],
      cache: { [A]: cache.moving, [B]: cache.moving2 }, askModel: noModel
    });
    assert.equal(res.method, "rules");
    assert.deepEqual(res.kept, [A, B]);
    assert.deepEqual(res.dropped, []);
    assert.match(res.why, /2 boxes/);
  });

  await test("the words get the kept number: barcode, grouped and link forms", () => {
    const res = { kept: [B], dropped: [{ code: A, reason: "x" }] };
    const text = "Here it is: https://tools.usps.com/go/TrackConfirmAction?tLabels=" + barcode(A) + ".\nNumber " +
      A.replace(/(\d{4})(?=\d)/g, "$1 ") + ", also " + A + ".";
    const out = T.fixTrackingText(text, res);
    assert.equal(out, "Here it is: https://tools.usps.com/go/TrackConfirmAction?tLabels=" + B + ".\nNumber " + B + ", also " + B + ".");
    assert.deepEqual(T.codesInText(out, [A]), [B]);
  });

  await test("a link for another carrier is replaced whole; an unmentioned kept number takes the dropped one's place", () => {
    const out = T.fixTrackingText("Track it at https://tools.usps.com/go/TrackConfirmAction?tLabels=" + A + " today", { kept: ["1Z999AA10123456784"], dropped: [{ code: A }] });
    assert.equal(out, "Track it at https://www.ups.com/track?tracknum=1Z999AA10123456784 today");
    const out2 = T.fixTrackingText("Numbers: " + A + " and " + LX1 + ".", { kept: [LX1, LX2], dropped: [{ code: A }] });
    assert.equal(out2, "Numbers: " + LX2 + " and " + LX1 + ".");
  });

  await test("with every kept number already in the words, a dropped one is taken out cleanly", () => {
    const out = T.fixTrackingText("Your boxes: " + LX1 + " and " + LX2 + ".\nOld label:\n" + A + "\n\nThanks", { kept: [LX1, LX2], dropped: [{ code: A }] });
    assert.ok(!T.mentions(out, A));
    assert.equal(out, "Your boxes: " + LX1 + " and " + LX2 + ".\nOld label:\n\nThanks");
  });

  await test("the dropped number's picture comes off, by code or by storage path", () => {
    const res = { kept: [B], dropped: [{ code: A }] };
    const out = T.dropTrackingAttachments([
      { type: "tracking_image", trackingCode: barcode(A) },
      { type: "image", storagePath: "etsymail/tracking/" + A + ".png" },
      { type: "tracking_image", trackingCode: B },
      { type: "image", storagePath: "etsymail-collateral/sheet.png" }
    ], res);
    assert.deepEqual(out.map(a => a.trackingCode || a.storagePath), [B, "etsymail-collateral/sheet.png"]);
  });

  await test("Send takes the dead label off, keeps the typed number and reuses its decision on a retry", async () => {
    const threadId = "etsy_conv_1000000001";
    const draftId = "draft_" + threadId;
    store.set("EtsyMail_Threads/" + threadId, { buyerUserId: "555" });
    store.set("EtsyMail_Threads/" + threadId + "/messages/m1", { direction: "inbound", text: "Has my order shipped yet?", timestamp: 1 });
    store.set("EtsyMail_Receipts/9001", { buyer_user_id: "555", created_timestamp: 1787324216,
      raw: { receipt_id: 9001, created_timestamp: 1787324216, shipments: [{ tracking_code: barcode(A), carrier_name: "USPS", shipment_notification_timestamp: 1787932800 }] } });
    store.set("EtsyMail_TrackingCache/" + barcode(A), cache.voided);
    store.set("EtsyMail_Drafts/" + draftId, { threadId, text: "Yes! Your order shipped. Tracking below.",
      trackingImages: [{ trackingCode: barcode(A), status: "ready", imageStoragePath: "etsymail/tracking/" + barcode(A) + ".png" }] });
    const chip = { attachmentId: "trk_" + barcode(A), type: "tracking_image", trackingCode: barcode(A), proxyUrl: "/t?trackingCode=" + barcode(A) };
    const text = "It was delivered. Tracking: https://tools.usps.com/go/TrackConfirmAction?tLabels=" + barcode(B);

    const first = await S.resolveSendTracking({ threadId, draftId, text, claimText: text, attachments: [chip], startedAt: Date.now() });
    assert.equal(first.resolution.method, "rules");
    assert.deepEqual(first.resolution.kept, [B]);
    assert.deepEqual(first.resolution.dropped.map(d => d.code), [A]);
    assert.deepEqual(first.attachments, []);                 // the kept number has no picture yet
    assert.equal(first.text, text);                          // the words already carry the kept number
    assert.deepEqual(first.pending, []);                     // drawing it was refused (no network here)
    const saved = store.get("EtsyMail_Drafts/" + draftId);
    assert.equal(saved.trackingResolution.stage, "send");
    assert.deepEqual(saved.trackingImages, []);

    // A retry with the kept number's picture ready reuses the decision.
    saved.trackingImages = [{ trackingCode: B, status: "ready", imageStoragePath: "etsymail/tracking/" + B + ".png" }];
    const again = await S.resolveSendTracking({ threadId, draftId, text, attachments: [chip], startedAt: Date.now() });
    assert.equal(again.resolution.at, saved.trackingResolution.at);
    assert.deepEqual(again.attachments.map(a => a.trackingCode), [B]);

    // One number only: nothing is read or changed.
    const before = writes.length;
    assert.equal(await S.resolveSendTracking({ threadId, draftId, text, attachments: [], startedAt: Date.now() }), null);
    assert.equal(writes.length, before);
    assert.equal(fetchCalls, 1);                             // only the refused tracking-picture request
  });

  console.log(passed + " tracking-number tests passed");
})().catch(e => { console.error(e); process.exit(1); });
