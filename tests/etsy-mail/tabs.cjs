// Tests for the new inbox layout ("smart inbox") in etsy-mail-1.html and
// for the snapshot's waiting-state calculation.
//
//   node tests/etsy-mail/tabs.cjs
//
// Nothing here touches the network: the page's pure blocks (TABS-CORE and
// TABS-CHIPS, plus tsToMs and the layout choice) run in a vm context, and
// etsyMailSnapshot.js loads with its Firestore and auth modules stubbed.
"use strict";
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const Module = require("node:module");

const ROOT = path.resolve(__dirname, "../..");
const PAGE = path.join(ROOT, "etsy-mail-1.html");
const FN_DIR = path.join(ROOT, "netlify/functions");
const html = fs.readFileSync(PAGE, "utf8");

let passed = 0;
function test(name, fn) {
  fn();
  passed++;
  console.log("PASS " + name);
}
const plain = x => JSON.parse(JSON.stringify(x));

// ── Extract the page's blocks ─────────────────────────────────────────
function between(begin, end) {
  const a = html.indexOf(begin);
  assert(a >= 0, "marker not found: " + begin);
  const b = html.indexOf(end, a + begin.length);
  assert(b > a, "marker not found: " + end);
  return html.slice(a + begin.length, b);
}
function functionAt(head) {
  // From "    function name(" to the first closing brace at the same indent.
  const a = html.indexOf(head);
  assert(a >= 0, "not found: " + head);
  const indent = head.match(/^ */)[0];
  const close = "\n" + indent + "}\n";
  const b = html.indexOf(close, a);
  assert(b > a, "end not found: " + head);
  return html.slice(a, b + close.length);
}
const TS_TO_MS = functionAt("    function tsToMs(v) {");
const CORE = between("/* TABS-CORE-BEGIN */", "/* TABS-CORE-END */");
const CHIPS = between("/* TABS-CHIPS-BEGIN */", "/* TABS-CHIPS-END */");
const EXPORTS = ["tsToMs", "DAY", "SKEW_MS", "isArchived", "aiState", "hasSendProblem", "isAwaiting", "isNew",
  "isWaitingOnCustomer", "isDone", "isActiveRush", "aiFresh", "intentFresh", "lastReplyByAi", "mainFolderOf",
  "INTENT_MIN_CONF", "labelOverridden", "CHIP_DEFS", "smartChipsFor", "smartChipTip", "smartFilterDefs",
  "smartHasFilter", "SMART_VIEWS", "inSmartView"];
const T = (() => {
  const ctx = vm.createContext({});
  vm.runInContext(TS_TO_MS + "\n" + CORE + "\n" + CHIPS + "\nglobalThis.__T = { " + EXPORTS.join(", ") + " };", ctx,
    { filename: "etsy-mail-1.html#TABS" });
  return ctx.__T;
})();

const NOW = Date.UTC(2026, 8, 26, 15, 0, 0);
const MIN = 60e3, H = 3600e3, D = 86400e3;
const folder = t => T.mainFolderOf(t, NOW);
const chips = t => T.smartChipsFor(t, NOW);
const ids = t => chips(t).map(k => k.id);
const chip = (t, id) => chips(t).find(k => k.id === id) || null;
const hasFilter = (t, f) => T.smartHasFilter(t, f, chips(t));
const view = id => T.SMART_VIEWS.find(v => v.id === id);
const inView = (t, id) => T.inSmartView(t, view(id), NOW, chips(t));
const secs = ms => ({ _seconds: Math.floor(ms / 1000), _nanoseconds: 0 });   // Firestore's JSON form

// ── Folder rules: the nine cases of the design (section 7) ────────────
test("1. customer spoke last: Inbox, and New while unread", () => {
  const t = { id: "c1", status: "etsy_scraped", unread: true,
    lastInboundAt: NOW - 1 * H, awaitingReplySince: NOW - 1 * H, lastOutboundAt: NOW - 26 * H };
  assert.equal(folder(t), "x_inbox");
  assert.equal(T.isNew(t, NOW), true);
  const read = { ...t, unread: false };
  assert.equal(folder(read), "x_inbox");
  assert.equal(T.isNew(read, NOW), false);
  // The same conversation written before awaitingReplySince existed.
  const old = { ...t, awaitingReplySince: undefined };
  assert.equal(folder(old), "x_inbox");
  assert.equal(T.isNew(old, NOW), true);
});

test("2. AI auto-reply delivered: Waiting on customer, with an AI replied chip while unread", () => {
  const sent = NOW - 2 * H;
  const t = { id: "c2", status: "auto_replied", sendOrigin: "auto", unread: true,
    lastInboundAt: NOW - 3 * H, lastOutboundAt: sent, lastOperatorReplyAt: sent + 20e3,
    lastAutoDecision: "auto_send_confirmed", lastAutoDecisionAt: sent + 20e3, lastAutoProcessedInboundAt: NOW - 3 * H };
  assert.equal(folder(t), "x_waiting");
  assert.equal(T.isNew(t, NOW), false);
  assert.equal(chip(t, "ai_replied").label, "AI replied");
  assert.equal(chip({ ...t, unread: false }, "ai_replied"), null);
  // Before the scrape records the message, and before the send clears
  // awaitingReplySince: the send time alone moves it out of Inbox.
  const early = { ...t, lastOutboundAt: undefined, awaitingReplySince: NOW - 3 * H };
  assert.equal(folder(early), "x_waiting");
  // A reply sent by hand is not an AI reply.
  assert.equal(chip({ ...t, sendOrigin: "manual" }, "ai_replied"), null);
  // A later reply by hand (after the AI's) is not an AI reply either.
  assert.equal(chip({ ...t, lastOperatorReplyAt: sent + 30 * MIN }, "ai_replied"), null);
});

test("3. unverified send: stays in Inbox with Send problem", () => {
  const at = NOW - 1 * H;
  const t = { id: "c3", status: "pending_human_review", unread: false,
    lastInboundAt: NOW - 2 * H, awaitingReplySince: NOW - 2 * H,
    lastAutoDecision: "human_review_after_unverified_send", lastAutoDecisionAt: at, lastOperatorReplyAt: at };
  assert.equal(folder(t), "x_inbox");
  assert.equal(ids(t)[0], "send_problem");
  assert.equal(chip(t, "send_problem").label, "Send problem");
  assert.match(chip(t, "send_problem").why, /was not confirmed on Etsy/);
  // Also when the scrape already saw an older reply of ours after the message.
  assert.equal(folder({ ...t, lastOutboundAt: NOW - 90 * MIN }), "x_inbox");
  // Without awaitingReplySince (older document) as well.
  assert.equal(folder({ ...t, awaitingReplySince: undefined }), "x_inbox");
  // Resolved by a later reply sent from the inbox ...
  const fixed = { ...t, lastOperatorReplyAt: at + 10 * MIN };
  assert.equal(chip(fixed, "send_problem"), null);
  assert.equal(folder(fixed), "x_waiting");
  // ... or by the scrape seeing our reply on Etsy.
  const seen = { ...t, lastOutboundAt: at + 1 * MIN };
  assert.equal(chip(seen, "send_problem"), null);
  assert.equal(folder(seen), "x_waiting");
});

test("4. archived thread with a new customer message: Inbox, not Done", () => {
  const t = { id: "c4", status: "archived", archivedAt: NOW - 2 * D, unread: true,
    lastInboundAt: NOW - 1 * H, awaitingReplySince: NOW - 1 * H, lastOutboundAt: NOW - 3 * D };
  assert.equal(folder(t), "x_inbox");
  assert.equal(T.isDone(t, NOW), false);
  assert.equal(T.isNew(t, NOW), true);
  // Archived with nothing new: Done, even though the customer spoke last.
  const quiet = { id: "c4b", status: "archived", archivedAt: NOW - 2 * D,
    lastInboundAt: NOW - 3 * D, lastOutboundAt: NOW - 4 * D };
  assert.equal(folder(quiet), "x_done");
  assert.equal(T.isWaitingOnCustomer(quiet, NOW), false);
});

test("5. old-format document: customer last 3 days ago is Inbox; 20 days ago only when pending_human_review", () => {
  const t3 = { id: "c5", status: "etsy_scraped", lastInboundAt: secs(NOW - 3 * D), lastOutboundAt: secs(NOW - 4 * D) };
  assert.equal(folder(t3), "x_inbox");
  const t20 = { id: "c5b", status: "etsy_scraped", lastInboundAt: secs(NOW - 20 * D), lastOutboundAt: secs(NOW - 21 * D) };
  assert.notEqual(folder(t20), "x_inbox");
  // The customer spoke last, so it is not "Waiting on customer" either.
  assert.equal(folder(t20), "x_all");
  assert.equal(folder({ ...t20, status: "pending_human_review" }), "x_inbox");
  // Never answered at all.
  assert.equal(folder({ id: "c5c", lastInboundAt: secs(NOW - 2 * D) }), "x_inbox");
});

test("6. equal timestamps: Inbox", () => {
  const t = { id: "c6", lastInboundAt: NOW - 1 * H, lastOutboundAt: NOW - 1 * H };
  assert.equal(folder(t), "x_inbox");
  assert.equal(folder({ ...t, awaitingReplySince: NOW - 1 * H }), "x_inbox");
});

test("7. rush or completed-sale thread with a new question: Inbox", () => {
  const rush = { id: "c7", status: "production_rush", productionRush: { acceptedAt: NOW - 5 * D }, unread: true,
    lastInboundAt: NOW - 1 * H, awaitingReplySince: NOW - 1 * H, lastOutboundAt: NOW - 2 * D };
  assert.equal(folder(rush), "x_inbox");
  assert.ok(ids(rush).includes("rush"));
  const sold = { id: "c7b", status: "sales_completed", salesCompletedAt: NOW - 3 * D,
    lastInboundAt: NOW - 30 * MIN, lastOutboundAt: NOW - 3 * D };
  assert.equal(folder(sold), "x_inbox");
  assert.equal(chip(sold, "sold").label, "Listing sold");
  // A removed rush is no longer a rush.
  assert.equal(chip({ ...rush, status: "pending_human_review", productionRush: { acceptedAt: NOW - 5 * D, removedAt: NOW - 1 * D } }, "rush"), null);
});

test("8. intent read before the latest message is dimmed; confidence 0.6 is hidden", () => {
  const base = { id: "c8", status: "pending_human_review", lastInboundAt: NOW - 1 * H, awaitingReplySince: NOW - 1 * H,
    intentClassification: "post_purchase", intentConfidence: 0.9 };
  const stale = { ...base, intentClassifiedAt: NOW - 5 * H };
  const k = chip(stale, "intent");
  assert.equal(k.stale, true);
  assert.equal(k.tone, "stale");
  assert.equal(k.label, "Post-purchase (earlier message)");
  assert.equal(hasFilter(stale, "intent:post_purchase"), false);
  assert.equal(inView(stale, "x_v_post"), false);
  const fresh = { ...base, intentClassifiedAt: NOW - 30 * MIN };
  assert.equal(chip(fresh, "intent").label, "Post-purchase 90%");
  assert.equal(chip(fresh, "intent").tone, "purple");
  assert.equal(hasFilter(fresh, "intent:post_purchase"), true);
  assert.equal(inView(fresh, "x_v_post"), true);
  assert.equal(chip({ ...fresh, intentConfidence: 0.6 }, "intent"), null);
  assert.equal(chip({ ...fresh, intentConfidence: 0.7 }, "intent").label, "Post-purchase 70%");
  assert.equal(T.INTENT_MIN_CONF, 0.7);
});

test("9. refund flag from the AI draft: no chip; from the customer's words: chip", () => {
  const base = { id: "c9", status: "pending_human_review", lastInboundAt: NOW - 1 * H, awaitingReplySince: NOW - 1 * H,
    refundFlaggedAt: NOW - 30 * MIN };
  const draft = { ...base, refundFlaggedReason: 'draft:"refund"' };
  assert.equal(chip(draft, "refund"), null);
  assert.equal(hasFilter(draft, "refund"), false);
  assert.equal(inView(draft, "x_v_refund"), false);
  const customer = { ...base, refundFlaggedReason: 'inbound:"return it"' };
  const k = chip(customer, "refund");
  assert.equal(k.label, "Refund/return talk");
  assert.match(k.why, /The customer wrote “return it”/);
  assert.equal(inView(customer, "x_v_refund"), true);
  // Flagged before the customer's latest message: dimmed.
  const older = { ...customer, refundFlaggedAt: NOW - 2 * H };
  assert.equal(chip(older, "refund").stale, true);
  assert.equal(hasFilter(older, "refund"), false);
});

// ── Labels ────────────────────────────────────────────────────────────
test("rows show the two most important labels; dimmed labels come after fresh ones", () => {
  const at = NOW - 1 * H;
  const t = { id: "k1", status: "production_rush", productionRush: { acceptedAt: NOW - 2 * D },
    lastInboundAt: NOW - 2 * H, awaitingReplySince: NOW - 2 * H,
    lastAutoDecision: "human_review_after_partial_send", lastAutoDecisionAt: at,
    refundFlaggedAt: NOW - 90 * MIN, refundFlaggedReason: 'inbound:"refund"',
    etsyOrderId: "4170252963", etsyHeadingBadge: "Help request", riskFlags: ["image_attached"] };
  assert.deepEqual(plain(ids(t)), ["send_problem", "refund", "rush", "order", "help", "photo"]);
  assert.deepEqual(plain(ids(t).slice(0, 2)), ["send_problem", "refund"]);
  assert.equal(chip(t, "order").label, "Order …2963");
  assert.match(chip(t, "send_problem").why, /only partly went out/);
  // An intent read before the latest message sorts after every fresh label.
  const s = { id: "k2", lastInboundAt: NOW - 1 * H, etsyOrderId: "1234567",
    intentClassification: "sales_lead", intentConfidence: 0.95, intentClassifiedAt: NOW - 3 * H };
  assert.deepEqual(plain(ids(s)), ["order", "intent"]);
});

test("labels carry no source letters; the tooltip names the source in words", () => {
  const t = { id: "k3", status: "sales_quote", salesStage: "quote", lastInboundAt: NOW - 1 * H, awaitingReplySince: NOW - 1 * H,
    etsyOrderId: "99", intentClassification: "sales_lead", intentConfidence: 0.8, intentClassifiedAt: NOW - 10 * MIN,
    lastAutoDecision: "human_review", lastAutoProcessedInboundAt: NOW - 1 * H, aiDraftStatus: "ready" };
  const list = chips(t);
  assert.deepEqual(plain(list.map(k => k.label)), ["AI: needs you", "AI draft ready", "Order …99", "Sales lead 80%", "Sales: quote"]);
  for (const k of list) {
    assert.doesNotMatch(k.label, /^(E|S|R|AI)\s*[·|:-]?\s*$|^(E|S|R)\s|\s(E|S|R|AI)$|\[(E|S|R|AI)\]/, k.label);
    assert.match(T.smartChipTip(k), /Source: (Etsy|the inbox|the AI|a rule that reads the customer's words)\.$/);
  }
  assert.match(T.smartChipTip(chip(t, "order")), /Source: Etsy\.$/);
  assert.match(T.smartChipTip(chip(t, "intent")), /Source: the AI\.$/);
});

test("AI: needs you only for the customer's latest message; tracking hold is not a send problem", () => {
  const t = { id: "k4", status: "pending_human_review", lastInboundAt: NOW - 1 * H, awaitingReplySince: NOW - 1 * H,
    lastAutoDecision: "human_review_vetoed", lastAutoProcessedInboundAt: NOW - 1 * H };
  assert.match(chip(t, "needs_you").why, /a safety rule stopped it/);
  assert.equal(chip({ ...t, lastAutoProcessedInboundAt: NOW - 3 * H }, "needs_you"), null);
  const held = { ...t, lastAutoDecision: "human_review_after_tracking_unready", lastAutoDecisionAt: NOW - 50 * MIN };
  assert.equal(T.hasSendProblem(held), false);
  assert.equal(chip(held, "send_problem"), null);
  assert.match(chip(held, "needs_you").why, /tracking is not ready yet/);
  const failed = { ...held, lastAutoDecision: "human_review_after_enqueue_failure" };
  assert.equal(T.hasSendProblem(failed), true);
  assert.match(chip(failed, "send_problem").why, /could not be queued/);
});

test("AI working labels: writing, quiet period countdown, sending", () => {
  const t = { id: "k5", status: "pending_human_review", lastInboundAt: NOW - 5 * MIN, awaitingReplySince: NOW - 5 * MIN };
  assert.equal(chip({ ...t, lastAutoDecision: "in_progress" }, "ai_working").label, "AI replying");
  assert.equal(chip({ ...t, lastAutoDecision: "deferred_quiet_period", autoPipelineDeferUntilMs: NOW + 3.5 * MIN }, "ai_working").label, "AI replying · 4m");
  assert.equal(chip({ ...t, lastAutoDecision: "deferred_quiet_period", autoPipelineDeferUntilMs: NOW - MIN }, "ai_working"), null);
  assert.equal(chip({ ...t, status: "queued_for_auto_send" }, "ai_working").label, "Sending…");
  // A draft waiting for review is not shown while the AI is still working.
  const drafting = { ...t, lastAutoDecision: "in_progress", aiDraftStatus: "ready", lastAutoProcessedInboundAt: NOW - 5 * MIN };
  assert.equal(chip(drafting, "draft_ready"), null);
});

test("Not right hides a label until the customer writes again", () => {
  const t = { id: "k6", status: "pending_human_review", lastInboundAt: NOW - 1 * H, awaitingReplySince: NOW - 1 * H,
    refundFlaggedAt: NOW - 30 * MIN, refundFlaggedReason: 'inbound:"refund"',
    labelOverrides: { refund: { off: true, forInboundMs: NOW - 1 * H, by: "paul" } } };
  assert.equal(chip(t, "refund"), null);
  assert.equal(hasFilter(t, "refund"), false);
  // The customer writes again: the label may come back (dimmed, as it is about the earlier message).
  const later = { ...t, lastInboundAt: NOW - 10 * MIN };
  assert.equal(chip(later, "refund").stale, true);
  // Only "off: true" hides.
  assert.ok(chip({ ...t, labelOverrides: { refund: { off: false, forInboundMs: NOW } } }, "refund"));
  // Only labels marked fixable offer "Not right".
  const fixable = T.CHIP_DEFS.filter(d => d.fix).map(d => d.id);
  assert.deepEqual(plain(fixable), ["needs_you", "refund", "intent"]);
});

test("other labels: sales stage, sold, workshop, syncing, help, photo", () => {
  const t = { id: "k7", lastInboundAt: NOW - 1 * H, awaitingReplySince: NOW - 1 * H };
  assert.equal(chip({ ...t, status: "sales_pending_close_approval" }, "sales_stage").label, "Sales: ready to close");
  assert.equal(chip({ ...t, status: "sales_completed", salesCompletedAt: NOW - D }, "sales_stage"), null);
  assert.equal(chip({ ...t, orderLinkOpen: 2, orderLinkTitle: "Charm sizes" }, "workshop").label, "Workshop question");
  assert.equal(chip({ ...t, gmailReceivedAt: NOW - 30 * MIN }, "syncing").label, "Syncing from Etsy");
  assert.equal(chip({ ...t, gmailReceivedAt: NOW - 1 * H + 30e3 }, "syncing"), null);
  assert.equal(chip({ ...t, etsyHeadingBadge: "Help Request" }, "help").label, "Help request");
  assert.equal(chip({ ...t, riskFlags: ["image_attached"] }, "photo").label, "Photo");
});

// ── Filters and smart views ───────────────────────────────────────────
test("filters: Unread first, one per label kind, unique ids", () => {
  const defs = plain(T.smartFilterDefs());
  assert.deepEqual(defs[0], { id: "unread", label: "Unread" });
  const list = defs.map(d => d.id);
  assert.equal(new Set(list).size, list.length);
  for (const d of defs) assert.ok(d.label, "filter without a name: " + d.id);
  assert.ok(list.includes("intent:sales_lead") && list.includes("send_problem") && list.includes("refund"));
  const t = { id: "f1", unread: true, lastInboundAt: NOW - H };
  assert.equal(T.smartHasFilter(t, "unread", []), true);
  assert.equal(T.smartHasFilter({ ...t, unread: false }, "unread", []), false);
});

test("smart views cover Inbox and Waiting on customer only", () => {
  const lead = { id: "v1", status: "sales_quote", lastInboundAt: NOW - 1 * H, awaitingReplySince: NOW - 1 * H };
  assert.equal(inView(lead, "x_v_sales"), true);
  const intent = { id: "v2", lastInboundAt: NOW - 1 * H, intentClassification: "sales_lead", intentConfidence: 0.86,
    intentClassifiedAt: NOW - 50 * MIN };
  assert.equal(inView(intent, "x_v_sales"), true);
  const rushWaiting = { id: "v3", status: "production_rush", lastInboundAt: NOW - 2 * D, lastOutboundAt: NOW - 1 * D };
  assert.equal(folder(rushWaiting), "x_waiting");
  assert.equal(inView(rushWaiting, "x_v_rush"), true);
  const doneRefund = { id: "v4", status: "archived", lastInboundAt: NOW - 2 * D, refundFlaggedAt: NOW - 2 * D,
    refundFlaggedReason: 'inbound:"refund"' };
  assert.equal(folder(doneRefund), "x_done");
  assert.equal(inView(doneRefund, "x_v_refund"), false);
  const workshop = { id: "v5", lastInboundAt: NOW - 1 * H, orderLinkOpen: 1 };
  assert.equal(inView(workshop, "x_v_workshop"), true);
});

test("Waiting on customer ends after 30 days; Done is only for archived threads", () => {
  const t = { id: "w1", lastInboundAt: NOW - 40 * D, lastOutboundAt: NOW - 31 * D };
  assert.equal(folder(t), "x_all");
  assert.equal(folder({ ...t, lastOutboundAt: NOW - 29 * D }), "x_waiting");
  assert.equal(folder({ ...t, status: "archived", lastOutboundAt: NOW - 29 * D }), "x_done");
});

// ── Inbox layout choice ───────────────────────────────────────────────
test("layout: ?tabs override, owner setting, cached setting, default new inbox", () => {
  const block = html.slice(html.indexOf("    const TAB_LAYOUTS = "),
    html.indexOf("    let _tabLayout = resolveTabLayout();"));
  assert.ok(block.length > 100 && block.includes("function resolveTabLayout"));
  function run({ search = "", ls = {}, cfg = { _loaded: false } } = {}) {
    const store = new Map(Object.entries(ls));
    const ctx = vm.createContext({
      location: { search },
      URLSearchParams,
      localStorage: {
        getItem: k => (store.has(k) ? store.get(k) : null),
        setItem: (k, v) => store.set(k, String(v)),
        removeItem: k => store.delete(k)
      },
      _autoCfg: cfg
    });
    vm.runInContext(block + "\nglobalThis.__r = resolveTabLayout;", ctx);
    return { layout: ctx.__r(), store, ctx };
  }
  assert.equal(run().layout, "smart");
  assert.equal(run({ cfg: { _tabsKnown: true, inboxTabs: "legacy" } }).layout, "legacy");
  assert.equal(run({ cfg: { _tabsKnown: true, inboxTabs: null } }).layout, "smart");
  const r1 = run({ search: "?tabs=legacy", cfg: { _tabsKnown: true, inboxTabs: "smart" } });
  assert.equal(r1.layout, "legacy");
  assert.equal(r1.store.get("etsymail_tab_layout"), "legacy");
  const r2 = run({ search: "?tabs=default", ls: { etsymail_tab_layout: "smart" }, cfg: { _tabsKnown: true, inboxTabs: "legacy" } });
  assert.equal(r2.layout, "legacy");
  assert.equal(r2.store.has("etsymail_tab_layout"), false);
  assert.equal(run({ ls: { etsymail_tab_layout: "smart" }, cfg: { _tabsKnown: true, inboxTabs: "legacy" } }).layout, "smart");
  // First paint before the config loads: the cached owner setting.
  assert.equal(run({ ls: { etsymail_tab_layout_cfg: "legacy" } }).layout, "legacy");
  assert.equal(run({ ls: { etsymail_tab_layout_cfg: "legacy" }, cfg: { _tabsKnown: true, inboxTabs: "smart" } }).layout, "smart");
  // Unknown values are ignored.
  assert.equal(run({ search: "?tabs=fancy", ls: { etsymail_tab_layout: "weird" } }).layout, "smart");
});

// ── Snapshot: waiting state and previews (pure function) ──────────────
function stub(file, exportsObj) {
  const full = path.join(FN_DIR, file);
  const m = new Module(full, module);
  m.filename = full;
  m.loaded = true;
  m.exports = exportsObj;
  require.cache[full] = m;
}
const firestore = () => ({ collection: () => { throw new Error("no Firestore in tests"); } });
firestore.FieldValue = { serverTimestamp: () => ({}), delete: () => ({}), increment: n => n, arrayUnion: (...a) => a };
firestore.Timestamp = { fromMillis: ms => ({ toMillis: () => ms }) };
stub("firebaseAdmin.js", { firestore });
stub("_etsyMailAuth.js", { requireExtensionAuth: () => ({ ok: false }), CORS: {} });
const { computeAwaitingState } = require(path.join(FN_DIR, "etsyMailSnapshot.js"));
const fsTs = ms => ({ toMillis: () => ms });   // a Firestore Timestamp as read by firebase-admin
const calc = (o) => computeAwaitingState({ nowMs: NOW, ...o });

test("snapshot: first scrape of a new conversation sets the waiting time and previews", () => {
  const t1 = NOW - 2 * H, t2 = NOW - 1 * H;
  const r = calc({ prev: {}, newestInboundMs: t2, inboundTs: [t1, t2], newestInText: "  Hello\n  there  " });
  assert.equal(r.awaitingReplySinceMs, t1);
  assert.equal(r.lastInboundPreview, "Hello there");
  assert.equal(r.clearAwaiting, undefined);
  assert.equal(calc({ prev: {}, newestInboundMs: t2, inboundTs: [t2], newestInText: "x".repeat(400) }).lastInboundPreview.length, 160);
});

test("snapshot: incremental scrapes keep the first unanswered message", () => {
  const t0 = NOW - 5 * H, t1 = NOW - 3 * H, t2 = NOW - 2 * H, t3 = NOW - 10 * MIN;
  // Already waiting since t1: a newer customer message changes nothing.
  const prev = { lastOutboundAt: fsTs(t0), lastInboundAt: fsTs(t2), awaitingReplySince: fsTs(t1) };
  const r = calc({ prev, newestInboundMs: t3, inboundTs: [t3], newestInText: "Any news?" });
  assert.equal(r.awaitingReplySinceMs, undefined);
  assert.equal(r.clearAwaiting, undefined);
  assert.equal(r.lastInboundPreview, "Any news?");
  // A document written before the field existed: the stored latest
  // customer message is unanswered too, even when the scrape only has the newest.
  const old = { lastOutboundAt: fsTs(t0), lastInboundAt: fsTs(t2) };
  assert.equal(calc({ prev: old, newestInboundMs: t3, inboundTs: [t3] }).awaitingReplySinceMs, t2);
  // The scrape holds only our older messages: nothing to change.
  assert.deepEqual(plain(calc({ prev, newestOutboundMs: t0 - H })), {});
});

test("snapshot: a reply typed on Etsy clears the waiting time", () => {
  const t1 = NOW - 3 * H, t4 = NOW - 1 * H;
  const prev = { lastInboundAt: fsTs(t1), awaitingReplySince: fsTs(t1) };
  const r = calc({ prev, newestOutboundMs: t4, newestOutText: "Thanks, on its way!" });
  assert.equal(r.clearAwaiting, true);
  assert.equal(r.awaitingReplySinceMs, undefined);
  assert.equal(r.lastOutboundPreview, "Thanks, on its way!");
  // Our reply and a newer customer message in the same scrape: waiting again, since the new message.
  const t5 = NOW - 30 * MIN;
  const r2 = calc({ prev, newestInboundMs: t5, newestOutboundMs: t4, inboundTs: [t1, t5] });
  assert.equal(r2.awaitingReplySinceMs, t5);
  // A reply sent from the inbox (server time, allowed 2 minutes of clock skew).
  const r3 = calc({ prev: { ...prev, lastOperatorReplyAt: fsTs(t1 + 10 * MIN) }, newestInboundMs: t1, inboundTs: [t1] });
  assert.equal(r3.clearAwaiting, true);
  const r4 = calc({ prev: { ...prev, lastOperatorReplyAt: fsTs(t1 + 1 * MIN) }, newestInboundMs: t1, inboundTs: [t1] });
  assert.equal(r4.clearAwaiting, undefined);
});

test("snapshot: back-filled conversations older than 30 days are left alone", () => {
  const old = NOW - 40 * D;
  assert.equal(calc({ prev: {}, newestInboundMs: old, inboundTs: [old - D, old] }).awaitingReplySinceMs, undefined);
  // An old unanswered conversation where the customer writes again today is waiting.
  const prev = { lastInboundAt: fsTs(old), lastOutboundAt: fsTs(old - D) };
  const r = calc({ prev, newestInboundMs: NOW - H, inboundTs: [NOW - H] });
  assert.equal(r.awaitingReplySinceMs, old);
});

test("snapshot: archived threads wait again only for a message not seen before", () => {
  const t1 = NOW - 3 * D, t3 = NOW - 20 * MIN;
  const prev = { status: "archived", lastInboundAt: fsTs(t1), lastOutboundAt: fsTs(t1 - D) };
  assert.equal(calc({ prev, newestInboundMs: t3, inboundTs: [t1, t3] }).awaitingReplySinceMs, t3);
  assert.equal(calc({ prev, newestInboundMs: t1, inboundTs: [t1] }).awaitingReplySinceMs, undefined);
});

test("snapshot: equal times count as waiting; bad input never throws", () => {
  const t = NOW - H;
  assert.equal(calc({ prev: {}, newestInboundMs: t, newestOutboundMs: t, inboundTs: [t] }).awaitingReplySinceMs, t);
  assert.deepEqual(plain(computeAwaitingState()), {});
  assert.deepEqual(plain(calc({ prev: null, inboundTs: null })), {});
  assert.equal(calc({ prev: { lastInboundAt: "junk" }, newestInboundMs: t, inboundTs: ["x", t] }).awaitingReplySinceMs, t);
});

// ── The page's inline scripts ─────────────────────────────────────────
test("every inline script of etsy-mail-1.html parses", () => {
  const re = /<script\b([^>]*)>([\s\S]*?)<\/script>/gi;
  let m, n = 0;
  while ((m = re.exec(html))) {
    if (/\bsrc\s*=/.test(m[1])) continue;
    const type = (/\btype\s*=\s*["']?([^"'\s>]+)/i.exec(m[1]) || [])[1] || "";
    if (type && !/javascript|module/i.test(type)) continue;
    n++;
    const line = html.slice(0, m.index).split("\n").length;
    assert.doesNotThrow(() => new vm.Script(m[2], { filename: "inline-script@" + line }), "script at line " + line);
  }
  assert.ok(n >= 1);
});

test("the new layout's names are declared once in the page", () => {
  const names = ["TAB_LAYOUTS", "resolveTabLayout", "isSmartLayout", "DAY", "SKEW_MS", "isAwaiting", "isNew",
    "isWaitingOnCustomer", "isDone", "isArchived", "aiState", "mainFolderOf", "CHIP_DEFS", "SMART_VIEWS", "SMART_FOLDERS", "smartS"];
  const fns = Array.from(new Set((html.match(/function (smart[A-Za-z0-9_]*)\s*\(/g) || []).map(s => s.slice(9).replace(/\s*\($/, ""))));
  assert.ok(fns.length > 30);
  for (const nm of names.concat(fns)) {
    const decl = new RegExp("(?:\\bfunction\\s+" + nm + "\\s*\\(|\\b(?:const|let|var)\\s+(?:[A-Za-z0-9_$]+\\s*=[^,;]*,\\s*)*" + nm + "\\b\\s*=)", "g");
    assert.equal((html.match(decl) || []).length, 1, "declarations of " + nm);
  }
});

console.log(passed + " inbox layout tests passed");
