/*  netlify/functions/_employeeIssues.js
 *  Issues attributed to one employee, success and failure rates, and contact (inbox) rates: the HR numbers of the Employee page
 *  (Paul, 5 Oct 2026: "number of issues attributed to that employee ... their contact success and failure rates").
 *  Helper of the `person` op (_employeeProfile.js calls it). Shapes and kinds: plans/employee-hr/api.md, section E10.
 *
 *      issues(ctx, { name, from, to, prev, limit, cursor, eventDays, crossCheck, crossOrders })   async, never throws
 *      -> { ok, name, mode, from, to, issues:{ total, own, system, order, byKind, byDay, items, next ... }, rates, contact,
 *           definitions, estimated, eventWindow, reads, notes, partial?, errors? }
 *
 *  AN ISSUE IS A SIGNAL, NOT A VERDICT. Every kind below is something the station pages LOGGED, and each carries a plain
 *  definition that says what it is not (a reprint can be a printer jam; a cancelled-order alert is the customer's doing; an error
 *  on screen is usually the system). Each says who it is about: `own` (something the person pressed), `order` (something about
 *  the order) or `system` (a failure shown to them). Nothing is guessed: a kind is detected by one written rule (the phrase the
 *  station page writes: _activityKinds.js) and what the data cannot show is listed in `definitions.notDetected`.
 *
 *  WHERE THE NUMBERS COME FROM (cost: one call stays cheap)
 *   - Efficiency_Daily rollups (the caller's loaded rows, else one range read): the plain counters (scans, completes, prints,
 *     rejects, errors, undos) exist for every day; the issue counters x_* and the day's marker `ixv` exist for days written after
 *     they shipped. A day without them is NOT counted as zero: its kinds show a dash (count null, or "counted on n of m days"),
 *     unless the day is inside the event window (the newest days), where its raw events are read and classified the same way.
 *   - Station_Activity events, only for the event window, one equality query per person-day (day == D, person == spelling; no
 *     composite index), through the caller's shared reader (ctx.prof.events) when it has one, else straight from ctx.db (kept 10
 *     minutes, today 1 minute). They give the ITEMS list (newest first, at most `limit` = 200, paged by `cursor`), the first-pass
 *     rate, the median first reply, and the days the counters lack.
 *   - For the person's newest `crossOrders` (30) finished orders, one small query per order (kept 10 minutes): "reopened later by
 *     someone else" and "came back to an earlier station". `crossCheck:false` skips it.
 *   - Nothing here calls Etsy, writes anything or reads the order timeline.
 *
 *  THE CONTEXT (the one place this file touches the caller; rewire in adapter()):
 *    ctx.db, ctx.prefix ("" | "Sandbox_"), ctx.now, ctx.today, ctx.aliases ({map, display}), ctx.life   employeeEfficiency.js's own context
 *    ctx.prof.raw.rollups, ctx.prof.events(from, to), ctx.prof.key, ctx.prof.eventDays                      _employeeProfile.js's prepared window
 *    ctx.mode "real"|"sandbox" (instead of prefix), ctx.nameKey(name), ctx.rollups, ctx.readRollups(from,to), ctx.readEvents({person,day,scope})
 *                                                                                          hooks for any other caller and the tests
 *  Names: only the person asked for is ever named; other people are "another person". No PIN, passcode or digits-only name
 *  is read, kept or returned (a digits-only name is refused; a 6-digit number inside a detail is already blanked). */
"use strict";
const { ACT, DAILY, nyDayHour } = require("./_stationActivity");
const K = require("./_activityKinds");

/* ── limits ── */
const LIM = { rollups: 6000, dayEvents: 1500, orderEvents: 500, items: 200, eventDays: 14, profEventDays: 31, maxEventDays: 31, contactDays: 31, crossOrders: 30, maxDays: 731, pool: 6, cameBackMin: 5 };
const TTL = { live: 5000, eventsToday: 60000, past: 600000, sandbox: 5000 };
const DAY_RE = /^\d{4}-\d{2}-\d{2}$/;

/* ── small helpers ── */
const num = v => { const x = Number(v); return Number.isFinite(x) ? x : 0; };
const ms = v => v == null ? 0 : typeof v.toMillis === "function" ? v.toMillis() : v instanceof Date ? v.getTime() : Number.isFinite(+v) ? +v : 0;
const r1 = x => Math.round(x * 10) / 10;
const pct = (n, d) => d > 0 ? r1(n / d * 100) : null;
const digits = (v, n = 30) => String(v == null ? "" : v).replace(/\D/g, "").slice(0, n);
/* A name never carries a PIN: four or more digits in a name are a login number that slipped in, so the digits are dropped ("Paul 482915" is "Paul"); the same rule as the console's. */
const noPin = s => (s.match(/\p{Nd}/gu) || []).length >= 4 ? s.replace(/\p{Nd}+/gu, " ").replace(/\s+/g, " ").trim() : s;
const cleanName = v => noPin(String(v == null ? "" : v).replace(/[\u0000-\u001f\u007f]/g, " ").replace(/\s+/g, " ").trim().slice(0, 200)).slice(0, 80);
const okName = n => !!n && /\p{L}/u.test(n);                       // a name with no letter is a PIN, never a person
/* the console's name folding (employeeEfficiency.js `fold`): one person however the login spelled the name. Used only when the
   caller gives no ctx.nameKey / ctx.prof.key. Keep identical to it. */
const fold = n => cleanName(n).normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLowerCase()
  .replace(/['\u2018\u2019`\u00b4]/g, "").replace(/[^\p{L}\p{N}]+/gu, " ").trim();
const scrub = v => { const s = String(v == null ? "" : v).replace(/[\u0000-\u001f\u007f]/g, " ").replace(/\s+/g, " ").trim().slice(0, 120); return /^\d+$/.test(s) ? "" : s.replace(/(?<!\d)\d{6}(?!\d)/g, "[#]"); };
const addDays = (day, n) => { const [y, m, d] = day.split("-").map(Number); return new Date(Date.UTC(y, m - 1, d + n)).toISOString().slice(0, 10); };
const validDay = s => typeof s === "string" && DAY_RE.test(s) && addDays(s, 0) === s;
const daysBetween = (a, z) => Math.round((Date.parse(z + "T00:00:00Z") - Date.parse(a + "T00:00:00Z")) / 86400000) + 1;
const maxDay = (a, b) => a > b ? a : b;
const median = list => { if (!list.length) return null; const s = list.slice().sort((x, y) => x - y), m = s.length >> 1; return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2; };
const { displayStation } = require("./_activityKinds");   // ONE Sorting station: the stored keys "sorter" and "qr" are shown as "sorting" (history keeps its keys)
const STATION_LABEL = { sorting: "Sorting", welding: "Welding", assembly: "Assembly", shipping: "Shipping", design: "Design", laser: "Laser", inbox: "Inbox" };
const stLabel = s => STATION_LABEL[s] || (s ? s.charAt(0).toUpperCase() + s.slice(1) : "a station");
const dur = t => { const m = Math.max(0, Math.round(t / 60000)); if (m < 60) return m + " min"; const h = Math.floor(m / 60); if (h < 48) return h + " h" + (m % 60 ? " " + (m % 60) + " min" : ""); return Math.round(h / 24) + " days"; };
/* the route of an order through the four core stations (the console's CORE list); a later scan at an EARLIER one is a "came back" */
const ROUTE = { sorting: 1, welding: 2, assembly: 3, shipping: 4 };
/* where the logged name is typed (not a PIN sign-in), and where a phone's scan is credited to the desktop's signed-in person */
const TYPED = new Set(["laser", "design", "sorting"]);
const PHONE = new Set(["sorting", "welding", "assembly", "shipping", "design"]);
const WHY_TYPED = "Some of it was done at the Sorting, Laser or Design stations, where the name is typed, not signed in with a PIN.";
const WHY_PHONE = "Phone scans are credited to the desktop's signed-in person.";
/* the sorter's Review card names, in plain words (charm-nest-bridge.js KIND_WORDS) */
const REVIEW_WORDS = { customOrder: "custom order", needsMaterial: "metal choice", needsMapping: "options", unmatchedSku: "unknown SKU", blockedSku: "blocked SKU", missingSize: "size", oversize: "too big", fontMissing: "font", engraveWords: "engraving words", notRepresentable: "characters", flipFailed: "flip", placement: "placement", orderChanged: "order changed", heldOrder: "held order" };

/* ── the kinds: label, who it is about, what it is and is NOT, how it is detected ── */
const KINDS = [
  { key: "undone", label: "Completions taken back", attr: "own", cov: "range",
    def: "The person pressed Undo on something finished: an order, a sheet or a sticker. It is not proof of a mistake: people undo a wrong click or a customer change.",
    how: "Counted from the Undo presses the stations record. The Inbox is left out (its Reopen is shown under contact)." },
  { key: "reprint", label: "Labels printed again", attr: "own", cov: "range",
    def: "A label, sticker or seal was printed again for an order that computer had already printed. It is not proof of a mistake: a printer jam, an empty roll or a smudged label does the same.",
    how: "The station page marks a print 'again' or 'reprint' when its computer printed that order before. A reprint from another computer, or after the page forgot, is not marked." },
  { key: "rescan", label: "Same order again", attr: "own", cov: "range", scan: true,
    def: "A scan or a Team stamp that the station page itself marked 'again' (Welding scans an order again, Assembly stamps it again). It is not proof of carelessness: a phone camera can read a code twice, and people re-check an order. A plain second scan that the page did not mark is not counted.",
    how: "A scan or stamp whose logged reason says 'again'. Only Welding and Assembly write it today; other stations show zero here." },
  { key: "heldOrSkipped", label: "Held, skipped or sent back", attr: "own", cov: "range",
    def: "The person held or skipped a Review card in the sorter, or sent engraving back to the words. It is a decision, not a fault: holding is often the right call.",
    how: "The sorter's Review answers 'held' and 'skipped' (except Unknown SKU), and 'engraving sent back to the words'." },
  { key: "reopenedLater", label: "Reopened later by someone else", attr: "order", cov: "window", cross: true,
    def: "After the person finished an order at a station, a different person undid or reopened it at that station. It is not proof of a mistake: a reopen can follow a customer change or a lead's check.",
    how: "For the person's newest finished orders, the order's own events are read. A later Undo, or 'completed order reopened', at the same station by another person is counted. An order the person undid themselves is counted once, under 'Completions taken back'." },
  { key: "cameBack", label: "Order came back to an earlier station", attr: "order", cov: "window", cross: true,
    def: "After the person finished an order at Welding, Assembly or Shipping, the same order was scanned, printed or finished again at an earlier station (Sorting, Welding or Assembly), at least 5 minutes later. It does not say who caused it: orders are also looked up again for a customer question.",
    how: "For the person's newest finished orders, the order's own events are read. The route is taken as Sorting, Welding, Assembly, Shipping." },
  { key: "unknownSku", label: "Unknown SKU held or skipped", attr: "order", cov: "range",
    def: "A Review card for a SKU that is in no master file was held or skipped. The SKU is missing from the files, not from the person.",
    how: "The sorter's Review answers 'held' and 'skipped' on an Unknown SKU card." },
  { key: "qaFlag", label: "Problem flagged to the Team", attr: "order", cov: "range",
    def: "At Assembly, a message to the Team that says rework, redo, remake, reject, defect, broken, damaged, wrong, flag or hold. It shows the person spoke up; the problem may come from an earlier step.",
    how: "Messages the assembly page records as 'flag to Team'. The words of the message are never kept." },
  { key: "cancelAlert", label: "Cancelled order met", attr: "order", cov: "range", scan: true,
    def: "The station showed the red cancelled-order alert, or blocked a sticker or print for a cancelled order. The customer cancelled: it is not the person's doing.",
    how: "Refusals the pages record as 'cancelled order'." },
  { key: "refused", label: "Order refused at the station", attr: "order", cov: "range", scan: true,
    def: "Any other refusal a page recorded: order not found, no stud earrings on the order, a label left off because of the metal. Mostly about the order, not the person.",
    how: "Every other refusal a page records." },
  { key: "lookupFailed", label: "Order lookup failed", attr: "system", cov: "range", scan: true,
    def: "The page could not read the order from Etsy (not found, or Etsy did not answer). Usually Etsy or the network, not the person.",
    how: "Errors that say the order lookup failed, or the order was not found or not loaded." },
  { key: "failed", label: "Something failed on screen", attr: "system", cov: "range",
    def: "Any other error the page showed: a print that did not open, a send that failed, an Undo that was not completed, or text typed that is not an order number. Usually the system, not the person.",
    how: "Every other error event." },
  { key: "replyFailed", label: "Reply did not go out", attr: "system", cov: "range",
    def: "An inbox reply failed at Etsy, or the server refused it before it reached Etsy. The reply may have been sent again afterwards; this says it failed once.",
    how: "The inbox events 'reply failed' and 'reply not sent'." }
];
const KIND = Object.fromEntries(KINDS.map(k => [k.key, k]));

const RATES = {
  firstPass: { label: "First-pass rate", better: "up", cov: "window",
    def: "Of the orders the person finished at a station, the share with no Undo, Reopen or reprint by them afterwards. Only their own presses are seen: a reopen by somebody else is shown as 'Reopened later by someone else'." },
  reworkRate: { label: "Rework rate", better: "down", cov: "range",
    def: "Undo presses divided by Complete presses at the production stations (the Inbox is left out). Lower means fewer finished things were taken back." },
  successRate: { label: "Error-free actions", better: "up", cov: "range",
    def: "The share of logged actions (scans, completes, prints, refusals, undos, notes) that did not end in an error on screen." },
  failureRate: { label: "Error rate", better: "down", cov: "range",
    def: "The share of logged actions that ended in an error on screen: a print that did not open, an Etsy lookup that failed. Most errors come from the system, not the person." },
  holdRate: { label: "Hold and cancel rate", better: null, cov: "range",
    def: "Refusals, holds, skips, flags and cancelled-order alerts, per 100 orders the person handled. It includes cancelled orders, which are not the person's doing: open the list to see each reason." },
  reprintRate: { label: "Reprint rate", better: "down", cov: "range",
    def: "Labels printed again divided by all label prints. A jammed printer raises it." },
  rescanRate: { label: "Repeat scan rate", better: "down", cov: "range",
    def: "Scans and stamps the page marked 'again', divided by all scans. Only Welding and Assembly mark them. A phone that reads a code twice raises it." }
};
const RATE_ORDER = ["firstPass", "reworkRate", "successRate", "failureRate", "holdRate", "reprintRate", "rescanRate"];

const CONTACT = {
  drafted: ["Replies drafted", "count", "Times the person asked the AI to draft, follow up or revise a reply. Their own typing is not counted."],
  sent: ["Replies sent", "count", "Replies Etsy's send queue accepted from the person."],
  delivered: ["Replies delivered", "count", "Replies Etsy's helper confirmed as sent."],
  unconfirmed: ["Replies unconfirmed", "count", "Probably sent, but Etsy did not confirm. Neither delivered nor failed: shown apart."],
  failed: ["Replies failed", "count", "Replies Etsy's helper could not send."],
  refused: ["Replies refused", "count", "Replies the server turned away before they reached Etsy's helper."],
  edited: ["AI drafts edited", "count", "AI drafts the person changed before sending. The log says a draft was edited, not by how much."],
  aiSentUnchanged: ["AI drafts sent unchanged", "count", "AI drafts the person sent as they were."],
  deliveryRate: ["Delivery rate", "percent", "Delivered divided by replies with a known result (delivered, failed or refused). Replies still waiting, or unconfirmed, are left out."],
  failureRate: ["Failure rate", "percent", "Failed or refused divided by replies with a known result (delivered, failed or refused)."],
  editedShare: ["Drafts edited", "percent", "AI drafts edited divided by all AI drafts sent (edited and unchanged)."],
  medianFirstReplyMs: ["Time to first reply (middle)", "ms", "The middle time from the customer's first message to the person's first reply, in whole minutes, over the days whose events were read."],
  meanFirstReplyMs: ["Time to first reply (average)", "ms", "The average time from the customer's first message to the person's first reply, over the days counted."],
  conversationsDone: ["Conversations done", "count", "Conversations the person archived as done."],
  reopened: ["Conversations reopened", "count", "Conversations the person reopened after they were done."],
  reopenRate: ["Reopen rate", "percent", "Conversations reopened divided by conversations done."]
};

const CONTACT_BETTER = { deliveryRate: "up", failureRate: "down", failed: "down", refused: "down", reopenRate: "down", reopened: "down" };
const NOT_DETECTED = [
  { topic: "Cancels the person pressed", text: "The order timeline records who cancelled an order, but it is not read here: it would mean reading every order's history of a day. Cancelled orders the stations met are counted as 'Cancelled order met'." },
  { topic: "Abandoned work", text: "An order left in progress that never finished needs the live station board's data, which is not read here. Scans with no finish are not counted either: the stations do not always record a finish." },
  { topic: "How much a reply was edited", text: "The inbox log says an AI draft was edited, not by how much, so 'edited a lot' cannot be told." },
  { topic: "Whether a customer was satisfied", text: "A delivered reply says Etsy accepted it, not that the customer was happy with it." },
  { topic: "Work with nobody signed in", text: "A station records nothing when nobody is signed in, so that work is in nobody's numbers." },
  { topic: "Who did the work at a phone", text: "A phone scan is credited to the signed-in person of the desktop it relays to. The Sorter, Laser and Design names are typed." },
  { topic: "Inbox names", text: "Inbox numbers belong to the name the inbox account shows. If the same person is 'Ana' in the inbox and 'Ana M.' at a station, add the alias to config/employeeAliases so the two join." }
];
const HYGIENE = "An issue is a signal, not a verdict. The data shows what was logged at a station, not how hard someone worked or why something happened. Read each number with its definition.";

/* ── the adapter: the only place that touches the caller's context ── */
const caches = new WeakMap();
function adapter(ctx) {
  const prof = ctx.prof && typeof ctx.prof === "object" ? ctx.prof : null;
  const sandbox = ctx.mode ? ctx.mode === "sandbox" : (prof && prof.mode ? prof.mode === "sandbox" : (!!ctx.prefix || ctx.sandbox === true));
  const now = Number.isFinite(+ctx.now) && +ctx.now > 0 ? +ctx.now : Date.now();
  const db = ctx.db || null;
  const aliasMap = ctx.aliases && ctx.aliases.map instanceof Map ? ctx.aliases.map : null;
  let cache; if (db && typeof db === "object") { cache = caches.get(db); if (!cache) caches.set(db, cache = new Map()); } else cache = new Map();
  const A = {
    ctx, prof, db, sandbox, prefix: sandbox ? "Sandbox_" : "", now, today: validDay(ctx.today) ? ctx.today : nyDayHour(now).day, cache,
    life: sandbox ? TTL.sandbox : (Number.isFinite(+ctx.life) ? +ctx.life : Infinity),
    reads: { rollups: { queries: 0, docs: 0, cached: 0 }, events: { queries: 0, docs: 0, cached: 0 }, orders: { queries: 0, docs: 0, cached: 0 } },
    keyOf(name) {
      if (typeof ctx.nameKey === "function") { try { const k = ctx.nameKey(name); if (k) return String(k); } catch (_) {} }
      if (prof && prof.key && cleanName(name) === cleanName(prof.name)) return String(prof.key);
      let k = fold(name);
      if (aliasMap) for (let i = 0; i < 3 && aliasMap.has(k) && aliasMap.get(k) !== k; i++) k = aliasMap.get(k);
      return k;
    },
    display(name) {
      if (prof && prof.name && cleanName(name) === cleanName(prof.name)) return String(prof.name);
      const d = ctx.aliases && ctx.aliases.display && typeof ctx.aliases.display.get === "function" ? ctx.aliases.display.get(A.keyOf(name)) : "";
      return d || cleanName(name);
    },
    col(name) { return db.collection(A.prefix + name); }
  };
  return A;
}
/** A shared, time-limited read; a failed read is forgotten at once. `bucket` names the reads counter. */
function cachedRead(A, bucket, key, ttl, fn) {
  ttl = Math.min(ttl, A.life);
  const hit = A.cache.get(key);
  if (hit && A.now - hit.at < Math.min(ttl, hit.ttl)) { A.reads[bucket].cached++; return hit.p; }
  const entry = { at: A.now, ttl, p: null };
  entry.p = Promise.resolve().then(fn);
  A.cache.set(key, entry);
  entry.p.catch(() => { if (A.cache.get(key) === entry) A.cache.delete(key); });
  if (A.cache.size > 800) for (const [k, e] of A.cache) if (A.now - e.at > TTL.past) A.cache.delete(k);
  return entry.p;
}
async function pool(list, n, fn) {
  const out = new Array(list.length); let i = 0;
  await Promise.all(Array.from({ length: Math.min(n, list.length) }, async () => { while (i < list.length) { const j = i++; out[j] = await fn(list[j], j); } }));
  return out;
}
const isIndexError = e => !!e && (e.code === 9 || /FAILED_PRECONDITION|index/i.test(String(e.message || "")));
const errText = e => String((e && (e.message || e.code)) || e || "failed").slice(0, 160);

/** The rollups of from..to: the caller's loaded rows, a hook, or one range read (kept 5 s for today, 10 minutes for the past). */
async function loadRollups(A, from, to) {
  const c = A.ctx;
  if (Array.isArray(c.rollups)) return { docs: c.rollups, capped: !!c.rollupsCapped, preloaded: true };
  if (A.prof && A.prof.raw && Array.isArray(A.prof.raw.rollups)) return { docs: A.prof.raw.rollups, capped: !!(A.prof.capped && A.prof.capped.includes("rollups")), preloaded: true };
  if (typeof c.readRollups === "function") { const r = await c.readRollups(from, to); return Array.isArray(r) ? { docs: r, capped: false, preloaded: true } : { docs: (r && r.docs) || [], capped: !!(r && r.capped), preloaded: true }; }
  if (!A.db) throw new Error("no database handle");
  return cachedRead(A, "rollups", `iss|roll|${A.prefix}|${from}|${to}`, to >= A.today ? TTL.live : TTL.past, async () => {
    const snap = await A.col(DAILY).where("day", ">=", from).where("day", "<=", to).limit(LIM.rollups + 1).get();
    A.reads.rollups.queries++; A.reads.rollups.docs += snap.docs.length;
    return { docs: snap.docs.slice(0, LIM.rollups).map(d => d.data() || {}), capped: snap.docs.length > LIM.rollups };
  });
}

/** An event as this file uses it, or null (not this store's, no person). */
function eventRow(id, d, A) {
  d = d || {};
  const person = cleanName(d.person), action = String(d.action || "");
  if (!okName(person) || !action || !!d.sandbox !== A.sandbox) return null;
  const at = ms(d.at), tsMs = ms(d.ts), serverAt = ms(d.serverAt) || tsMs || at;
  return { id: String(d.id || id || "").slice(0, 100), at, k: tsMs || serverAt, person, station: displayStation(String(d.station || "")), action, orderId: digits(d.orderId),
    orders: num(d.orders) >= 1 ? 1 : 0, parts: Math.max(0, Math.floor(num(d.parts))), detail: scrub(d.detail), seq: num(d.seq), day: validDay(d.day) ? d.day : nyDayHour(at || A.now).day };
}
const byCommit = (a, b) => a.k - b.k || a.seq - b.seq || (a.id < b.id ? -1 : a.id > b.id ? 1 : 0);

/** One person-day's events under one spelling (scope "all", or "inbox" only), kept 10 minutes (today 1 minute). */
function readPersonDay(A, spelling, day, scope) {
  return cachedRead(A, "events", `iss|ev|${A.prefix}|${spelling}|${day}|${scope}`, day === A.today ? TTL.eventsToday : TTL.past, async () => {
    if (typeof A.ctx.readEvents === "function") {
      const r = await A.ctx.readEvents({ person: spelling, day, scope });
      const docs = Array.isArray(r) ? r : (r && (r.rows || r.docs)) || [];
      return { rows: docs.map(d => eventRow(d.id, d, A)).filter(e => e && (scope === "all" || e.station === "inbox")), capped: !!(r && r.capped) };
    }
    if (!A.db) throw new Error("no database handle");
    let q = A.col(ACT).where("day", "==", day).where("person", "==", spelling);
    if (scope === "inbox") q = q.where("station", "==", "inbox");
    let snap, filtered = false;
    try { snap = await q.limit(LIM.dayEvents + 1).get(); }
    catch (e) {                                              // no index for the combination: read the day and keep the person in memory
      if (!isIndexError(e)) throw e;
      snap = await A.col(ACT).where("day", "==", day).limit(LIM.dayEvents * 2 + 1).get(); filtered = true;
    }
    A.reads.events.queries++; A.reads.events.docs += snap.docs.length;
    const rows = [];
    for (const d of snap.docs) {
      const x = d.data() || {};
      if (filtered && (x.person !== spelling || (scope === "inbox" && x.station !== "inbox"))) continue;
      const e = eventRow(d.id, x, A); if (e) rows.push(e);
    }
    return { rows: rows.slice(0, LIM.dayEvents), capped: rows.length > LIM.dayEvents };
  });
}
/** One order's events, every person (for the cross-check), kept 10 minutes. */
function readOrder(A, rid) {
  return cachedRead(A, "orders", `iss|ord|${A.prefix}|${rid}`, TTL.past, async () => {
    if (!A.db) throw new Error("no database handle");
    const snap = await A.col(ACT).where("orderId", "==", rid).limit(LIM.orderEvents + 1).get();
    A.reads.orders.queries++; A.reads.orders.docs += snap.docs.length;
    return snap.docs.slice(0, LIM.orderEvents).map(d => eventRow(d.id, d.data() || {}, A)).filter(e => e && e.orderId === rid);
  });
}

/** The events of the window, grouped for the classifier: Map day -> { scope, bySpelling: Map(spelling -> rows), capped }.
    Through the caller's shared reader (ctx.prof.events) when it has one (its cache is shared with the profile's own reads), else straight from ctx.db. */
async function readWindow(A, key, agg, activeDays, winFrom, to, conFrom, errors) {
  const out = new Map(); let capped = false;
  const group = (rows, scope) => {
    for (const e of rows) {
      const d = e.day; if (!agg.has(d)) continue;
      const m = out.get(d) || out.set(d, { scope, bySpelling: new Map(), capped: false }).get(d);
      (m.bySpelling.get(e.person) || m.bySpelling.set(e.person, []).get(e.person)).push(e);
    }
  };
  if (A.prof && typeof A.prof.events === "function" && !A.ctx.readEvents) {
    try {
      const rows = (await A.prof.events(winFrom, to)).filter(Boolean).map(e => Object.assign({ orders: 0, seq: 0 }, e));   // (rows of an older reader carry no orders / seq)
      group(rows, "all");
      capped = !!(A.prof.capped && A.prof.capped.includes("events"));
      for (const m of out.values()) m.capped = capped;
    } catch (e) { errors.push("events: " + errText(e)); }
    return { days: out, capped };
  }
  const jobs = [];
  for (const d of activeDays) {
    const a = agg.get(d);
    const scope = d >= winFrom ? "all" : (d >= conFrom && a.st.inbox && !a.ixv ? "inbox" : "");
    if (scope) for (const sp of a.spell) jobs.push({ day: d, spelling: sp, scope });
  }
  const res = await pool(jobs, LIM.pool, async j => { try { return { j, r: await readPersonDay(A, j.spelling, j.day, j.scope) }; } catch (e) { return { j, err: errText(e) }; } });
  const bad = new Set();
  for (const x of res) if (x.err) { bad.add(x.j.day); errors.push(`events ${x.j.day}: ${x.err}`); }
  for (const x of res) {
    if (x.err || bad.has(x.j.day)) continue;                            // part of that day failed: it is not counted from events
    if (x.r.capped) capped = true;
    const m = out.get(x.j.day) || out.set(x.j.day, { scope: x.j.scope, bySpelling: new Map(), capped: false }).get(x.j.day);
    m.bySpelling.set(x.j.spelling, x.r.rows); m.capped = m.capped || !!x.r.capped;
  }
  return { days: out, capped };
}

/* ── rollups -> per-day numbers of one person ── */
const BASE = ["scans", "scanParts", "completes", "parts", "orders", "prints", "rejects", "errors", "notes", "undos", "undoParts", "undoOrders"];
const zero = () => { const o = {}; for (const k of BASE) o[k] = 0; for (const k of K.X_KEYS) o[k] = 0; return o; };
function aggregate(A, docs, key, from, to) {
  const days = new Map();
  for (const d of docs) {
    if (!d || !validDay(d.day) || d.day < from || d.day > to || !!d.sandbox !== A.sandbox) continue;
    const person = cleanName(d.person); if (!okName(person) || A.keyOf(person) !== key) continue;
    let a = days.get(d.day); if (!a) days.set(d.day, a = { day: d.day, spell: new Set(), st: {}, ids: new Set(), ixv: true, events: 0 });
    a.spell.add(String(d.person)); a.events += num(d.events);
    if (d.ixv !== 1) a.ixv = false;                                  // one spelling without the marker makes the whole day "not counted"
    for (const [s, v] of Object.entries(d.stations && typeof d.stations === "object" ? d.stations : {})) {
      if (!/^[a-z][\w-]{0,19}$/.test(s) || s in Object.prototype || !v || typeof v !== "object") continue;
      const ds = displayStation(s), t = a.st[ds] || (a.st[ds] = zero());        // (counters stored under "sorter" or "qr" add to Sorting)
      for (const k of BASE) t[k] += Math.max(0, num(v[k]));
      for (const k of K.X_KEYS) t[k] += Math.max(0, num(v[k]));
    }
    for (const [id, m] of Object.entries(d.touched && typeof d.touched === "object" ? d.touched : {})) {
      const oid = digits(id); if (!oid || !m || typeof m !== "object") continue;
      if (Object.keys(m).some(s => m[s] && s !== "inbox")) a.ids.add(oid);
    }
  }
  return days;
}
/** Orders handled that day at the production stations: the distinct orders in `touched`, never fewer than the orders finished. */
function handledOf(a) {
  let fin = 0; for (const [s, t] of Object.entries(a.st)) if (s !== "inbox") fin += Math.max(0, t.orders - t.undoOrders);
  return Math.max(a.ids.size, fin);
}

/* ── a day's kinds, from counters or from events ── */
const put = (k, kind, station, n) => { if (!(n > 0)) return; const c = k[kind] || (k[kind] = { n: 0, st: {} }); c.n += n; c.st[station] = (c.st[station] || 0) + n; };
function undoneOf(a) { const k = {}; for (const [s, t] of Object.entries(a.st)) if (s !== "inbox") put(k, "undone", s, t.undos); return k.undone || null; }
function kindsFromCounters(a) {
  const k = {};
  for (const [s, t] of Object.entries(a.st)) {
    if (s === "inbox") { const rf = t.x_fail + t.x_refuse; put(k, "replyFailed", s, rf); put(k, "failed", s, Math.max(0, t.errors - rf)); continue; }
    put(k, "cancelAlert", s, t.x_cancel); put(k, "heldOrSkipped", s, t.x_held); put(k, "unknownSku", s, t.x_sku); put(k, "qaFlag", s, t.x_flag);
    put(k, "refused", s, Math.max(0, t.rejects - t.x_cancel - t.x_held - t.x_sku - t.x_flag));
    put(k, "lookupFailed", s, t.x_lookup); put(k, "failed", s, Math.max(0, t.errors - t.x_lookup));
    put(k, "reprint", s, t.x_reprint); put(k, "rescan", s, t.x_rescan);
  }
  return k;
}
function kindsFromEvents(cls) { const k = {}; for (const c of cls) if (c.kind && c.kind !== "undone") put(k, c.kind, c.e.station, 1); return k; }
/** The events of ONE spelling and day, in the order the writer saw them, each with its kind (the same classifier the writer uses). */
function classifyRows(rows) {
  const out = [];
  for (const e of rows.slice().sort(byCommit)) { const c = K.classify(e); out.push({ e, kind: c.kind, x: c.x }); }
  return out;
}

/* ── the numbers of one window, from rollup counters and (for the newest days) classified events ── */
function analyse(agg, evDays) {
  const activeDays = [...agg.keys()].sort();
  const perDay = new Map(), withSrc = [];
  for (const d of activeDays) {
    const a = agg.get(d), ev = evDays.get(d);
    const src = a.ixv ? "counters" : ev && ev.scope === "all" ? "events" : "";
    const k = src === "counters" ? kindsFromCounters(a) : src === "events" ? kindsFromEvents(ev.cls) : {};
    const u = undoneOf(a); if (u) k.undone = u;
    perDay.set(d, { day: d, src, k, handled: handledOf(a) });
    if (src) withSrc.push(d);
  }
  const kinds = {};
  for (const kd of KINDS) {
    if (kd.cross) continue;
    let n = 0, dc = 0, handled = 0; const st = {};
    for (const d of activeDays) {
      const pd = perDay.get(d); if (kd.key !== "undone" && !pd.src) continue;
      dc++; handled += pd.handled; const c = pd.k[kd.key];
      if (c) { n += c.n; for (const [s, v] of Object.entries(c.st)) st[s] = (st[s] || 0) + v; }
    }
    kinds[kd.key] = { count: dc ? n : null, daysCounted: dc, handled, stations: st, complete: activeDays.length > 0 && dc === activeDays.length };
  }
  let handledCounted = 0, totalCounted = 0;
  for (const d of withSrc) { const pd = perDay.get(d); handledCounted += pd.handled; for (const c of Object.values(pd.k)) totalCounted += c.n; }
  const base = (fields, days) => {
    const total = {}, sts = new Set(); for (const f of fields) total[f] = 0;
    for (const d of days) for (const [s, t] of Object.entries(agg.get(d).st)) { if (s === "inbox") continue; let any = false; for (const f of fields) { total[f] += t[f]; if (t[f]) any = true; } if (any) sts.add(s); }
    return { total, sts };
  };
  const rates = {};
  { const b = base(["undos", "completes"], activeDays); rates.reworkRate = { n: b.total.undos, d: b.total.completes, sts: b.sts, days: activeDays.length }; }
  { const b = base(["scans", "completes", "prints", "rejects", "errors", "undos", "notes"], activeDays), t = b.total;
    const acts = t.scans + t.completes + t.prints + t.rejects + t.errors + t.undos + t.notes;
    rates.successRate = { n: acts - t.errors, d: acts, sts: b.sts, days: activeDays.length }; rates.failureRate = { n: t.errors, d: acts, sts: b.sts, days: activeDays.length }; }
  { const b = base(["rejects"], activeDays); let h = 0; for (const d of activeDays) h += perDay.get(d).handled; rates.holdRate = { n: b.total.rejects, d: h, sts: b.sts, days: activeDays.length }; }
  for (const [rk, kind, field] of [["reprintRate", "reprint", "prints"], ["rescanRate", "rescan", "scans"]]) {
    const b = base([field], withSrc); let n = 0; const sts = new Set(b.sts);
    for (const d of withSrc) { const c = perDay.get(d).k[kind]; if (c) { n += c.n; for (const s of Object.keys(c.st)) sts.add(s); } }
    rates[rk] = { n: withSrc.length ? n : null, d: withSrc.length ? b.total[field] : null, sts, days: withSrc.length };
  }
  return { activeDays, perDay, withSrc, kinds, handledCounted, totalCounted, rates };
}

/* ── the contact block (the inbox): counts from the counters, or from the classified events of days that have none ── */
function contactCounts(agg, evDays) {
  const days = [...agg.keys()].sort().filter(d => agg.get(d).st.inbox);
  const X = {}; for (const k of K.INBOX_X) X[k] = 0;
  let counted = 0, done = 0, reopened = 0; const source = new Set();
  for (const d of days) {
    const a = agg.get(d), ib = a.st.inbox, ev = evDays.get(d);
    done += ib.completes; reopened += ib.undos;                                    // exact on every day: the old counters
    if (a.ixv) { counted++; source.add("counters"); for (const k of K.INBOX_X) X[k] += ib[k]; }
    else if (ev) { counted++; source.add("events"); for (const c of ev.cls) if (c.e.station === "inbox") for (const [k, v] of Object.entries(c.x)) if (k in X) X[k] += v; }
  }
  return { days, counted, X, done, reopened, source };
}
function contactValues(cc, firstMins) {
  const { X, days, counted, done, reopened } = cc;
  const known = X.x_deliv + X.x_fail + X.x_refuse, aiSent = X.x_edit + X.x_aiok;
  const available = days.length > 0, none = !available || counted === 0;           // no inbox days, or none counted yet: dashes
  const v = {
    drafted: none ? null : X.x_draft, sent: none ? null : X.x_sent, delivered: none ? null : X.x_deliv, unconfirmed: none ? null : X.x_unconf,
    failed: none ? null : X.x_fail, refused: none ? null : X.x_refuse, edited: none ? null : X.x_edit, aiSentUnchanged: none ? null : X.x_aiok,
    deliveryRate: none ? null : pct(X.x_deliv, known), failureRate: none ? null : pct(X.x_fail + X.x_refuse, known), editedShare: none ? null : pct(X.x_edit, aiSent),
    medianFirstReplyMs: firstMins && firstMins.length ? Math.round(median(firstMins) * 60000) : null,
    meanFirstReplyMs: !none && X.x_first > 0 ? Math.round(X.x_firstMin / X.x_first * 60000) : null,
    conversationsDone: available ? done : null, reopened: available ? reopened : null, reopenRate: available ? pct(reopened, done) : null
  };
  const nums = { deliveryRate: [X.x_deliv, known], failureRate: [X.x_fail + X.x_refuse, known], editedShare: [X.x_edit, aiSent], reopenRate: [reopened, done] };
  return { v, nums, available, none };
}
/** The earlier window's number beside this one: `prev`, and `delta` / `deltaPct` only when both are known (`field` = "value", or "count" on a kind). */
const withDelta = (m, prev, better, field) => {
  const now = m[field || "value"];
  m.prev = prev == null ? null : prev;
  m.delta = now != null && prev != null ? r1(now - prev) : null;
  m.deltaPct = now != null && prev ? r1((now - prev) / Math.abs(prev) * 100) : null;
  if (better !== undefined) m.better = better;
  return m;
};

/** Why a number is an estimate: the stations that fed it are typed-name or phone-credited. */
function whyOf(stations, scanBased) {
  const list = [...stations], why = [];
  if (list.some(s => TYPED.has(s))) why.push(WHY_TYPED);
  if (scanBased && list.some(s => PHONE.has(s))) why.push(WHY_PHONE);
  return why;
}
/* the item notes: a plain reason, only fixed phrases */
function noteOf(kind, e) {
  const d = e.detail || "", tail = d ? ": " + d : "";
  switch (kind) {
    case "undone": return "Undo pressed" + tail;
    case "reprint": return "Label printed again" + tail;
    case "rescan": return "Same order again" + tail;
    case "heldOrSkipped": { const m = /^(held|skipped):\s*(\w+)/i.exec(d); return m ? `${m[1][0].toUpperCase()}${m[1].slice(1).toLowerCase()} a Review card (${REVIEW_WORDS[m[2]] || m[2]})` : "Sent back" + tail; }
    case "unknownSku": return (/^held/i.test(d) ? "Held" : "Skipped") + " an Unknown SKU card";
    case "qaFlag": { const m = /^flag to team:\s*(.+)$/i.exec(d); return "Problem flagged to the Team" + (m ? ": " + m[1] : ""); }
    case "cancelAlert": return "Cancelled order" + tail;
    case "refused": return "Refused at the station" + tail;
    case "lookupFailed": return "Order lookup failed" + tail;
    case "failed": return "Error shown" + tail;
    case "replyFailed": return "Reply did not go out" + tail;
    default: return d;
  }
}

/** The whole answer. Never throws: a failed read is named in `errors` and the answer says `partial`. */
async function issues(ctx, args) {
  ctx = ctx || {}; args = args || {};
  const A = adapter(ctx);
  const name = cleanName(args.name);
  if (!okName(name)) return { ok: false, error: "name required" };
  const key = A.keyOf(name);
  let to = args.to == null || args.to === "" ? A.today : String(args.to);
  if (!validDay(to)) return { ok: false, error: "to must be YYYY-MM-DD" };
  if (to > A.today) to = A.today;
  let from = args.from == null || args.from === "" ? addDays(to, -29) : String(args.from);
  if (!validDay(from)) return { ok: false, error: "from must be YYYY-MM-DD" };
  if (from > to) from = to;
  if (daysBetween(from, to) > LIM.maxDays) return { ok: false, error: "range too long (731 days at most)" };
  const prevWin = args.prev && validDay(args.prev.from) && validDay(args.prev.to) && args.prev.from <= args.prev.to ? { from: args.prev.from, to: args.prev.to, days: daysBetween(args.prev.from, args.prev.to) } : null;
  const limit = Math.max(1, Math.min(LIM.items, Math.floor(num(args.limit)) || LIM.items));
  const profDays = A.prof && typeof A.prof.events === "function" && !ctx.readEvents ? Math.min(LIM.maxEventDays, Math.floor(num(A.prof.eventDays)) || LIM.profEventDays) : 0;
  const eventDays = Math.max(1, Math.min(LIM.maxEventDays, Math.floor(num(args.eventDays)) || profDays || LIM.eventDays));
  const contactDays = Math.max(eventDays, LIM.contactDays);
  const crossOrders = args.crossCheck === false ? 0 : Math.max(0, Math.min(LIM.crossOrders, args.crossOrders == null ? LIM.crossOrders : Math.floor(num(args.crossOrders))));
  const errors = [], notes = [];
  const out = { ok: true, name: A.display(name), mode: A.sandbox ? "sandbox" : "real", from, to, days: daysBetween(from, to), today: A.today, prev: prevWin, found: false };

  /* 1 · the rollups (the window, and the window before it for the comparison) */
  const loadFrom = prevWin && prevWin.from < from ? prevWin.from : from;
  let R = { docs: [], capped: false, preloaded: false };
  try { R = await loadRollups(A, loadFrom, to); } catch (e) { errors.push("rollups: " + errText(e)); }
  if (R.capped) errors.push("rollups: the read was cut at its size limit");
  const agg = aggregate(A, R.docs, key, from, to);
  const aggPrev = prevWin ? aggregate(A, R.docs, key, prevWin.from, prevWin.to) : null;
  const activeDays = [...agg.keys()].sort();
  out.found = activeDays.length > 0;

  /* 2 · the newest events: every active day of the window (and inbox events of older days that have no counters) */
  const winFrom = maxDay(addDays(to, -(eventDays - 1)), from), conFrom = maxDay(addDays(to, -(contactDays - 1)), from);
  const W = await readWindow(A, key, agg, activeDays, winFrom, to, conFrom, errors);
  const evDays = new Map();
  for (const [d, m] of W.days) { const cls = []; for (const rows of m.bySpelling.values()) cls.push(...classifyRows(rows)); evDays.set(d, { scope: m.scope, cls, capped: m.capped }); }
  const readDays = [...evDays.keys()].filter(d => evDays.get(d).scope === "all").sort();
  const eventWindow = readDays.length ? { from: readDays[0], to: readDays[readDays.length - 1], days: daysBetween(readDays[0], readDays[readDays.length - 1]), capped: W.capped } : null;
  if (W.capped) notes.push("Some days have more events than one read holds; their counts from events may be low.");

  /* 3 · counts per day and kind */
  const main = analyse(agg, evDays), daysActive = main.activeDays.length;
  const prevRes = aggPrev ? analyse(aggPrev, new Map()) : null;

  /* 4 · the kinds that need the order's other events: reopened later, came back (newest finished orders only) */
  const winRows = []; for (const d of readDays) for (const c of evDays.get(d).cls) winRows.push(c);
  const ownUndone = new Set(), crossItems = []; let checked = 0, crossErr = 0, crossTried = 0;
  const found = { reopenedLater: 0, cameBack: 0 };
  for (const c of winRows) if (c.e.orderId && (c.e.action === "undo" || (c.e.action === "note" && /reopened/i.test(c.e.detail)))) ownUndone.add(c.e.orderId + "|" + c.e.station);
  if (crossOrders > 0 && A.db) {
    const done = new Map();
    for (const c of winRows.slice().sort((x, y) => y.e.at - x.e.at)) {
      const e = c.e; if (e.action !== "complete" || e.orders < 1 || !e.orderId || e.station === "inbox") continue;
      const id = e.orderId + "|" + e.station; if (!done.has(id)) done.set(id, e);
      if (done.size >= crossOrders) break;
    }
    crossTried = done.size;
    await pool([...done.values()], LIM.pool, async comp => {
      let evs; try { evs = await readOrder(A, comp.orderId); } catch (e) { crossErr++; return; }
      checked++;
      const later = evs.slice().sort((x, y) => x.at - y.at || byCommit(x, y)), mine = comp.at;
      if (!ownUndone.has(comp.orderId + "|" + comp.station)) {
        const re = later.find(x => x.at > mine && x.station === comp.station && A.keyOf(x.person) !== key && (x.action === "undo" || (x.action === "note" && /reopened/i.test(x.detail))));
        if (re) { found.reopenedLater++; crossItems.push({ id: "x~reopenedLater~" + comp.orderId + "~" + comp.station, at: re.at, day: re.day, rid: comp.orderId, station: comp.station, kind: "reopenedLater",
          note: `A completion at ${stLabel(comp.station)} was undone or reopened by another person ${dur(re.at - mine)} later` }); }
      }
      const rank = ROUTE[comp.station];
      if (rank) {
        const cb = later.find(x => x.at >= mine + LIM.cameBackMin * 60000 && ROUTE[x.station] && ROUTE[x.station] < rank && (x.action === "scan" || x.action === "complete" || x.action === "print"));
        if (cb) { found.cameBack++; crossItems.push({ id: "x~cameBack~" + comp.orderId + "~" + comp.station, at: cb.at, day: cb.day, rid: comp.orderId, station: comp.station, kind: "cameBack",
          note: `After it was finished at ${stLabel(comp.station)}, the order was ${cb.action === "scan" ? "scanned" : cb.action === "print" ? "printed" : "finished"} at ${stLabel(cb.station)} ${dur(cb.at - mine)} later` }); }
      }
    });
    if (crossErr) errors.push(`orders: ${crossErr} of ${crossTried} could not be read`);
  }

  /* 5 · the kinds, as the answer carries them */
  const byKind = [], totals = { all: 0, own: 0, system: 0, order: 0, any: false }, prevTotals = { all: 0, any: false, complete: !!prevRes };
  for (const kd of KINDS) {
    const row = { kind: kd.key, label: kd.label, count: null, definition: kd.def, def: kd.def, how: kd.how, attribution: kd.attr, coverage: kd.cov, estimated: false, why: [],
      daysCounted: 0, daysActive, complete: false, stations: {}, per100Orders: null, prev: null, delta: null, deltaPct: null, better: kd.attr === "own" ? "down" : null };
    if (kd.cross) {
      row.daysCounted = readDays.length; row.checked = { orders: checked, of: crossTried };
      if (crossOrders > 0 && checked > 0) row.count = found[kd.key];
      row.estimated = true; row.why = ["It looks at other people's events for the order, and takes the route as Sorting, Welding, Assembly, Shipping."];
      row.window = eventWindow ? { from: eventWindow.from, to: eventWindow.to } : null;
    } else {
      const m = main.kinds[kd.key];
      row.count = m.count; row.daysCounted = m.daysCounted; row.complete = m.complete; row.stations = m.stations;
      row.per100Orders = m.count != null && m.handled > 0 ? r1(m.count / m.handled * 100) : null;
      row.why = whyOf(Object.keys(m.stations), !!kd.scan); row.estimated = row.why.length > 0;
      if (m.count != null && !m.complete) row.coverage = "range-partial";
      if (prevRes) {                                                       // a comparison only when the earlier window is counted on every day, like this one
        const p = prevRes.kinds[kd.key];
        if (p.complete) withDelta(row, p.count, row.better, "count"); if (!m.complete) row.delta = row.deltaPct = null;
        if (p.count != null) { prevTotals.any = true; prevTotals.all += p.count; }
        if (!p.complete) prevTotals.complete = false;
      }
    }
    if (row.count != null) { totals.all += row.count; totals[kd.attr] += row.count; }
    byKind.push(row);
  }
  const per100 = main.handledCounted > 0 ? r1(main.totalCounted / main.handledCounted * 100) : null;

  /* the items: the newest events that are a kind, then the cross-check hits; newest first, paged */
  let items = [];
  for (const c of winRows) {
    if (!c.kind) continue;
    const kd = KIND[c.kind]; if (!kd) continue;
    items.push({ id: c.e.id, at: c.e.at, day: c.e.day, rid: c.e.orderId, number: c.e.orderId, station: c.e.station, kind: c.kind, label: kd.label, attribution: kd.attr, note: noteOf(c.kind, c.e), source: "events" });
  }
  for (const x of crossItems) items.push({ id: x.id, at: x.at, day: x.day, rid: x.rid, number: x.rid, station: x.station, kind: x.kind, label: KIND[x.kind].label, attribution: KIND[x.kind].attr, note: x.note, source: "events" });
  items.sort((a, b) => b.at - a.at || (a.id < b.id ? 1 : a.id > b.id ? -1 : 0));
  const itemsTotal = items.length;
  const cur = typeof args.cursor === "string" ? /^(\d+)~(.*)$/.exec(args.cursor) : null;
  if (cur) { const ca = +cur[1], ci = cur[2]; items = items.filter(i => i.at < ca || (i.at === ca && i.id < ci)); }
  const page = items.slice(0, limit), more = items.length > page.length;
  const next = more && page.length ? `${page[page.length - 1].at}~${page[page.length - 1].id}` : null;

  const byDay = main.activeDays.map(d => {
    const pd = main.perDay.get(d); if (!pd.src) return { day: d, total: null, own: null, system: null, order: null, source: "none" };
    const t = { total: 0, own: 0, system: 0, order: 0 };
    for (const [kk, c] of Object.entries(pd.k)) { t.total += c.n; t[KIND[kk].attr] += c.n; }
    return { day: d, total: t.total, own: t.own, system: t.system, order: t.order, source: pd.src };
  });
  const rangeKinds = byKind.filter(r => !KIND[r.kind].cross);
  const first = main.activeDays.find(d => main.perDay.get(d).src === "counters") || null;
  const counted = main.withSrc.length > 0;                               // a total only when at least one day was fully counted: never a lower bound drawn as the number
  const issuesOut = { total: counted ? totals.all : null, own: counted ? totals.own : null, system: counted ? totals.system : null, order: counted ? totals.order : null,
    per100Orders: per100, daysCounted: main.withSrc.length, daysActive, countersFrom: first, complete: daysActive > 0 && rangeKinds.every(r => r.complete),
    prevTotal: prevRes && prevTotals.complete && prevTotals.any ? prevTotals.all : null, byKind, byDay, items: page, itemsTotal, itemsCapped: more || itemsTotal > limit, next };
  if (daysActive && !issuesOut.complete) notes.push(`Issue kinds are counted on ${main.withSrc.length} of ${daysActive} working days: older days have no issue counters yet and show dashes, except the newest ${eventDays} days, which are read from events. Undo presses are counted on every day.`);

  /* 6 · the rates */
  const rates = {};
  const mkRate = (rk, r, extra) => {
    const m = RATES[rk], why = whyOf(r.sts, rk === "rescanRate" || rk === "holdRate"), has = r.d != null;
    return Object.assign({ key: rk, label: m.label, unit: "percent", value: has ? pct(r.n, r.d) : null, numerator: has ? r.n : null, denominator: has ? r.d : null, num: has ? r.n : null, den: has ? r.d : null,
      definition: m.def, def: m.def, better: m.better, coverage: m.cov, estimated: why.length > 0, why, daysCounted: r.days, daysActive, prev: null, delta: null, deltaPct: null }, extra || {});
  };
  for (const rk of ["reworkRate", "successRate", "failureRate", "holdRate", "reprintRate", "rescanRate"]) {
    const r = main.rates[rk]; rates[rk] = mkRate(rk, r, rk === "reprintRate" || rk === "rescanRate" ? { coverage: main.withSrc.length && main.withSrc.length < daysActive ? "range-partial" : "range" } : {});
    if (prevRes) {
      const p = prevRes.rates[rk], full = rk === "reprintRate" || rk === "rescanRate" ? p.days === prevRes.activeDays.length && r.days === daysActive : true;
      if (full && p.d) withDelta(rates[rk], pct(p.n, p.d), RATES[rk].better);
    }
  }
  { // first pass: orders finished at a station in the window, with no Undo, Reopen or reprint by the person afterwards
    const fin = new Map(), bad = new Set(), sts = new Set();
    for (const c of winRows) {
      const e = c.e; if (!e.orderId || e.station === "inbox") continue;
      const id = e.orderId + "|" + e.station;
      if (e.action === "complete" && e.orders >= 1) { fin.set(id, true); sts.add(e.station); }
      if (e.action === "undo" || (e.action === "note" && /reopened/i.test(e.detail)) || c.kind === "reprint") bad.add(id);
    }
    let good = 0; for (const id of fin.keys()) if (!bad.has(id)) good++;
    const m = RATES.firstPass, why = ["Only the person's own undo, reopen and reprint presses are seen.", ...whyOf(sts, true)];
    rates.firstPass = { key: "firstPass", label: m.label, unit: "percent", value: pct(good, fin.size), numerator: good, denominator: fin.size, num: good, den: fin.size, definition: m.def, def: m.def,
      better: m.better, coverage: "window", estimated: true, why, daysCounted: readDays.length, daysActive, prev: null, delta: null, deltaPct: null, window: eventWindow ? { from: eventWindow.from, to: eventWindow.to } : null };
  }
  const ratesOut = {}; for (const k of RATE_ORDER) ratesOut[k] = rates[k];

  /* 7 · contact */
  const cc = contactCounts(agg, evDays);
  const firstMins = [];
  for (const ev of evDays.values()) for (const c of ev.cls) { const ib = K.inboxOf(c.e); if (ib && ib.firstMin != null) firstMins.push(ib.firstMin); }
  const cv = contactValues(cc, firstMins);
  const pcc = aggPrev ? contactCounts(aggPrev, new Map()) : null, pcv = pcc ? contactValues(pcc, null) : null;
  const metrics = {};
  for (const [ck, [label, unit, def]] of Object.entries(CONTACT)) {
    const n = cv.nums[ck], isBase = ck === "conversationsDone" || ck === "reopened" || ck === "reopenRate";
    const m = { key: ck, label, unit, value: cv.v[ck], def, definition: def, estimated: false, coverage: isBase ? "range" : "counted", daysCounted: cc.counted, daysActive: cc.days.length, prev: null, delta: null, deltaPct: null,
      better: CONTACT_BETTER[ck] || null };
    if (n) { m.numerator = n[0]; m.denominator = n[1]; m.num = n[0]; m.den = n[1]; }
    if (ck === "medianFirstReplyMs") { m.n = firstMins.length; m.window = eventWindow ? { from: eventWindow.from, to: eventWindow.to } : null; }
    if (ck === "meanFirstReplyMs") m.n = cc.X.x_first;
    if (pcv && pcc.days.length > 0 && (isBase || pcc.counted === pcc.days.length) && ck !== "medianFirstReplyMs") { withDelta(m, pcv.v[ck], m.better); if (!isBase && !(cc.days.length > 0 && cc.counted === cc.days.length)) m.delta = m.deltaPct = null; }
    metrics[ck] = m;
  }
  const csrc = cc.source.size > 1 ? "mixed" : cc.source.size ? [...cc.source][0] : "none";
  const contact = Object.assign({ available: cv.available, source: csrc, daysCounted: cc.counted, daysActive: cc.days.length, firstReplyCount: firstMins.length, metrics }, cv.v);

  /* 8 · definitions, the answer */
  const definitions = {
    hygiene: HYGIENE,
    kinds: Object.fromEntries(KINDS.map(k => [k.key, { label: k.label, definition: k.def, how: k.how, attribution: k.attr }])),
    rates: Object.fromEntries(RATE_ORDER.map(k => [k, { label: RATES[k].label, definition: RATES[k].def }])),
    contact: Object.fromEntries(Object.entries(CONTACT).map(([k, v]) => [k, { label: v[0], definition: v[2] }])),
    attribution: { own: "Something the person pressed or did.", order: "Something about the order, not the person.", system: "A failure the screen showed them, usually not their doing." },
    notDetected: NOT_DETECTED
  };
  const estimated = byKind.some(r => r.estimated) || Object.values(ratesOut).some(r => r.estimated);
  const total = { queries: 0, docs: 0, cached: 0 };
  for (const r of Object.values(A.reads)) { total.queries += r.queries; total.docs += r.docs; total.cached += r.cached; }
  const res = Object.assign(out, { issues: issuesOut, rates: ratesOut, contact, definitions, estimated, eventWindow, firstDay: activeDays[0] || null, rollupsPreloaded: !!R.preloaded,
    reads: Object.assign({}, A.reads, { total }), notes });
  if (errors.length) { res.partial = true; res.errors = errors; }
  return res;
}

module.exports = { issues, KINDS, RATES, CONTACT, _t: { adapter, aggregate, analyse, classifyRows, kindsFromCounters, kindsFromEvents, handledOf, noteOf, LIM, TTL, fold, caches } };
