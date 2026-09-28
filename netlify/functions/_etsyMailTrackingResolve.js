/*  netlify/functions/_etsyMailTrackingResolve.js
 *
 *  One reply, one tracking number (owner, 2026-09-28). A reply that carries
 *  two different tracking numbers, as tracking pictures or in its words,
 *  has a mistake in it. The ones seen in the inbox:
 *    - Etsy keeps the first label on an order after the shop refunded it
 *      and shipped on a new label. The drafter attached the dead label and
 *      the person sending typed the real number into the words.
 *    - A replacement went out on a label Etsy's order doesn't list. The
 *      drafter attached the original, long-delivered label.
 *    - A buyer with two orders got both orders' tracking on a reply about
 *      one of them.
 *  This module works out which number is real from the carrier's scans
 *  (EtsyMail_TrackingCache), the buyer's orders, the conversation and who
 *  put each number in the reply. Clear-cut cases are settled by rules; the
 *  rest are put to a small AI model once. The reply is then rewritten
 *  around the number kept. Firestore reads only: no Etsy or carrier calls.
 */

"use strict";

const { cleanTrackingCode } = require("./_etsyMailTrackingCode");

const CACHE_COLL = "EtsyMail_TrackingCache";
const MODEL = "claude-haiku-4-5-20251001";

// The tracking number, whatever form it came in: spaces and dashes removed,
// upper case, the USPS "420" + ZIP barcode prefix dropped.
function normCode(code) {
  const c = cleanTrackingCode(String(code == null ? "" : code).replace(/[\s -]+/g, "").toUpperCase());
  const m = c.match(/^420\d{5}(?:\d{4})?(9\d{21})$/);
  return m ? m[1] : c;
}

const USPS_RX = /^9[1-5](?:\d{18,20}|\d{24})$/;
const S10_RX  = /^[A-Z]{2}\d{9}[A-Z]{2}$/;
const UPS_RX  = /^1Z[0-9A-Z]{16}$/;
function carrierShape(code) {
  const c = normCode(code);
  return USPS_RX.test(c) ? "usps" : S10_RX.test(c) ? "s10" : UPS_RX.test(c) ? "ups" : /^\d+$/.test(c) ? "digits" : "other";
}
const looksLikeTracking = (c) => USPS_RX.test(c) || S10_RX.test(c) || UPS_RX.test(c);

// Milliseconds from a Firestore Timestamp, Unix seconds or ms, or a date string.
function toMs(v) {
  if (v == null || v === "") return 0;
  if (typeof v === "number") return v > 0 ? (v < 1e11 ? v * 1000 : v) : 0;
  if (typeof v === "string") { const n = Date.parse(v); return Number.isFinite(n) ? n : (/^\d+$/.test(v) ? toMs(Number(v)) : 0); }
  if (typeof v.toMillis === "function") { try { return v.toMillis(); } catch { return 0; } }
  if (typeof v._seconds === "number") return v._seconds * 1000;
  if (typeof v.seconds === "number") return v.seconds * 1000;
  if (typeof v.ms === "number") return v.ms;
  return 0;
}
const day = (ms) => ms ? new Date(ms).toISOString().slice(0, 10) : null;
const uniq = (a) => Array.from(new Set(a));
const short = (s, n) => String(s == null ? "" : s).replace(/\s+/g, " ").trim().slice(0, n);

// A number in the text in any of its forms: grouped with spaces or dashes,
// behind the 420+ZIP barcode prefix, or inside a link.
const SEP = "[ \\u00a0-]?";
function looseRx(code, flags) {
  const c = normCode(code);
  const body = c.split("").map(ch => /[A-Za-z0-9]/.test(ch) ? ch : "\\" + ch).join(SEP);
  const pre = /^9[1-5]\d+$/.test(c) ? "(?:4" + SEP + "2" + SEP + "0" + SEP + "(?:\\d" + SEP + "){5}(?:(?:\\d" + SEP + "){4})?)?" : "";
  return new RegExp("(?<![A-Za-z0-9])" + pre + body + "(?![A-Za-z0-9])", flags || "i");
}
const mentions = (text, code) => !!code && looseRx(code).test(String(text || ""));

/** Every tracking number in a text, as tracking numbers (normCode). `known`
 *  are numbers found in any form; other numbers count when they have a
 *  tracking number's shape (USPS, international S10, UPS) or sit in a
 *  carrier's tracking link. */
function codesInText(text, known) {
  const s = String(text == null ? "" : text);
  const found = new Set();
  if (!s) return [];
  for (const k of uniq((known || []).map(normCode).filter(Boolean))) if (mentions(s, k)) found.add(k);
  // A run of digits: its own pieces when one is a whole number (a number
  // next to a year or an order number), else the run itself (a grouped
  // number such as "9214 4903 6289 ...").
  for (const m of s.matchAll(/(?<![A-Za-z0-9])\d(?:[  -]?\d){15,40}(?![A-Za-z0-9])/g)) {
    const pieces = m[0].split(/[  -]+/).map(normCode).filter(looksLikeTracking);
    if (pieces.length) { pieces.forEach(p => found.add(p)); continue; }
    const whole = normCode(m[0]);
    if (looksLikeTracking(whole)) found.add(whole);
  }
  for (const m of s.matchAll(/(?<![A-Za-z0-9])(?:[A-Za-z]{2}\d{9}[A-Za-z]{2}|1Z[0-9A-Za-z]{16})(?![A-Za-z0-9])/g)) found.add(normCode(m[0]));
  for (const m of s.matchAll(/[?&](?:q?t?c?_?tLabels\d*|tracknums?|trackNums|trackingNumbers?|tracking_?number|trknbr|InquiryNumber\d*)=([A-Za-z0-9%,]+)/gi)) {
    for (const v of m[1].split(/,|%2C/i)) {
      const c = normCode(v);
      if (looksLikeTracking(c) || /^\d{12,34}$/.test(c)) found.add(c);
    }
  }
  return [...found];
}

/** The buyer's labels from receipts (Firestore mirror docs or the drafter's
 *  context, where timestamps may already be ISO strings). */
function shipmentsFromReceipts(receipts) {
  const out = [];
  for (const r of (Array.isArray(receipts) ? receipts : [])) {
    const raw = (r && r.raw && typeof r.raw === "object") ? r.raw : (r || {});
    const receiptId = String(raw.receipt_id || (r && (r.receipt_id || r.receiptId || r.id)) || "") || null;
    const orderedAt = toMs(raw.created_timestamp || raw.create_timestamp || (r && r.created_timestamp));
    for (const s of (Array.isArray(raw.shipments) ? raw.shipments : [])) {
      if (!s || !s.tracking_code) continue;
      out.push({ code: String(s.tracking_code).replace(/\s+/g, ""), carrier: s.carrier_name || null,
                 shippedAt: toMs(s.shipment_notification_timestamp), receiptId, orderedAt });
    }
  }
  return out;
}

/** The freshest tracking-cache doc per number. Images made before
 *  2026-09-28 are cached under the full USPS barcode, so every form given
 *  is read. One Firestore getAll. */
async function loadTrackingCache(db, rawCodes) {
  const byNorm = new Map();
  for (const r of (rawCodes || [])) {
    const raw = String(r == null ? "" : r).replace(/\s+/g, "");
    if (!raw) continue;
    const n = normCode(raw);
    if (!byNorm.has(n)) byNorm.set(n, new Set([n]));
    byNorm.get(n).add(raw);
  }
  const refs = [];
  for (const [n, keys] of byNorm) for (const k of keys) if (!/[/]/.test(k)) refs.push({ n, ref: db.collection(CACHE_COLL).doc(k) });
  const out = {};
  if (!refs.length) return out;
  const snaps = await db.getAll(...refs.slice(0, 30).map(x => x.ref));
  snaps.forEach((snap, i) => {
    if (!snap || !snap.exists) return;
    const d = snap.data() || {};
    const n = refs[i].n;
    if (!out[n] || toMs(d.cachedAt) > toMs(out[n].cachedAt)) out[n] = d;
  });
  return out;
}

// What the carrier has seen. "Postage Purchased" and the like are the
// label, not a scan; "Postage refunded" means the label was cancelled.
const LABEL_EVENT_RX = /postage\s+purchased|label\s+(?:created|printed|purchased)|shipping\s+label|pre-?shipment|shipment\s+information|electronic\s+shipping|information\s+received|awaiting\s+item/i;
const VOID_EVENT_RX = /refund|\bvoid|cancel/i;
function scanFacts(doc) {
  if (!doc || typeof doc !== "object") return null;
  const events = (Array.isArray(doc.events) ? doc.events : [])
    .map(e => e && ({ at: toMs(e.at), title: String(e.title || e.status || ""), location: e.location || null, status: e.status || null }))
    .filter(e => e && e.at).sort((a, b) => a.at - b.at);
  const voids = events.filter(e => VOID_EVENT_RX.test(e.title));
  const moves = events.filter(e => !VOID_EVENT_RX.test(e.title) && !LABEL_EVENT_RX.test(e.title));
  const lastMove = moves[moves.length - 1] || null;
  const voidAt = voids.length ? voids[voids.length - 1].at : 0;
  const delivered = moves.filter(e => e.status === "delivered" || (/\bdelivered\b/i.test(e.title) && !/\bnot\s+delivered|undeliver/i.test(e.title))).pop() || null;
  let scanned = moves.length > 0;
  if (!events.length) {
    scanned = /^(in_transit|out_for_delivery|delivered|returned|exception|rerouted)$/.test(String(doc.statusKey || "")) ? true
            : doc.statusKey === "pre_shipment" ? false : null;
  }
  const label = events.find(e => LABEL_EVENT_RX.test(e.title));
  return {
    carrier    : doc.carrierDisplay || doc.carrier || null,
    status     : doc.status || null,
    scanned,
    voided     : !!voidAt && !(lastMove && lastMove.at > voidAt),
    labelAt    : (label && label.at) || toMs(doc.shipDate) || 0,
    lastScan   : lastMove ? { at: lastMove.at, title: lastMove.title, location: lastMove.location } : null,
    deliveredAt: delivered ? delivered.at : 0,
    recent     : events.slice(-4).reverse().map(e => day(e.at) + " " + short(e.title, 60) + (e.location ? " (" + short(e.location, 40) + ")" : ""))
  };
}

// ─── The decision ────────────────────────────────────────────────────────
const SYSTEM = [
  "You check a jewellery shop's reply to a customer just before it is sent.",
  "The reply carries more than one tracking number, as tracking pictures or in its words.",
  "Almost always only one is right and the others are mistakes: a label the shop cancelled or replaced, a label from another of the buyer's orders, or a number that belongs to no order.",
  "From the facts given, decide which tracking number(s) this reply should carry.",
  "Keep more than one only when each is a real, current package this reply is about: one order sent in two boxes that both have carrier scans, or a customer asking about two different orders.",
  "A number the person sending typed into the reply by hand is deliberate unless the facts show it is wrong.",
  'Answer with JSON only, no other text: {"keep":["<number>"],"keep_reason":"<under 8 words>","drop":[{"code":"<number>","reason":"<under 8 words>"}],"why":"<one sentence>"}'
].join(" ");

function describeCode(f, i) {
  const lines = [(i + 1) + ". " + f.code];
  const where = [f.inImage && "tracking picture", f.inText && (f.typed ? "typed into the words by the person sending" : "in the words")].filter(Boolean);
  lines.push("   - in the reply as: " + (where.join(", ") || "unknown"));
  if (f.orders.length) {
    for (const o of f.orders) lines.push("   - on order " + (o.receiptId || "?") + (o.orderedAt ? " (ordered " + day(o.orderedAt) + ")" : "") + (o.shippedAt ? ", label added " + day(o.shippedAt) : ""));
  } else lines.push("   - not on any of this buyer's orders");
  if (f.scan) {
    // The cached status word can lag (a refunded label still reads "In
    // Transit"), so the events decide what is said.
    const st = f.scan.voided ? "LABEL CANCELLED (postage refunded)"
      : f.scan.deliveredAt ? "delivered " + day(f.scan.deliveredAt)
      : f.scan.scanned === false ? "never scanned by the carrier (label only)"
      : f.scan.lastScan ? "last scan " + day(f.scan.lastScan.at) + " " + short(f.scan.lastScan.title, 60)
      : (f.scan.status ? '"' + f.scan.status + '"' : "status unknown");
    lines.push("   - carrier record (" + (f.scan.carrier || "carrier") + "): " + st);
    if (f.scan.recent.length) lines.push("   - latest events: " + f.scan.recent.join("; "));
  } else lines.push("   - no carrier record yet");
  if (f.mentioned.length) lines.push("   - mentioned earlier in the conversation by " + uniq(f.mentioned.map(m => m.by + (m.at ? " on " + day(m.at) : ""))).join(", "));
  return lines.join("\n");
}

function buildPrompt({ facts, alive, setAside, customerText, draftText, now }) {
  return [
    "Today: " + day(now),
    "Customer's latest message:\n\"\"\"\n" + short(customerText, 1500) + "\n\"\"\"",
    "The reply:\n\"\"\"\n" + String(draftText || "").slice(0, 2000) + "\n\"\"\"",
    "Tracking numbers still in question:\n" + alive.map((c, i) => describeCode(facts[c], i)).join("\n"),
    setAside.length ? "Already set aside (clear-cut): " + setAside.map(d => d.code + " (" + d.reason + ")").join("; ") : ""
  ].filter(Boolean).join("\n\n");
}

async function defaultAskModel({ system, user, timeoutMs }) {
  if (!process.env.ANTHROPIC_API_KEY) throw new Error("no AI key configured");
  const { callClaudeRaw } = require("./_etsyMailAnthropic");
  const res = await callClaudeRaw({
    model: MODEL, maxTokens: 400, useThinking: false, timeoutMs,
    system, messages: [{ role: "user", content: [{ type: "text", text: user }] }]
  });
  return (res.content || []).filter(b => b.type === "text").map(b => b.text).join("\n");
}

function withTimeout(p, ms) {
  let t;
  return Promise.race([
    Promise.resolve(p).finally(() => clearTimeout(t)),
    new Promise((_, rej) => { t = setTimeout(() => rej(new Error("timed out after " + ms + " ms")), ms); })
  ]);
}

const isReal = (f) => f.orders.length > 0 || f.scan && f.scan.scanned === true || f.typed;

// The model's answer, or null when it cannot be used: every number must be
// one from the reply, at least one kept, none both kept and dropped, and
// more than one kept only when each is a real package.
function parseVerdict(text, alive, facts) {
  const m = String(text || "").match(/\{[\s\S]*\}/);
  if (!m) return null;
  let j;
  try { j = JSON.parse(m[0]); } catch { return null; }
  if (!j || !Array.isArray(j.keep)) return null;
  const keepAll = j.keep.map(normCode).filter(Boolean);
  if (!keepAll.length || keepAll.some(c => !facts[c])) return null;
  let keep = uniq(keepAll.filter(c => alive.includes(c)));
  if (keep.length > 1) keep = keep.filter(c => isReal(facts[c]));
  if (!keep.length) return null;
  const reasons = {};
  for (const d of (Array.isArray(j.drop) ? j.drop : [])) {
    const c = normCode(d && (d.code || d));
    if (!facts[c]) return null;
    if (keep.includes(c)) return null;
    if (d && d.reason) reasons[c] = short(d.reason, 80);
  }
  return { keep, reasons, keepReason: short(j.keep_reason, 80), why: short(j.why, 300) };
}

function keptReasonFor(f) {
  if (f.typed) return "the number typed into the reply";
  if (f.scan && f.scan.deliveredAt) return "the label " + (f.scan.carrier || "the carrier") + " delivered";
  if (f.scan && f.scan.scanned) return "the label " + (f.scan.carrier || "the carrier") + " scanned";
  if (f.orders.length) return "the label on order " + (f.orders[0].receiptId || "");
  return "the newest label";
}

/** Decide which tracking number(s) one reply keeps.
 *  imageCodes / textCodes : numbers on the reply's tracking pictures and in its words
 *  typedCodes             : numbers the person sending typed in (not from the AI)
 *  shipments              : the buyer's labels [{ code, carrier, shippedAt, receiptId, orderedAt }]
 *  cache                  : EtsyMail_TrackingCache docs by number (loadTrackingCache)
 *  history                : conversation [{ direction, text, at }]
 *  askModel               : ({ system, user, timeoutMs }) => answer text (tests pass a stub)
 *  Returns null for fewer than two different numbers, else
 *  { codes, kept, dropped:[{code, reason}], method: "rules"|"ai"|"fallback", why, keptReason, evidence }. */
async function resolveTrackingConflict({
  imageCodes = [], textCodes = [], typedCodes = [], shipments = [], cache = {}, history = [],
  customerText = "", draftText = "", askModel = defaultAskModel, timeoutMs = 6000, now = Date.now()
} = {}) {
  const img = imageCodes.map(normCode).filter(Boolean);
  const txt = textCodes.map(normCode).filter(Boolean);
  const typed = new Set(typedCodes.map(normCode).filter(Boolean));
  const codes = uniq(img.concat(txt));
  if (codes.length < 2) return null;

  const facts = {};
  for (const c of codes) {
    facts[c] = {
      code: c, inImage: img.includes(c), inText: txt.includes(c), typed: typed.has(c),
      orders: shipments.filter(s => s && s.code && normCode(s.code) === c)
        .map(s => ({ receiptId: s.receiptId || null, shippedAt: toMs(s.shippedAt), orderedAt: toMs(s.orderedAt), carrier: s.carrier || null })),
      scan: scanFacts(cache && (cache[c] || cache[String(c)])),
      mentioned: (history || []).filter(m => m && mentions(m.text, c))
        .map(m => ({ by: m.direction === "inbound" ? "the customer" : "the shop", at: toMs(m.at) }))
    };
  }
  const boughtAt = (c) => {
    const f = facts[c];
    const t = f.orders.map(o => o.shippedAt).filter(Boolean);
    return t.length ? Math.max(...t) : (f.scan && f.scan.labelAt) || 0;
  };

  let alive = codes.slice();
  const dropped = [];
  const why = [];
  const drop = (c, reason) => { if (!alive.includes(c)) return; alive = alive.filter(x => x !== c); dropped.push({ code: c, reason }); };

  // 1. A cancelled label (postage refunded, never scanned) is dead.
  const voided = alive.filter(c => facts[c].scan && facts[c].scan.voided && !facts[c].scan.scanned);
  if (voided.length && voided.length < alive.length) {
    voided.forEach(c => drop(c, "label cancelled, postage refunded"));
    why.push("the carrier shows the postage for " + voided.join(", ") + " was refunded");
  }
  // 2. A number known nowhere (no order, no scan, not typed by the sender,
  //    never in the conversation) loses to one on the buyer's orders or scanned.
  const known = (c) => isReal(facts[c]) || facts[c].mentioned.length > 0;
  const unknown = alive.filter(c => !known(c));
  if (unknown.length && unknown.length < alive.length &&
      alive.some(c => facts[c].orders.length || (facts[c].scan && facts[c].scan.scanned))) {
    unknown.forEach(c => drop(c, "not on any of the buyer's orders"));
    why.push(unknown.join(", ") + " is on none of the buyer's orders and has no carrier record");
  }
  // 3. Two labels on one order: the older one that never got a scan was replaced.
  for (const c of alive.slice()) {
    const f = facts[c];
    if (!(f.scan && f.scan.scanned === false) || !boughtAt(c)) continue;
    const newer = alive.find(o => o !== c && facts[o].orders.some(x => f.orders.some(y => y.receiptId && y.receiptId === x.receiptId)) && boughtAt(o) > boughtAt(c));
    if (newer) { drop(c, "replaced label, never scanned"); why.push(c + " was replaced by the newer label " + newer + " on the same order"); }
  }
  // 4. A scanned label beats a never-scanned one bought before it.
  for (const c of alive.slice()) {
    const f = facts[c];
    if (!(f.scan && f.scan.scanned === false) || !boughtAt(c)) continue;
    const later = alive.find(o => o !== c && facts[o].scan && facts[o].scan.scanned === true && boughtAt(o) > boughtAt(c));
    if (later) { drop(c, "older label, never scanned"); why.push(c + " was never scanned while the later label " + later + " was"); }
  }

  const finish = (keep, method, text, reasons = {}, keptReason = "") => {
    const kept = uniq(keep);
    for (const c of alive) if (!kept.includes(c)) dropped.push({ code: c, reason: reasons[c] || (method === "fallback" ? "less likely the current label" : "not the package this reply is about") });
    return {
      codes, kept, dropped: dropped.filter(d => !kept.includes(d.code)), method,
      why: short(text || why.join("; "), 400),
      keptReason: keptReason || (kept.length === 1 ? keptReasonFor(facts[kept[0]]) : "each is a package this reply is about"),
      evidence: codes.map(c => {
        const f = facts[c];
        return { code: c, inImage: f.inImage, inText: f.inText, typed: f.typed, orders: f.orders.map(o => o.receiptId).filter(Boolean),
                 scanned: f.scan ? f.scan.scanned : null, voided: !!(f.scan && f.scan.voided), deliveredAt: f.scan && f.scan.deliveredAt ? day(f.scan.deliveredAt) : null };
      })
    };
  };

  if (alive.length === 1) return finish(alive, "rules");
  // One order sent in several boxes, each scanned: all of them are real.
  const receiptsOf = (c) => facts[c].orders.map(o => o.receiptId).filter(Boolean);
  const shared = receiptsOf(alive[0]).filter(r => alive.every(c => receiptsOf(c).includes(r)));
  if (shared.length && alive.every(c => facts[c].scan && facts[c].scan.scanned === true && !facts[c].scan.voided)) {
    return finish(alive, "rules", (why.length ? why.join("; ") + "; " : "") + "order " + shared[0] + " went out in " + alive.length + " boxes, each scanned by the carrier",
      {}, "one order sent in " + alive.length + " boxes");
  }

  let err = null;
  if (typeof askModel === "function" && timeoutMs > 0) {
    try {
      const user = buildPrompt({ facts, alive, setAside: dropped, customerText, draftText, now });
      const answer = await withTimeout(askModel({ system: SYSTEM, user, timeoutMs }), timeoutMs + 500);
      const v = parseVerdict(answer, alive, facts);
      if (v) return finish(v.keep, "ai", (why.length ? why.join("; ") + "; " : "") + (v.why || ""), v.reasons, v.keepReason);
      err = new Error("the AI's answer could not be used");
    } catch (e) { err = e; }
  }
  // Without the AI: the number typed by the sender, then one on the buyer's
  // orders, then a scanned one, then the newest.
  const newest = (c) => Math.max(boughtAt(c), (facts[c].scan && facts[c].scan.lastScan && facts[c].scan.lastScan.at) || 0);
  const rank = (c) => [facts[c].typed ? 1 : 0, facts[c].orders.length ? 1 : 0, facts[c].scan && facts[c].scan.scanned ? 1 : 0, newest(c)];
  const best = alive.slice().sort((a, b) => { const x = rank(a), y = rank(b); for (let i = 0; i < x.length; i++) if (x[i] !== y[i]) return y[i] - x[i]; return 0; })[0];
  return finish([best], "fallback", (why.length ? why.join("; ") + "; " : "") + "picked without the AI (" + short(err ? err.message : "not asked", 120) + ")");
}

// ─── Applying the decision ───────────────────────────────────────────────
function trackingLink(code) {
  const shape = carrierShape(code);
  if (shape === "usps") return "https://tools.usps.com/go/TrackConfirmAction?tLabels=" + code;
  if (shape === "ups") return "https://www.ups.com/track?tracknum=" + code;
  return null;
}

/** The reply's words with every dropped number replaced by the number kept
 *  (or, with several kept, by a kept one the words don't mention yet; with
 *  none left to use, the number is taken out). A link for another carrier
 *  is replaced whole. No dropped number is left in the words. */
function fixTrackingText(text, res) {
  let s = String(text == null ? "" : text);
  if (!res || !Array.isArray(res.dropped) || !res.dropped.length) return s;
  const kept = (res.kept || []).map(normCode).filter(Boolean);
  let removed = false;
  for (const d of res.dropped) {
    const code = normCode(d && (d.code || d));
    if (!code || !mentions(s, code)) continue;
    const to = kept.length === 1 ? kept[0] : (kept.find(k => !mentions(s, k)) || null);
    s = s.replace(/https?:\/\/[^\s<>"'\])]+/gi, (match) => {
      const [, url, tail] = match.match(/^(.*?)([.,;:!?]*)$/);
      if (!mentions(url, code)) return match;
      if (!to) { removed = true; return tail; }
      if (carrierShape(to) === carrierShape(code)) return url.replace(looseRx(code, "gi"), to) + tail;
      return (trackingLink(to) || to) + tail;
    });
    s = s.replace(looseRx(code, "gi"), () => { if (!to) removed = true; return to || ""; });
  }
  if (removed) {
    s = s.replace(/[ \t]{2,}/g, " ").replace(/[ \t]+([.,;:!?])/g, "$1").replace(/^[ \t]*[:\-–•]?[ \t]*$/gm, "").replace(/\n{3,}/g, "\n\n");
  }
  return s;
}

/** Attachments without the tracking pictures of dropped numbers. */
function dropTrackingAttachments(list, res) {
  const gone = new Set(((res && res.dropped) || []).map(d => normCode(d.code)));
  if (!gone.size) return Array.isArray(list) ? list : [];
  return (Array.isArray(list) ? list : []).filter(a => {
    if (!a) return false;
    if (a.trackingCode && gone.has(normCode(a.trackingCode))) return false;
    const t = String(a.storagePath || "").match(/^etsymail\/tracking\/([A-Za-z0-9]+)\.png$/);
    return !(t && gone.has(normCode(t[1])));
  });
}

/** "9214…0327" for a short note. */
function shortCode(code) {
  const c = normCode(code);
  return c.length > 10 ? c.slice(0, 4) + "…" + c.slice(-4) : c;
}

/** One line for the inbox: what was kept and what was taken out. */
function describeResolution(res) {
  if (!res || !res.dropped || !res.dropped.length) return "";
  return "Kept tracking " + res.kept.map(shortCode).join(" + ") + (res.keptReason ? " (" + res.keptReason + ")" : "") +
    "; removed " + res.dropped.map(d => shortCode(d.code) + (d.reason ? " (" + d.reason + ")" : "")).join(", ");
}

module.exports = {
  normCode, codesInText, shipmentsFromReceipts, loadTrackingCache, scanFacts, toMs,
  resolveTrackingConflict, fixTrackingText, dropTrackingAttachments, describeResolution, shortCode, mentions
};
