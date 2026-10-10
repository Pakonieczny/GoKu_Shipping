/*  netlify/functions/_charmNestSandboxPull.js
 *  ═══════════════════════════════════════════════════════════════════════
 *  The sandbox's order source (Paul, 10 Oct 2026: "when in the Sandbox mode always pull the 250 newest orders from etsy.
 *  Never retain previous or old orders from prior Sandbox runs/testing."). Op sandboxPullOrders (charmNestLibrary.js)
 *  calls pull() below, which
 *    1. checks the daily budget and the retry marker in ONE transaction (Charm_Sandbox/pulls: counts and start ids only),
 *    2. discards the previous order set (Charm_Sandbox/current, the old file under charmnest/sandbox/orders-pull/, and the
 *       stream that was playing it) BEFORE asking Etsy, so any failure leaves the sandbox empty, never the old orders,
 *    3. reads the newest 250 receipts, with their transactions, from Etsy: 3 calls (pages of 100, 100 and 50), with the
 *       OAuth token and request headers the inbox and the receipts mirror already use (_etsyMailEtsy.js),
 *    4. stores them as one JSON file and the pointer document Charm_Sandbox/current, which etsySandbox.js serves and
 *       the stream (charmNestLibrary sandboxStream) plays,
 *    5. starts the stream for the new set.
 *  No other Etsy call is made: no listing, image or per-order look-up (SKUs stay on the 7-day listingSkus cache).
 *  Only the sandbox's own records are written; the one thing outside is the inbox's Etsy call counter (_etsyApiMeter).
 *  The families are in charm-nest-sandbox-families.js: Charm_Sandbox/current and /stream and the Storage prefix are wiped,
 *  Charm_Sandbox/pulls (the budget, no order data) is kept.
 *  ═══════════════════════════════════════════════════════════════════════ */
"use strict";
const crypto = require("crypto");
const admin = require("./firebaseAdmin");
const db = admin.firestore();
const FV = admin.firestore.FieldValue;

const SANDBOX = "Charm_Sandbox";
const DIR = "charmnest/sandbox/orders-pull/";
const SOURCE = "etsy-pull";
const WANT = 250;                 // the newest receipts kept
const PAGES = [[0, 100], [100, 100], [200, 50]];   // [offset, limit]: 3 calls
const MAX_PULLS = 6;              // sandbox starts per UTC day (Etsy's day): 6 x 3 = 18 calls
const MAX_CALLS = 20;             // and never more than this many sandbox receipt calls in a day
const CLAIM_MS = 90000;           // a pull that did not finish releases its claim after this
const FETCH_MS = 6000;            // one Etsy call may take this long
const WORK_MS = 8200;             // and the whole pull this long: the function's own limit is 10 s, and a pull cut off mid-way leaves a claim
const OAUTH_DOC = "config/etsyOauth";   // where production keeps the Etsy token (_etsyMailEtsy.js)
const TOKEN_BUFFER_MS = 2 * 60 * 1000;  // the same margin _etsyMailEtsy.js refreshes at

const ledgerRef = () => db.collection(SANDBOX).doc("pulls");
const currentRef = () => db.collection(SANDBOX).doc("current");
const streamRef = () => db.collection(SANDBOX).doc("stream");
const dayOf = ms => new Date(ms).toISOString().slice(0, 10);
const nextDay = ms => Date.parse(dayOf(ms) + "T00:00:00Z") + 86400000;

/** Etsy's own flags decide: paid, not shipped, not cancelled (the same test etsySandbox.js and the stream use). */
const isOpen = r => !!r && r.is_paid !== false && r.was_paid !== false && !r.is_shipped && !r.was_shipped && !r.is_canceled && !r.was_canceled && !/cancel/i.test(r.status || "");
const createdMs = r => (+(r && (r.created_timestamp || r.create_timestamp || r.creation_tsz)) || 0) * 1000;
/** Why a receipt is not open (for the count only; the receipt itself is stored as Etsy returned it). */
function closedWhy(r) {
  if (r.is_canceled || r.was_canceled || /cancel/i.test(r.status || "")) return "canceled";
  if (r.is_paid === false || r.was_paid === false) return "unpaid";
  return "shipped";
}

/* ── the budget: one small document, today's counts and the starts that spent them ── */
const fresh = (led, day) => (led && led.day === day
  ? { day, pulls: +led.pulls || 0, calls: +led.calls || 0, tokenRefreshes: +led.tokenRefreshes || 0, starts: Array.isArray(led.starts) ? led.starts : [], claim: led.claim || null }
  : { day, pulls: 0, calls: 0, tokenRefreshes: 0, starts: [], claim: null });
const budget = led => ({ day: led.day, pullsToday: led.pulls, cap: MAX_PULLS, capLeft: Math.max(0, MAX_PULLS - led.pulls), callsToday: led.calls, callsCap: MAX_CALLS });
/** For sandboxStatus: today's budget (one read). */
async function readBudget(now) {
  const s = await ledgerRef().get();
  return budget(fresh(s.exists ? s.data() : null, dayOf(now || Date.now())));
}

const fail = (reason, error, status, extra) => Object.assign({ ok: false, error, reason, empty: reason !== "busy" && reason !== "bad", status }, extra || {});

/** Remove the previous order set: the pointer first (readers see an empty sandbox at once), then the stream, then the files. */
async function discard() {
  const was = await currentRef().get(), old = was.exists ? String((was.data() || {}).path || "") : "";
  await currentRef().delete();
  const bucket = admin.storage().bucket();
  // the file the old pointer named (an old snapshot lives elsewhere under charmnest/sandbox/; never the master files)
  await Promise.all([streamRef().delete(), /^charmnest\/sandbox\//.test(old) && !/^charmnest\/sandbox\/master\//.test(old) && !old.includes("..") ? bucket.file(old).delete({ ignoreNotFound: true }) : null]);
  let pageToken;
  do {
    const [list, next] = await bucket.getFiles({ prefix: DIR, autoPaginate: false, maxResults: 200, pageToken });
    await Promise.all(list.map(f => f.delete({ ignoreNotFound: true })));
    pageToken = next && next.pageToken;
  } while (pageToken);
}

/** One more look at the ledger when the pull is over: the claim is released, the calls it made are counted. */
async function settle(startId, tally, ok, now) {
  const day = dayOf(now);
  return db.runTransaction(async tx => {
    const s = await tx.get(ledgerRef()), led = fresh(s.exists ? s.data() : null, day);
    led.calls += tally.pages; led.tokenRefreshes += tally.refreshes;
    if (!tally.pages && !tally.refreshes && led.pulls > 0) led.pulls -= 1;   // it never reached Etsy: the slot is given back
    if (led.claim && led.claim.startId === startId) led.claim = null;
    led.starts = [{ id: startId, at: now, ok: !!ok, calls: tally.pages }].concat(led.starts.filter(x => x.id !== startId)).slice(0, 40);
    tx.set(ledgerRef(), Object.assign({}, led, { updatedAt: FV.serverTimestamp() }));
    return led;
  });
}

/* ── Etsy: the token production already keeps, the request shape production already uses ── */
const etsyEnv = () => ({ shop: process.env.SHOP_ID, id: process.env.CLIENT_ID, secret: process.env.CLIENT_SECRET || process.env.ETSY_SHARED_SECRET });
/** Is the stored token due a refresh (then asking for it makes one more call, to Etsy's token endpoint)? */
async function tokenDue(now) {
  const s = await db.doc(OAUTH_DOC).get();
  if (!s.exists) return { due: false, seeded: false };
  const t = s.data() || {}, exp = (typeof t.expires_at_ms === "number" && t.expires_at_ms) || (typeof t.expires_at === "number" && t.expires_at) || 0;
  return { seeded: !!t.refresh_token, due: !(t.access_token && exp - now > TOKEN_BUFFER_MS) };
}
class EtsyError extends Error { constructor(reason, message, status) { super(message); this.reason = reason; this.etsyStatus = status || 0; } }
async function page(fetchFn, meter, env, token, [offset, limit], tally, deadline) {
  const qs = new URLSearchParams({ limit: String(limit), offset: String(offset), sort_on: "created", sort_order: "desc" });   // any status: no filters
  const url = `https://api.etsy.com/v3/application/shops/${env.shop}/receipts?${qs}`;
  const ctl = new AbortController(), timer = setTimeout(() => ctl.abort(), Math.max(1000, Math.min(FETCH_MS, deadline - Date.now()))), mark = meter.bump("sandbox.receiptsPage");
  tally.pages += 1;
  try {
    let res;
    try { res = await fetchFn(url, { headers: { Authorization: `Bearer ${token}`, "x-api-key": `${env.id}:${env.secret}`, Accept: "application/json" }, signal: ctl.signal }); }
    catch (e) { mark.failNet(); throw new EtsyError("etsy", `Etsy did not answer (${e && e.name === "AbortError" ? "timed out" : "network error"}), so the sandbox is empty. Press Start to try again.`); }
    mark.fromHttp(res.status);
    if (!res.ok) {
      const text = await res.text().catch(() => ""), retry = parseInt((res.headers && res.headers.get && res.headers.get("retry-after")) || "0", 10) || 0;
      if (res.status === 401 || res.status === 403) throw new EtsyError("token", `Etsy refused the server's sign-in (HTTP ${res.status}), so the sandbox is empty.`, res.status);
      if (res.status === 429) {
        if (/daily|day/i.test(text) || retry > 3600) throw new EtsyError("limit", "Etsy says its daily limit is reached, so the sandbox is empty until tomorrow.", 429);
        throw new EtsyError("etsy", "Etsy is limiting requests right now (HTTP 429), so the sandbox is empty. Try again in a minute.", 429);
      }
      throw new EtsyError("etsy", `Etsy did not answer (HTTP ${res.status}), so the sandbox is empty. Press Start to try again.`, res.status);
    }
    let data; try { data = await res.json(); } catch (_) { data = null; }
    if (!data || !Array.isArray(data.results)) throw new EtsyError("etsy", "Etsy sent a list the server could not read, so the sandbox is empty. Press Start to try again.");
    return data.results;
  } finally { clearTimeout(timer); }
}
/** The newest 250 receipts: page one first (a failure there costs one call), then the other two together. */
async function readNewest(deps, tally, now) {
  const deadline = Date.now() + WORK_MS;
  const env = etsyEnv();
  if (!env.shop || !env.id || !env.secret) throw new EtsyError("token", "The server has no Etsy settings, so the sandbox is empty.");
  const due = await tokenDue(now);
  if (!due.seeded) throw new EtsyError("token", "The server's Etsy sign-in is not set up, so the sandbox is empty.");
  let token;
  try { token = await deps.token(); tally.refreshes = due.due ? 1 : 0; }
  catch (e) { tally.refreshes = due.due ? 1 : 0; console.warn("[sandboxPull] Etsy token not available:", (e && e.message || "").slice(0, 200)); throw new EtsyError("token", "The server's Etsy sign-in is not working, so the sandbox is empty."); }
  const first = await page(deps.fetch, deps.meter, env, token, PAGES[0], tally, deadline);
  let rest = [];
  if (first.length >= PAGES[0][1]) {
    const more = await Promise.allSettled(PAGES.slice(1).map(p => page(deps.fetch, deps.meter, env, token, p, tally, deadline)));
    const bad = more.find(r => r.status === "rejected");
    if (bad) throw bad.reason;
    rest = more.flatMap(r => r.value);
  }
  // newest first; a receipt that came into the first page while the others were read could show twice: once
  const seen = new Set(), list = [];
  for (const r of first.concat(rest)) { const id = r && String(r.receipt_id); if (!id || !/^\d+$/.test(id) || seen.has(id)) continue; seen.add(id); list.push(r); }
  list.sort((a, b) => createdMs(b) - createdMs(a) || Number(b.receipt_id) - Number(a.receipt_id));
  return list.slice(0, WANT);
}

const defaults = () => ({
  fetch: (...a) => require("node-fetch")(...a),
  token: () => require("./_etsyMailEtsy").getValidEtsyAccessToken(),
  meter: require("./_etsyApiMeter")
});
const safeId = s => (typeof s === "string" && /^[A-Za-z0-9_.:-]{8,80}$/.test(s) ? s : "");

/** The answer for a set that stands. */
function setAnswer(cur, led, extra) {
  return Object.assign({ ok: true, already: false, startId: cur.startId, source: SOURCE, label: cur.label || `${cur.count} newest Etsy orders`, count: cur.count, open: cur.open, closed: cur.closed, closedBy: cur.closedBy || {},
    pulledAt: cur.pulledAt, newestAt: cur.newestAt || null, oldestAt: cur.oldestAt || null, calls: 0, tokenRefreshes: 0 }, budget(led), { path: cur.path }, extra || {});
}
async function startStream(ctx, b) {
  if (b.stream === false) return { stream: null };
  try {
    const r = await ctx.stream({ action: "ensure", seed: b.seed, speed: b.speed });
    return r && r.error ? { stream: null, streamError: String(r.error) } : { stream: (r && r.stream) || null };
  } catch (e) { console.warn("[sandboxPull] stream not started:", e && e.message); return { stream: null, streamError: String((e && e.message) || e).slice(0, 200) }; }
}

/** ctx: { stream(b) → the library's sandboxStream, by } · deps: { fetch, token, meter } (tests replace them). */
async function pull(b, ctx, deps) {
  b = b || {};
  const startId = safeId(b.startId);
  if (!startId) return fail("bad", "The pull needs a startId of 8 to 80 plain characters, one per sandbox start.", 400);
  deps = Object.assign(defaults(), deps || {});
  const now = Date.now(), day = dayOf(now);
  // the retry marker, the claim and the budget: one transaction
  const gate = await db.runTransaction(async tx => {
    const [ls, cs] = await Promise.all([tx.get(ledgerRef()), tx.get(currentRef())]);
    const led = fresh(ls.exists ? ls.data() : null, day), cur = cs.exists ? cs.data() : null;
    if (cur && cur.source === SOURCE && cur.startId === startId) return { kind: "already", cur, led };
    if (led.claim && now - (+led.claim.at || 0) < CLAIM_MS) return { kind: "busy", led };
    const mine = led.starts.find(x => x.id === startId);
    if (mine && mine.ok) return { kind: "startUsed", led };
    if (led.pulls >= MAX_PULLS || led.calls + PAGES.length > MAX_CALLS) return { kind: "cap", led };
    const next = Object.assign({}, led, { pulls: led.pulls + 1, claim: { startId, at: now } });
    tx.set(ledgerRef(), Object.assign({}, next, { updatedAt: FV.serverTimestamp() }));
    return { kind: "go", led: next };
  });
  const base = budget(gate.led);
  if (gate.kind === "already") return Object.assign(setAnswer(gate.cur, gate.led, { already: true }), await startStream(ctx, b));
  if (gate.kind === "busy") return fail("busy", "This start is already pulling its orders; wait a moment and ask again.", 409, Object.assign({ calls: 0, tokenRefreshes: 0 }, base));
  if (gate.kind === "startUsed") return fail("startUsed", "This start already pulled its orders and they were cleared; begin a new start.", 409, Object.assign({ calls: 0, tokenRefreshes: 0 }, base));
  // from here the old orders go, whatever happens next
  const tally = { pages: 0, refreshes: 0 };
  try {
    await discard();
    if (gate.kind === "cap") {
      return fail("cap", `Today's ${MAX_PULLS} sandbox pulls from Etsy are used up, so the sandbox stays empty until tomorrow (UTC midnight).`, 429, Object.assign({ calls: 0, tokenRefreshes: 0, resetAt: new Date(nextDay(now)).toISOString() }, base));
    }
    const list = await readNewest(deps, tally, now);
    if (!list.length) throw new EtsyError("etsy", "Etsy sent no orders, so the sandbox is empty. Press Start to try again.");
    const closedBy = { shipped: 0, canceled: 0, unpaid: 0 };
    let open = 0;
    for (const r of list) { if (isOpen(r)) open += 1; else closedBy[closedWhy(r)] += 1; }
    const pullId = `${now}-${crypto.randomBytes(3).toString("hex")}`, path = `${DIR}${pullId}.json`;
    const body = Buffer.from(JSON.stringify({ at: now, count: list.length, source: SOURCE, startId, receipts: list }));
    const file = admin.storage().bucket().file(path);
    await file.save(body, { resumable: false, contentType: "application/json", metadata: { cacheControl: "no-store" } });
    const doc = { path, source: SOURCE, startId, count: list.length, open, closed: list.length - open, closedBy, at: now, pulledAt: now, newestAt: createdMs(list[0]) || null, oldestAt: createdMs(list[list.length - 1]) || null,
      takenBy: String(b.by || "").slice(0, 80) || null, label: `${list.length} newest Etsy orders`, calls: tally.pages, tokenRefreshes: tally.refreshes, bytes: body.length, note: null };
    try { await currentRef().set(Object.assign({}, doc, { updatedAt: FV.serverTimestamp() })); }
    catch (e) { await file.delete({ ignoreNotFound: true }).catch(() => {}); throw e; }
    const [led, , started] = await Promise.all([settle(startId, tally, true, now), Promise.resolve().then(() => deps.meter.flushNow()).catch(() => {}), startStream(ctx, b)]);
    return Object.assign(setAnswer(doc, led, { calls: tally.pages, tokenRefreshes: tally.refreshes }), started);
  } catch (e) {
    let led = null;
    try { led = await settle(startId, tally, false, now); } catch (se) { console.warn("[sandboxPull] budget not settled:", se && se.message); }
    try { await discard(); } catch (de) { console.warn("[sandboxPull] old orders not cleared:", de && de.message); }
    try { await deps.meter.flushNow(); } catch (_) {}
    const known = e instanceof EtsyError;
    if (!known) console.error("[sandboxPull]", e);
    const view = budget(led || gate.led);
    const statuses = { cap: 429, etsy: 502, limit: 502, token: 503 };
    return fail(known ? e.reason : "etsy", known ? e.message : "The pull did not finish, so the sandbox is empty. Press Start to try again.", statuses[known ? e.reason : "etsy"] || 502,
      Object.assign({ calls: tally.pages, tokenRefreshes: tally.refreshes }, view));
  }
}

module.exports = { pull, readBudget, isOpen, SOURCE, WANT, MAX_PULLS, MAX_CALLS, DIR };
