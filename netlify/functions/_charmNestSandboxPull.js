/*  netlify/functions/_charmNestSandboxPull.js
 *  ═══════════════════════════════════════════════════════════════════════
 *  The sandbox's order source (Paul, 10 Oct 2026: "when in the Sandbox mode always pull the 250 newest orders from etsy.
 *  Never retain previous or old orders from prior Sandbox runs/testing."). Op sandboxPullOrders (charmNestLibrary.js)
 *  calls pull() below. HOW MANY (Paul, 11 Oct 2026: "adjust the # of newest listings the Sandbox test pulls from Etsy"): the
 *  number is a setting, 250 until it is changed, kept in the ONE document config/charmNestSandboxPull (not sandbox data: no
 *  wipe touches it), read once per pull (readLimit) and held to 10..500 by the server (clampWant). Op sandboxPullLimitSet
 *  writes it behind the Charm repo password door (_charmNestRepoAuth.js, need "modify"); reading it is free (sandboxStatus).
 *  Each page of up to 100 orders is one Etsy call, so 250 is 3 calls and 500 is 5; the daily caps below are unchanged. pull()
 *    1. checks the daily budget and the retry marker in ONE transaction (Charm_Sandbox/pulls: counts and start ids only),
 *    2. discards the previous order set (Charm_Sandbox/current, the old file under charmnest/sandbox/orders-pull/, and the
 *       stream that was playing it) BEFORE asking Etsy, so any failure leaves the sandbox empty, never the old orders,
 *    3. reads the newest receipts (250 unless the setting says otherwise), with their transactions, from Etsy: one call per
 *       100 (250 is 3 calls: pages of 100, 100 and 50), ALL AT ONCE inside one clock that runs from the start of the pull (WORK_MS), with the
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
const WANT = 250;                 // the newest receipts kept until the setting says otherwise (Paul, 10 Oct)
const MIN_WANT = 10, MAX_WANT = 500;   // what the setting may be (Paul, 11 Oct): 500 is 5 Etsy calls, and the 20-call day below still allows 4 such pulls
const PAGE = 100;                 // Etsy's receipts page: one call per 100 orders
const LIMIT_DOC = "config/charmNestSandboxPull";   // the setting: ONE document, a setting and not sandbox data (the wipe never touches config)
const limitRef = () => db.collection("config").doc("charmNestSandboxPull");
const MAX_PULLS = 6;              // sandbox starts per UTC day (Etsy's day): 6 x 3 = 18 calls at 250
const MAX_CALLS = 20;             // and never more than this many sandbox receipt calls in a day
const CLAIM_MS = 30000;           // a pull that did not finish releases its claim after this: a function lives 10 s (26 s at most), and the page asks the same start again for about 35 s.
                                  // (ADVPULL, 15 Oct 2026: it was 90 s, so a pull cut off at the limit made the page give up on a start the cloud would have taken over)
const FETCH_MS = 8000;            // one Etsy call may take this long (WORK_MS below is what really bounds it: every page is asked at once)
const WORK_MS = 8300;             // the Etsy part of a pull, counted from the moment the pull began (the sweep of the old files, the token and every page are inside it). The function's own limit is 10 s, and the file,
                                  // the pointer, the ledger and the stream are written after it; a pull cut off mid-way leaves a claim. (SBXPULL, 15 Oct 2026: the old clock started AFTER the clean-up and gave page one 6 s and the other pages
                                  // what was left, so a 400-order pull of four 100-order pages needed Etsy to answer twice in turn inside 8.2 s, and a slow day made it fail every time.)
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
const budget = led => ({ day: led.day, pullsToday: led.pulls, cap: null, capLeft: null, callsToday: led.calls, callsCap: null });   // (NOLIMIT, Paul 13 Oct 2026: no daily limit on the sandbox's pulls or calls: nothing is capped here, only counted)
/** For sandboxStatus: today's budget (one read). */
async function readBudget(now) {
  const s = await ledgerRef().get();
  return budget(fresh(s.exists ? s.data() : null, dayOf(now || Date.now())));
}

/* ── how many of the newest orders a pull brings: Paul's number (config/charmNestSandboxPull { v, limit, by, at }) ── */
const pagesFor = n => Math.ceil(n / PAGE);
/** The Etsy pages for the newest `n` receipts: [[offset, limit]…] (250 → [0,100] [100,100] [200,50]). */
const planFor = n => Array.from({ length: pagesFor(n) }, (_, i) => [i * PAGE, Math.min(PAGE, n - i * PAGE)]);
/** A number as the setting may hold it: { value, clamped } held to MIN_WANT..MAX_WANT, or null when it is not a number at all. */
function clampWant(x) {
  if (typeof x === "string" && x.trim()) x = Number(x.trim());
  if (typeof x !== "number" || !Number.isFinite(x)) return null;
  const v = Math.round(x);
  return v < MIN_WANT ? { value: MIN_WANT, clamped: true } : v > MAX_WANT ? { value: MAX_WANT, clamped: true } : { value: v, clamped: false };
}
/** The one plain line when a number had to be held: what the limit is and why. */
const clampLine = value => (value <= MIN_WANT
  ? `Held to ${MIN_WANT}: the fewest one pull may bring.`
  : `Held to ${MAX_WANT}: the most one pull may bring (${pagesFor(MAX_WANT)} Etsy calls).`);
/** What the browser shows for a number: the limits and what a pull costs (about: a token refresh can add one call). */
function limitInfo(value, o) {
  const pages = pagesFor(value);
  return Object.assign({ limit: value, def: WANT, min: MIN_WANT, max: MAX_WANT, perPage: PAGE, callsPerPull: pages, pullsPerDay: null, callsCap: null, pullsCap: null,
    isDefault: value === WANT, isSet: false, setBy: null, setAt: null }, o || {});
}
/** The number now (one read of one document): 250 when nothing was ever set, a stored number held to the limits whatever was written. */
async function readLimit() {
  const s = await limitRef().get(), d = s.exists ? s.data() || {} : null, c = d ? clampWant(d.limit) : null;
  return limitInfo(c ? c.value : WANT, c ? { isSet: true, setBy: String(d.by || "").slice(0, 80) || null, setAt: +d.at || null } : {});
}
/** Changes the number (the caller has passed the Charm repo door: op sandboxPullLimitSet needs a repoToken). The server holds it to the limits. */
async function setLimit(b) {
  b = b || {};
  const c = clampWant(b.limit);
  if (!c) return { ok: false, error: `Type a whole number from ${MIN_WANT} to ${MAX_WANT}.`, reason: "bad", status: 400 };
  const by = String(b.by || "").slice(0, 80), at = Date.now();
  await limitRef().set({ v: 1, limit: c.value, by, at, updatedAt: FV.serverTimestamp() });
  return Object.assign({ ok: true, clamped: c.clamped, note: c.clamped ? clampLine(c.value) : "" }, limitInfo(c.value, { isSet: true, setBy: by || null, setAt: at }));
}

const fail = (reason, error, status, extra) => Object.assign({ ok: false, error, reason, empty: reason !== "busy" && reason !== "bad", status }, extra || {});

/** Remove the previous order set: the pointer first (readers see an empty sandbox at once), then the stream, then the files. */
async function discard(defer) {
  const was = await currentRef().get(), old = was.exists ? String((was.data() || {}).path || "") : "";
  await currentRef().delete();   // (readers see an empty sandbox from here on)
  const rest = async () => {
    const bucket = admin.storage().bucket();
    // the file the old pointer named (an old snapshot lives elsewhere under charmnest/sandbox/; never the master files)
    await Promise.all([streamRef().delete(), /^charmnest\/sandbox\//.test(old) && !/^charmnest\/sandbox\/master\//.test(old) && !old.includes("..") ? bucket.file(old).delete({ ignoreNotFound: true }) : null]);
    let pageToken;
    do {
      const [list, next] = await bucket.getFiles({ prefix: DIR, autoPaginate: false, maxResults: 200, pageToken });
      await Promise.all(list.map(f => f.delete({ ignoreNotFound: true })));
      pageToken = next && next.pageToken;
    } while (pageToken);
  };
  if (!defer) return rest();
  const sweeping = rest(); sweeping.catch(() => {});   // (with `defer` the pointer is gone and the rest is swept while Etsy answers; a failure of it is raised where readNewest waits for it)
  return { sweeping };
}

/** One more look at the ledger when the pull is over: the claim is released, the calls it made are counted. */
async function settle(startId, tally, ok, now) {
  const day = dayOf(now);
  return db.runTransaction(async tx => {
    const s = await tx.get(ledgerRef()), led = fresh(s.exists ? s.data() : null, day);
    led.calls += tally.pages; led.tokenRefreshes += tally.refreshes;
    if (!tally.pages && !tally.refreshes && led.pulls > 0) led.pulls -= 1;   // it never reached Etsy: the slot is given back
    if (led.claim && led.claim.startId === startId) led.claim = null;
    led.starts = [{ id: startId, at: now, ok: !!ok, calls: tally.pages, ms: Date.now() - now, pages: (tally.timing || []).slice(0, 8) }].concat(led.starts.filter(x => x.id !== startId)).slice(0, 40);
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
/** One Etsy page. `where` = { k, n, t0 }: which page of how many, and when the pull began (the seconds in a timed-out line count from there). The answer's own timing goes to `tally.timing`
    ({ o: offset, ms, s: HTTP status, or 0 for no answer }: numbers only, no order data), so a slow day shows in the ledger which page was slow. */
async function page(fetchFn, meter, env, token, [offset, limit], tally, deadline, where) {
  const qs = new URLSearchParams({ limit: String(limit), offset: String(offset), sort_on: "created", sort_order: "desc" });   // any status: no filters
  const url = `https://api.etsy.com/v3/application/shops/${env.shop}/receipts?${qs}`;
  const ctl = new AbortController(), timer = setTimeout(() => ctl.abort(), Math.max(1000, Math.min(FETCH_MS, deadline - Date.now()))), mark = meter.bump("sandbox.receiptsPage"), began = Date.now();
  const at = where && where.n > 1 ? ` on page ${where.k} of ${where.n}` : "", secs = () => ((Date.now() - (where && where.t0 || began)) / 1000).toFixed(1);
  const note = status => { (tally.timing = tally.timing || []).push({ o: offset, ms: Date.now() - began, s: status }); };
  tally.pages += 1;
  try {
    let res;
    try { res = await fetchFn(url, { headers: { Authorization: `Bearer ${token}`, "x-api-key": `${env.id}:${env.secret}`, Accept: "application/json" }, signal: ctl.signal }); }
    catch (e) { mark.failNet(); note(0); throw new EtsyError("etsy", `Etsy did not answer (${e && e.name === "AbortError" ? `timed out after ${secs()} s${at}` : "network error"}), so the sandbox is empty. Press Start to try again.`); }
    mark.fromHttp(res.status); note(res.status);
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
    // the headers came, the body did not finish: when it was the clock that cut it, that is a slow answer, not a list that cannot be read
    if (!data && ctl.signal.aborted) throw new EtsyError("etsy", `Etsy did not finish sending its list (timed out after ${secs()} s${at}), so the sandbox is empty. Press Start to try again.`);
    if (!data || !Array.isArray(data.results)) throw new EtsyError("etsy", `Etsy sent a list the server could not read${at}, so the sandbox is empty. Press Start to try again.`);
    return data.results;
  } finally { clearTimeout(timer); }
}
/** The newest `want` receipts (`plan`: their Etsy pages): every page is asked AT ONCE, so the pull needs Etsy to answer once inside `deadline`, not once per round (SBXPULL, 15 Oct 2026). No daily cap
    exists any more (NOLIMIT), so a failing first page no longer has to be the only call made. `sweeping` (the old files being removed) finishes while Etsy answers. */
async function readNewest(deps, tally, now, want, plan, deadline, sweeping) {
  const env = etsyEnv();
  if (!env.shop || !env.id || !env.secret) throw new EtsyError("token", "The server has no Etsy settings, so the sandbox is empty.");
  const due = await tokenDue(now);
  if (!due.seeded) throw new EtsyError("token", "The server's Etsy sign-in is not set up, so the sandbox is empty.");
  let token;
  // the sign-in answers inside the same clock: Etsy's token endpoint that never answers must not hold the pull until the platform cuts the function (ADVPULL, 15 Oct 2026); it gets half of what is left
  const signIn = () => { let timer; const late = new Promise((_, no) => { timer = setTimeout(() => no(Object.assign(new Error("no answer in time"), { late: true })), Math.max(1000, (deadline - Date.now()) / 2)); });
    return Promise.race([Promise.resolve().then(() => deps.token()), late]).finally(() => clearTimeout(timer)); };
  try { token = await signIn(); tally.refreshes = due.due ? 1 : 0; }
  catch (e) {
    tally.refreshes = due.due ? 1 : 0; console.warn("[sandboxPull] Etsy token not available:", (e && e.message || "").slice(0, 200));
    if (e && e.late) throw new EtsyError("etsy", "Etsy's sign-in did not answer in time, so the sandbox is empty. Press Start to try again.");
    throw new EtsyError("token", "The server's Etsy sign-in is not working, so the sandbox is empty.");
  }
  const got = await Promise.allSettled(plan.map((p, i) => page(deps.fetch, deps.meter, env, token, p, tally, deadline, { k: i + 1, n: plan.length, t0: now })));
  const bad = got.filter(r => r.status === "rejected").map(r => r.reason);
  if (bad.length) throw bad.find(e => e instanceof EtsyError && e.reason !== "etsy") || bad[0];   // a sign-in or limit answer says more than a slow page
  await sweeping;   // (the old set is gone before the new one is written: the file below must not be swept away with it)
  // newest first; a receipt that came into the first page while the others were read could show twice: once
  const seen = new Set(), list = [];
  for (const r of got.flatMap(x => x.value)) { const id = r && String(r.receipt_id); if (!id || !/^\d+$/.test(id) || seen.has(id)) continue; seen.add(id); list.push(r); }
  list.sort((a, b) => createdMs(b) - createdMs(a) || Number(b.receipt_id) - Number(a.receipt_id));
  return list.slice(0, want);
}

const defaults = () => ({
  fetch: (...a) => require("node-fetch")(...a),
  token: () => require("./_etsyMailEtsy").getValidEtsyAccessToken(),
  meter: require("./_etsyApiMeter")
});
const safeId = s => (typeof s === "string" && /^[A-Za-z0-9_.:-]{8,80}$/.test(s) ? s : "");

/** The answer for a set that stands. */
function setAnswer(cur, led, extra) {
  return Object.assign({ ok: true, already: false, startId: cur.startId, source: SOURCE, label: cur.label || `${cur.count} newest Etsy orders`, count: cur.count, want: cur.want || WANT, open: cur.open, closed: cur.closed, closedBy: cur.closedBy || {},
    pulledAt: cur.pulledAt, newestAt: cur.newestAt || null, oldestAt: cur.oldestAt || null, calls: 0, tokenRefreshes: 0 }, budget(led), { path: cur.path }, extra || {});
}
async function startStream(ctx, b) {
  if (b.stream === false) return { stream: null };
  try {
    const r = await ctx.stream({ action: "ensure", seed: b.seed, speed: b.speed });
    return r && r.error ? { stream: null, streamError: String(r.error) } : { stream: (r && r.stream) || null };
  } catch (e) { console.warn("[sandboxPull] stream not started:", e && e.message); return { stream: null, streamError: String((e && e.message) || e).slice(0, 200) }; }
}

/** ctx: { stream(b) → the library's sandboxStream, by } · deps: { fetch, token, meter, workMs } (tests replace them; workMs shortens the clock so a slow Etsy can be tested in a second). */
async function pull(b, ctx, deps) {
  b = b || {};
  const startId = safeId(b.startId);
  if (!startId) return fail("bad", "The pull needs a startId of 8 to 80 plain characters, one per sandbox start.", 400);
  deps = Object.assign(defaults(), deps || {});
  const now = Date.now(), day = dayOf(now);
  // how many of the newest orders this pull brings: Paul's setting (one read), 250 when nothing was set or the setting cannot be read
  let lim; try { lim = await readLimit(); } catch (e) { console.warn("[sandboxPull] pull number not read, using", WANT + ":", e && e.message); lim = limitInfo(WANT); }
  const want = lim.limit, plan = planFor(want);
  // the retry marker, the claim and the budget: one transaction
  const gate = await db.runTransaction(async tx => {
    const [ls, cs] = await Promise.all([tx.get(ledgerRef()), tx.get(currentRef())]);
    const led = fresh(ls.exists ? ls.data() : null, day), cur = cs.exists ? cs.data() : null;
    if (cur && cur.source === SOURCE && cur.startId === startId) return { kind: "already", cur, led };
    if (led.claim && now - (+led.claim.at || 0) < CLAIM_MS) return { kind: "busy", led };
    const mine = led.starts.find(x => x.id === startId);
    if (mine && mine.ok) return { kind: "startUsed", led };
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
    const { sweeping } = await discard(true);
    if (gate.kind === "cap") {
      return fail("cap", gate.byPulls ? `Today's ${MAX_PULLS} sandbox pulls from Etsy are used up, so the sandbox stays empty until tomorrow (UTC midnight).`
        : `Today's ${MAX_CALLS} Etsy calls for the sandbox are used up (a pull of ${want} orders needs ${plan.length}), so the sandbox stays empty until tomorrow (UTC midnight).`, 429, Object.assign({ calls: 0, tokenRefreshes: 0, resetAt: new Date(nextDay(now)).toISOString() }, base));
    }
    const list = await readNewest(deps, tally, now, want, plan, now + (deps.workMs > 0 ? deps.workMs : WORK_MS), sweeping);
    if (!list.length) throw new EtsyError("etsy", "Etsy sent no orders, so the sandbox is empty. Press Start to try again.");
    const closedBy = { shipped: 0, canceled: 0, unpaid: 0 };
    let open = 0;
    for (const r of list) { if (isOpen(r)) open += 1; else closedBy[closedWhy(r)] += 1; }
    const pullId = `${now}-${crypto.randomBytes(3).toString("hex")}`, path = `${DIR}${pullId}.json`;
    const body = Buffer.from(JSON.stringify({ at: now, count: list.length, source: SOURCE, startId, receipts: list }));
    const file = admin.storage().bucket().file(path);
    await file.save(body, { resumable: false, contentType: "application/json", metadata: { cacheControl: "no-store" } });
    const doc = { path, source: SOURCE, startId, count: list.length, want, open, closed: list.length - open, closedBy, at: now, pulledAt: now, newestAt: createdMs(list[0]) || null, oldestAt: createdMs(list[list.length - 1]) || null,
      takenBy: String(b.by || "").slice(0, 80) || null, label: `${list.length} newest Etsy orders`, calls: tally.pages, tokenRefreshes: tally.refreshes, bytes: body.length, note: null };
    try { await currentRef().set(Object.assign({}, doc, { updatedAt: FV.serverTimestamp() })); }
    catch (e) { await file.delete({ ignoreNotFound: true }).catch(() => {}); throw e; }
    const [led, , started] = await Promise.all([settle(startId, tally, true, now), Promise.resolve().then(() => deps.meter.flushNow()).catch(() => {}), startStream(ctx, b)]);
    return Object.assign(setAnswer(doc, led, { calls: tally.pages, tokenRefreshes: tally.refreshes, ms: Date.now() - now }), started);
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
      Object.assign({ calls: tally.pages, tokenRefreshes: tally.refreshes, ms: Date.now() - now }, view));
  }
}

module.exports = { pull, readBudget, readLimit, setLimit, clampWant, planFor, pagesFor, isOpen, SOURCE, WANT, MIN_WANT, MAX_WANT, MAX_PULLS, MAX_CALLS, LIMIT_DOC, DIR };
