/*  netlify/functions/firebaseOrders.js  */
const admin = require("./firebaseAdmin");
const db    = admin.firestore();

const COMPLETED_COLL = "Design_Completed Orders";
const REALTIME_COLL  = "Design_RealTime_Selected_Orders";
/* sandbox: ?sandbox=1 keeps every read and write in Sandbox_-prefixed copies of these collections (and of Brites_Orders),
   so a sorter run against the emulated Etsy never touches a real lock, claim, ledger entry or note */
let PREFIX = "";
const col = name => db.collection(PREFIX + name);

/* Released locks and claims stay as tombstones so delta polls see them; nothing reads one a month on. They go here, a page
   at a time and at most once every 10 minutes per instance and workspace (a TTL policy on expireAt, if one is switched on,
   does the same). A lock or claim still held goes only once it is 60 days old: none is held that long. */
const rtSweptAt = new Map();
async function sweepRealtime() {
  if (Date.now() - (rtSweptAt.get(PREFIX) || 0) < 600000) return 0;
  rtSweptAt.set(PREFIX, Date.now());
  try {
    const now = Date.now(), ms = v => (v && typeof v.toMillis === "function" ? v.toMillis() : v instanceof Date ? v.getTime() : Number(v) || 0);
    const snap = await col(REALTIME_COLL).where("at", "<", new Date(now - 30 * 86400000)).limit(200).get();
    const doomed = snap.docs.filter(d => { const v = d.data() || {}; return !(v.selected === true || v.claimed === true) || ms(v.at) < now - 60 * 86400000; });
    if (!doomed.length) return 0;
    const batch = db.batch(); doomed.forEach(d => batch.delete(d.ref)); await batch.commit();
    return doomed.length;
  } catch (e) { console.warn("[firebaseOrders] realtime sweep:", e && e.message); return 0; }
}

/* The stations' timeline door is open (no sign-in): a sender (by IP, per warm instance) may write at most 600 events a
   minute. A whole shop behind one address stays far below it (a scan is 1–3 events); a refused batch stays in the
   station's outbox and is sent again later (order-timeline.js backs off). */
const flood = {
  seen: new Map(), PER_MIN: 600,
  allow(event, n) {
    const h = (event && event.headers) || {};
    const ip = String(h["x-nf-client-connection-ip"] || h["client-ip"] || String(h["x-forwarded-for"] || "").split(",")[0] || "?").trim();
    const now = Date.now(), w = this.seen.get(ip);
    if (!w || now - w.t0 >= 60000) { if (this.seen.size > 5000) this.seen.clear(); this.seen.set(ip, { t0: now, n }); return true; }
    if (w.n + n > this.PER_MIN) return false;
    w.n += n; return true;
  }
};

/* the same open-door guard for station-activity.js, with its own counter (a busy hour of scans must not starve the timeline) */
const activityFlood = { seen: new Map(), PER_MIN: 1500, allow: flood.allow };
/* and for the live layer's keep-alives (station-activity.js working/idle): a station sends about two a minute */
const liveFlood = { seen: new Map(), PER_MIN: 600, allow: flood.allow };

/* The stations' sign-in sessions (station-session.js): one document per session in Station_Sessions, from sign-in to
   sign-out, for one person on one computer at one station or page:
     { id, person, employeeId, station, device, computerId, computerLabel, startAt, lastSeenAt, endAt, endReason, minutes }
   Times are ms. The server stamps them: a start and a beat are "now"; an end may say an earlier moment (an end sent late,
   from a browser that was offline) but never before the last beat the server saw, never after now, and never past the
   New York midnight after the start (everybody is signed out at midnight). A beat after 15 quiet minutes does not bring
   a session back: it ended "closed" at its last beat, and the page starts a new one. A PIN is never kept.
   Auto sign-out (_stationAutoSignout.js, plans/stations-round2/api.md "AD2"): a start, beat or end may carry `lastInputAt` (the page's
   last user input) and `sentAt` (the page's clock, to undo a wrong computer clock); the document keeps `lastInputAt` (server clock,
   never backwards) and `admin` (set when it starts). A non-Admin session whose page reports 10+ minutes without input, or whose page
   went silent for 15 minutes, ends "idle" (or "closing" after 17:00 Toronto) at its LAST INPUT, never when the server noticed; an
   end the page sends with `idle` or `closing` keeps the last input it names (every other reason is raised to the last beat). */
const SESSION_COLL = "Station_Sessions";
const SESSION_REASONS = new Set(["signOut", "midnight", "switched", "closed", "idle", "closing"]);
const SESSION_CLOSED_MS = 15 * 60000;
let nyFmt = null;
try { nyFmt = new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", hourCycle: "h23", year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit" }); } catch (_) {}
function nyParts(t) {
  if (nyFmt) { const o = {}; for (const p of nyFmt.formatToParts(new Date(t))) o[p.type] = p.value; return { y: +o.year, m: +o.month, d: +o.day, h: +o.hour % 24, mi: +o.minute, s: +o.second }; }
  const e = new Date(t - 5 * 3600e3);
  return { y: e.getUTCFullYear(), m: e.getUTCMonth() + 1, d: e.getUTCDate(), h: e.getUTCHours(), mi: e.getUTCMinutes(), s: e.getUTCSeconds() };
}
/** the first New York midnight after t (ms) */
function nyMidnightAfter(t) {
  const off = x => { const p = nyParts(x); return Date.UTC(p.y, p.m - 1, p.d, p.h, p.mi, p.s) - Math.floor(x / 1000) * 1000; };
  const p = nyParts(t), wall = Date.UTC(p.y, p.m - 1, p.d + 1);
  let u = wall - off(t); u = wall - off(u);
  return u > t ? u : t + 86400e3;
}
async function sessionWrite(s) {
  const str = (v, n) => String(v == null ? "" : v).replace(/[\u0000-\u001f\u007f]/g, " ").replace(/\s+/g, " ").trim().slice(0, n);
  const { STATIONS } = require("./_orderTimeline");
  const id = typeof s.id === "string" && /^[\w.:-]{8,100}$/.test(s.id) ? s.id : "";
  const ev = s.event === "start" || s.event === "beat" || s.event === "end" ? s.event : "";
  const station = typeof s.station === "string" && STATIONS.has(s.station) ? s.station : "";
  const computerId = typeof s.computerId === "string" && /^[\w-]{6,64}$/.test(s.computerId) ? s.computerId : "";
  let person = str(s.person, 200);
  if ((person.match(/\p{Nd}/gu) || []).length >= 4) person = person.replace(/\p{Nd}+/gu, " ").replace(/\s+/g, " ").trim();   // a name never carries a PIN: "Paul 482915" is "Paul"
  person = person.slice(0, 80);
  if (!id || !ev || !station || !computerId || (ev !== "end" && !/\p{L}/u.test(person))) return [400, { error: "not a session event" }];       // (no letter, "123456" or "12 34 56": a PIN, never a name)
  const eid = str(s.employeeId, 60), employeeId = /^\d+$/.test(eid) ? "" : eid;   // a PIN is only digits: never kept
  const device = str(s.device, 40).replace(/[^\w .:-]/g, "") || station;
  const computerLabel = str(s.computerLabel, 80);
  const task = station === "welding" && (s.task === "welding" || s.task === "matching") ? s.task : "";   // the Welding station's task (welding | matching); any other value, or another station: none, as on every old session
  const role = (station === "laser" || station === "design") && s.role === station ? s.role : "";   // the Sorter app's Laser or Design person (LD1): the role IS the station there; any other value, or another station: none, as on every old session
  const reason = SESSION_REASONS.has(s.reason) ? s.reason : "signOut";
  const clientAt = Number(s.at);
  const ref = col(SESSION_COLL).doc(id);
  const AS = require("./_stationAutoSignout"), Admins = require("./_stationAdmins");
  // who is an Admin (config/stationAdmins, kept 60 s; never throws): read before the transaction, only for a start or a beat
  const list = ev === "end" ? null : await Admins.load(db);
  return db.runTransaction(async tx => {
    const now = Date.now();
    const snap = await tx.get(ref), prev = snap.exists ? (snap.data() || {}) : null;
    if (prev && prev.computerId && prev.computerId !== computerId) return [409, { error: "not this computer's session" }];
    if (prev && prev.endAt != null) return [200, { success: true, id, ended: true, endReason: prev.endReason || null }];
    if (!prev && ev === "end") return [200, { success: true, id, missing: true }];
    const startAt = prev ? (Number(prev.startAt) || now) : now;
    const lastSeen = prev ? (Number(prev.lastSeenAt) || startAt) : now;
    const cap = nyMidnightAfter(startAt);
    // the page's last input, on the server's clock (undoing a wrong computer clock), never backwards, between the start and now
    const skew = AS.skewOf(s.sentAt != null ? s.sentAt : ev === "end" ? null : s.at, now);   // (a start or a beat's `at` is the moment it was sent; an end's `at` is the end)
    const reported = AS.pageTime(s.lastInputAt, skew, now), prevInput = prev ? (Number(prev.lastInputAt) || 0) : 0;
    const lastInput = Math.max(reported, prevInput) > 0 ? Math.min(Math.max(reported, prevInput, startAt), now) : 0;
    const adm = AS.adminState({ admin: prev ? prev.admin : undefined, person: prev && prev.person ? prev.person : person }, list);   // true | false | null (unknown)
    let endAt = null, endReason = null, lastSeenAt = now;
    if (ev === "end") {
      if (reason === "idle" || reason === "closing") {          // the page names its LAST INPUT as the end: it may be before the last beat, never before an input already known
        const at = AS.pageTime(clientAt, skew, now);
        endAt = Math.min(now, Math.max(startAt, lastInput || 0, at || lastInput || lastSeen)); endReason = reason;
      } else {
        const want = Number.isFinite(clientAt) && clientAt > 0 ? Math.max(lastSeen, clientAt) : now;
        endAt = Math.min(now, want); endReason = reason;
      }
      if (endAt > cap) { endAt = cap; endReason = "midnight"; }
      lastSeenAt = Math.max(lastSeen, endAt);
    } else if (prev && now - lastSeen >= SESSION_CLOSED_MS) {
      // a page silent for 15 minutes does not come back: a non-Admin with a known last input ends "idle" at it, the rest "closed" at the last beat (the old rule)
      const d = AS.decide({ startAt, lastSeenAt: lastSeen, lastInputAt: prev.lastInputAt }, now, adm === null ? true : adm);
      if (d) { endAt = d.endAt; endReason = d.endReason; } else { endAt = Math.min(lastSeen, cap); endReason = lastSeen > cap ? "midnight" : "closed"; }
      lastSeenAt = lastSeen;
    } else if (now > cap) {
      endAt = cap; endReason = "midnight"; lastSeenAt = Math.max(lastSeen, cap);
    } else if (prev && reported > 0 && adm !== null) {
      // a live beat that says the person has had no input for 10 minutes: the page should have signed out; the end is the last input
      const d = AS.decide({ startAt, lastSeenAt: now, lastInputAt: lastInput }, now, adm);
      if (d) { endAt = d.endAt; endReason = d.endReason; }
    }
    const minutes = Math.max(0, Math.round(((endAt != null ? endAt : lastSeenAt) - startAt) / 6000) / 10);
    const doc = prev
      ? { lastSeenAt, endAt, endReason, minutes }
      : Object.assign({ id, person, employeeId, station, device, computerId, computerLabel, startAt, lastSeenAt, endAt, endReason, minutes }, task ? { task } : {}, role ? { role } : {}, list && list.ok ? { admin: list.keys.has(Admins.keyOf(person)) } : {});
    if (lastInput > 0) doc.lastInputAt = lastInput;
    if (prev && computerLabel && computerLabel !== prev.computerLabel) doc.computerLabel = computerLabel;
    tx.set(ref, doc, { merge: true });
    return [200, { success: true, id, startAt, lastSeenAt, endAt, endReason, minutes, ended: endAt != null }];
  });
}

/* Global CORS headers */
const CORS = {
  "Access-Control-Allow-Origin" : "*",
  "Access-Control-Allow-Headers": "Content-Type,Authorization",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS"
};

exports.handler = async (event) => {
  /* Pre-flight */
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers: CORS, body: "ok" };
  }

  try {
    const method = event.httpMethod;
    PREFIX = (event.queryStringParameters && event.queryStringParameters.sandbox === "1") || (event.headers && (event.headers["x-sandbox"] === "1" || event.headers["X-Sandbox"] === "1")) ? "Sandbox_" : "";

    /* ───────────────────────── POST ───────────────────────── */
    if (method === "POST") {
      let body;
      try { body = JSON.parse(event.body || "{}"); }
      catch (e) {
        // a body that is not JSON and mentions a login never reaches the log: the parser's own message can quote it
        if (/pinLogin/.test(String(event.body || ""))) return { statusCode: 400, headers: Object.assign({ "Cache-Control": "no-store" }, CORS), body: JSON.stringify({ ok: false, error: "an Employee Number is 6 digits" }) };
        return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: "not JSON" }) };   // (ST2: a body that is not JSON is the sender's mistake, a 4xx; the parser's own message can quote the body, so it is never shown or logged)
      }
      if (body === null) return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: "not an object" }) };   // (ST2: "null" is valid JSON and not a request)
      /* the stations' Employee Number login (_stationPinLogin.js): the page sends the number typed and gets back that person's
         name, or no. The roster document itself is never returned. The number is read from the body only, never from a URL,
         is never logged, and a guesser is slowed and locked out for a minute. An answer carries `ok`; anything else is a failure. */
      if (body && typeof body === "object" && body.pinLogin !== undefined) {
        const out = await require("./_stationPinLogin").pinLogin(db, event, body.pinLogin);
        return { statusCode: out.statusCode, headers: Object.assign({}, CORS, out.headers), body: JSON.stringify(out.body) };
      }
      /* "is this name an Admin?" (_stationAdmins.js): a page asks about ONE name once per sign-in and gets { ok, admin } and nothing
         else (never the list, never a PIN). Read-only; rate limited like the PIN door with counters of its own. Admins are exempt from
         the auto sign-out after 10 minutes without input and at 5:00 pm Toronto time; midnight still ends everybody. */
      if (body && typeof body === "object" && body.stationAdmin !== undefined) {
        const out = await require("./_stationAdmins").door(db, event, body.stationAdmin);
        return { statusCode: out.statusCode, headers: Object.assign({}, CORS, out.headers), body: JSON.stringify(out.body) };
      }
      /* the production stations' own events on an order's timeline (_orderTimeline.js): scans and what was done with
         them, by whom, where. Only station event types pass this open door. */
      if (Array.isArray(body.timeline)) {
        if (body.timeline.length > 100) return { statusCode: 413, headers: CORS, body: JSON.stringify({ error: "at most 100 events a request" }) };
        // an event keeps the store it was recorded in: one marked sandbox never lands in production (nor the reverse)
        // (an event that says which store it belongs to must match the door it came through; one that does not say follows the door)
        const events = body.timeline.filter(e => e && typeof e === "object" && (typeof e.sandbox !== "boolean" || e.sandbox === !!PREFIX));
        if (!flood.allow(event, events.length)) return { statusCode: 429, headers: CORS, body: JSON.stringify({ error: "too many timeline events, try again in a minute" }) };
        const out = await require("./_orderTimeline").add(db, admin.firestore.FieldValue, events, { prefix: PREFIX, source: "station", stationOnly: true });
        return { statusCode: 200, headers: CORS, body: JSON.stringify(Object.assign({ success: true }, out, events.length < body.timeline.length ? { refused: body.timeline.length - events.length } : {})) };
      }
      /* a station's sign-in session (station-session.js): start, beat or end; small, validated, the server's times */
      if (body.session && typeof body.session === "object" && !Array.isArray(body.session)) {
        if (String(event.body || "").length > 4096) return { statusCode: 413, headers: CORS, body: JSON.stringify({ error: "session event too large" }) };
        if (!flood.allow(event, 1)) return { statusCode: 429, headers: CORS, body: JSON.stringify({ error: "too many requests, try again in a minute" }) };
        const [statusCode, out] = await sessionWrite(body.session);
        return { statusCode, headers: CORS, body: JSON.stringify(out) };
      }
      /* what a person did at a station (station-activity.js): a batch of small events, each created once, with the person's
         daily rollup counted in the same transaction (_stationActivity.js). Write-only: the console reads through its own gate. */
      if (Array.isArray(body.activity)) {
        const A = require("./_stationActivity");
        if (String(event.body || "").length > A.MAX_BODY_CHARS || body.activity.length > A.MAX_BATCH) return { statusCode: 413, headers: CORS, body: JSON.stringify({ error: `at most ${A.MAX_BATCH} events and ${A.MAX_BODY_CHARS} characters a request` }) };
        if (!activityFlood.allow(event, body.activity.length)) return { statusCode: 429, headers: CORS, body: JSON.stringify({ error: "too many activity events, try again in a minute" }) };
        const out = await A.add(db, admin.firestore.FieldValue, body.activity, { prefix: PREFIX });
        return { statusCode: 200, headers: CORS, body: JSON.stringify(Object.assign({ success: true }, out)) };
      }
      /* what a station is working on right now (station-activity.js working / idle): one small document per station, page and
         person, overwritten, shown to the console while its keep-alive lasts (_stationLive.js). Write-only: the console reads it through its own gate. */
      if (body.live && typeof body.live === "object" && !Array.isArray(body.live)) {
        const L = require("./_stationLive");
        if (String(event.body || "").length > L.MAX_BODY_CHARS) return { statusCode: 413, headers: CORS, body: JSON.stringify({ error: "live event too large" }) };
        if (!liveFlood.allow(event, 1)) return { statusCode: 429, headers: CORS, body: JSON.stringify({ error: "too many live events, try again in a minute" }) };
        const [statusCode, out] = await L.write(db, admin.firestore.FieldValue, body.live, { prefix: PREFIX });
        return { statusCode, headers: CORS, body: JSON.stringify(out) };
      }
      const {
        orderNumber,
        orderNumField,
        clientName,
        britesMessages,
        shippingLabelTimestamps,
        employeeName,
        newMessage,
        designSetId,
        staffNote,
        /* design completion controls */
        designCompleted,
        completedIds,   // array of receipt IDs to mark completed
        uncompleteIds   // array of receipt IDs to unset
      } = body;

      /* A released claim or lock stays as a tombstone so delta polls see it, then goes: it carries expireAt (a Firestore
         TTL policy on that field deletes it 30 days on). A document still claimed or still selected never carries it, so
         a live claim or lock is never removed. */
      const TOMBSTONE_DAYS = 30;
      const expiries = async (ids, stillHeld) => {
        const snaps = await db.getAll(...ids.map((id) => col(REALTIME_COLL).doc(id)));
        return snaps.map((s) => (s.exists && stillHeld(s.data() || {}) ? admin.firestore.FieldValue.delete() : new Date(Date.now() + TOMBSTONE_DAYS * 86400000)));
      };

      /* ─── Sorter claims: a gold dot ("in a sorter run"), never a lock. The other bench keeps notes and chat. ─── */
      const { rtClaimIds, rtUnclaimIds, claimedBy, claimRun } = body;
      if (Array.isArray(rtClaimIds) && rtClaimIds.length) {
        const batch = db.batch();
        rtClaimIds.map(String).slice(0, 500).forEach((id) => {
          batch.set(col(REALTIME_COLL).doc(id), { claimed: true, claimedBy: String(claimedBy || "sorter"), claimRun: claimRun ? String(claimRun) : null, claimAt: admin.firestore.FieldValue.serverTimestamp(), at: admin.firestore.FieldValue.serverTimestamp(), expireAt: admin.firestore.FieldValue.delete() }, { merge: true });
        });
        await batch.commit();
        return { statusCode: 200, headers: CORS, body: JSON.stringify({ success: true, message: "Claimed", count: rtClaimIds.length }) };
      }
      if (Array.isArray(rtUnclaimIds) && rtUnclaimIds.length) {
        const ids = rtUnclaimIds.map(String).slice(0, 500), expireAt = await expiries(ids, (d) => d.selected === true);
        const batch = db.batch();
        ids.forEach((id, i) => {
          batch.set(col(REALTIME_COLL).doc(id), { claimed: false, claimedBy: null, claimRun: null, at: admin.firestore.FieldValue.serverTimestamp(), expireAt: expireAt[i] }, { merge: true });
        });
        await batch.commit();
        await sweepRealtime();
        return { statusCode: 200, headers: CORS, body: JSON.stringify({ success: true, message: "Unclaimed", count: rtUnclaimIds.length }) };
      }

      /* ─── Realtime selection locks (write via server) ───
         The station sends the orders it selected and those it let go in one request: both are written (the unlocks used
         to be dropped whenever locks came with them, so a let-go order stayed "being worked on at another station"). */
      const { rtLockIds, rtUnlockIds, clientId, page } = body;
      const lockIds = Array.isArray(rtLockIds) ? [...new Set(rtLockIds.map(String))] : [];
      const unlockIds = Array.isArray(rtUnlockIds) ? [...new Set(rtUnlockIds.map(String))].filter((id) => !lockIds.includes(id)) : [];
      if (lockIds.length || unlockIds.length) {
        /* 🔓 De-select → write a tombstone so delta polls see it */
        const expireAt = unlockIds.length ? await expiries(unlockIds, (d) => d.claimed === true) : [];
        const writes = [
          ...lockIds.map((id) => [id, {
            selected   : true,
            selectedBy : clientId || "server",
            page       : page || "design",
            at         : admin.firestore.FieldValue.serverTimestamp(),
            expireAt   : admin.firestore.FieldValue.delete()
          }]),
          ...unlockIds.map((id, i) => [id, {
            selected   : false,
            selectedBy : null,
            page       : null,
            at         : admin.firestore.FieldValue.serverTimestamp(),
            expireAt   : expireAt[i]
          }])
        ];
        for (let i = 0; i < writes.length; i += 450) {
          const batch = db.batch();
          writes.slice(i, i + 450).forEach(([id, v]) => batch.set(col(REALTIME_COLL).doc(id), v, { merge: true }));
          await batch.commit();
        }
        if (unlockIds.length) await sweepRealtime();
        return {
          statusCode: 200,
          headers: CORS,
          body: JSON.stringify({
            success: true,
            message: lockIds.length && unlockIds.length ? "Locked and unlocked" : lockIds.length ? "Locked" : "Unlocked (tombstone)",
            count: lockIds.length + unlockIds.length,
            locked: lockIds.length,
            unlocked: unlockIds.length
          })
        };
      }

      // If nothing actionable, short-circuit
      if (
        !orderNumber &&
        !Array.isArray(completedIds) &&
        !Array.isArray(uncompleteIds) &&
        !(typeof designCompleted === "boolean")
      ) {
        return {
          statusCode: 400,
          headers: CORS,
          body: JSON.stringify({ error: "No actionable fields provided" })
        };
      }

      /* 0) Bulk set completed → Design_Completed Orders */
      if (Array.isArray(completedIds) && completedIds.length) {
        const batch = db.batch();
        completedIds.forEach((id) => {
          const ref = col(COMPLETED_COLL).doc(String(id));
          batch.set(
            ref,
            {
              orderId     : String(id),
              completed   : true,
              completedAt : admin.firestore.FieldValue.serverTimestamp()
            },
            { merge: true }
          );
        });
        await batch.commit();
        return {
          statusCode: 200,
          headers: CORS,
          body: JSON.stringify({
            success: true,
            message: "Marked completed (bulk)",
            count: completedIds.length
          })
        };
      }

      /* 0b) Bulk UN-set completed → delete from Design_Completed Orders */
      if (Array.isArray(uncompleteIds) && uncompleteIds.length) {
        const batch = db.batch();
        uncompleteIds.forEach((id) => {
          const ref = col(COMPLETED_COLL).doc(String(id));
          batch.delete(ref);
        });
        await batch.commit();
        return {
          statusCode: 200,
          headers: CORS,
          body: JSON.stringify({
            success: true,
            message: "Unmarked completed (bulk)",
            count: uncompleteIds.length
          })
        };
      }

      /* 1) Live-chat messages */
      if (typeof newMessage === "string" && newMessage.trim() !== "") {
        if (!orderNumber) {
          return {
            statusCode: 400,
            headers: CORS,
            body: JSON.stringify({ error: "orderNumber required for messages" })
          };
        }
        if (designSetId !== undefined && (typeof designSetId !== "string" || !designSetId.trim() || designSetId.length > 80 || newMessage.trim() !== "DESIGNED :)")) {
          return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: "Invalid design completion message" }) };
        }
        const messages = db
          .collection(PREFIX + "Brites_Orders")
          .doc(String(orderNumber))
          .collection("messages");
        const message = {
            text       : newMessage.trim(),
            senderName : employeeName || "Staff",
            senderRole : "staff",
            timestamp  : admin.firestore.FieldValue.serverTimestamp()
          };
        // an image the sender uploaded first (the stations write the same field themselves)
        if (typeof body.imageUrl === "string" && /^https:\/\/[^\s"<>]{8,1900}$/.test(body.imageUrl)) message.imageUrl = body.imageUrl;
        // One internal completion message per order and set, even after a lost
        // response, a station reload or simultaneous retries from two stations.
        // A message sent from a browser's outbox carries its own id, so a send retried after a reload, a lost answer or a
        // dropped connection is written once.
        const clientId = typeof body.clientMessageId === "string" && /^[\w-]{8,80}$/.test(body.clientMessageId) ? body.clientMessageId : null;
        const messageId = designSetId !== undefined ? "designed-set-" + encodeURIComponent(designSetId) : clientId ? "c-" + clientId : null;
        if (messageId) {
          const ref = messages.doc(messageId);
          await db.runTransaction(async tx => {
            const prior = await tx.get(ref);
            if (!prior.exists) tx.set(ref, designSetId !== undefined ? { ...message, setId: designSetId } : message);
          });
        } else await messages.add(message);

        return {
          statusCode: 200,
          headers: CORS,
          body: JSON.stringify({ success: true, message: "Chat doc added.", ...(messageId ? { messageId } : {}) })
        };
      }

      /* 2) Merge order-level fields on Brites_Orders (optional dual-write flag) */
      const dataToStore = {};
      if (orderNumField           !== undefined) dataToStore["Order Number"]              = orderNumField;
      if (clientName              !== undefined) dataToStore["Client Name"]               = clientName;
      if (britesMessages          !== undefined) dataToStore["Brites Messages"]           = britesMessages;
      if (shippingLabelTimestamps !== undefined) dataToStore["Shipping Label Timestamps"] = shippingLabelTimestamps;
      if (employeeName            !== undefined) dataToStore["Employee Name"]             = employeeName;
      if (staffNote               !== undefined) dataToStore["Staff Note"]                = staffNote;
      if (typeof designCompleted  === "boolean") {
        dataToStore["Design Completed"] = !!designCompleted;
        if (designCompleted) {
          dataToStore["Design Completed At"] = admin.firestore.FieldValue.serverTimestamp();
        }
      }

      if (Object.keys(dataToStore).length === 0) {
        return {
          statusCode: 200,
          headers: CORS,
          body: JSON.stringify({ success: true, message: "Nothing to update." })
        };
      }

      if (!orderNumber) {
        return {
          statusCode: 400,
          headers: CORS,
          body: JSON.stringify({ error: "orderNumber required for order updates" })
        };
      }

      await db
        .collection(PREFIX + "Brites_Orders")
        .doc(String(orderNumber))
        .set(dataToStore, { merge: true });

      return {
        statusCode: 200,
        headers: CORS,
        body: JSON.stringify({
          success: true,
          message: `Order doc ${String(orderNumber)} created/updated.`
        })
      };
    }

    /* ───────────────────────── GET ───────────────────────── */

    if (method === "GET") {
      // helper: parse "a,b,c" → ["a","b","c"]
      const parseIds = (s) =>
        String(s || "")
          .split(",")
          .map(x => x.trim())
          .filter(Boolean);

      /* ?cancelCheck=rid1,rid2 → which of these orders are cancelled (by Etsy or by a person), for a station's scan */
      if (event.queryStringParameters?.cancelCheck) {
        const out = await require("./_orderTimeline").cancelCheck(db, event.queryStringParameters.cancelCheck, { prefix: PREFIX });
        return { statusCode: 200, headers: CORS, body: JSON.stringify(Object.assign({ success: true }, out)) };
      }
      /* ?messagesFor=rid1,rid2[&limit=80][&since=ms] → the internal thread of each order, oldest first. The real thread is
         always read (only read), so a sandbox run shows what the stations wrote on the real order; the sandbox's own
         messages come from its copy and carry sandbox: true. */
      if (event.queryStringParameters?.messagesFor) {
        const q = event.queryStringParameters;
        const ids = [...new Set(parseIds(q.messagesFor))].filter(id => /^[\w-]{1,40}$/.test(id)).slice(0, 40);
        const limit = Math.max(1, Math.min(200, Number(q.limit) || 80));
        const since = Number(q.since) || 0;
        const ms = t => (t && typeof t.toMillis === "function" ? t.toMillis() : t && t.seconds ? t.seconds * 1000 : null);
        const read = async (coll, id, sandbox) => {
          let ref = db.collection(coll).doc(id).collection("messages");
          if (since) ref = ref.where("timestamp", ">", admin.firestore.Timestamp.fromMillis(since));
          const snap = await ref.orderBy("timestamp", "desc").limit(limit).get();
          return snap.docs.map(d => { const m = d.data() || {}; return Object.assign({ id: d.id, senderName: String(m.senderName || "Staff"), senderRole: String(m.senderRole || "staff"), text: String(m.text || ""), imageUrl: m.imageUrl || null, at: ms(m.timestamp) }, sandbox ? { sandbox: true } : {}); });
        };
        const byOrder = {}; let next = 0;
        const worker = async () => {
          while (next < ids.length) {
            const id = ids[next++];
            const parts = await Promise.all([read("Brites_Orders", id, false)].concat(PREFIX ? [read(PREFIX + "Brites_Orders", id, true)] : []));
            byOrder[id] = [].concat(...parts).sort((a, b) => (a.at || 0) - (b.at || 0)).slice(-limit);
          }
        };
        await Promise.all(Array.from({ length: Math.min(5, ids.length) }, worker));
        return { statusCode: 200, headers: CORS, body: JSON.stringify({ success: true, byOrder, now: Date.now() }) };
      }

      /* ?dcFor=rid1,rid2 → return subset that exist in Design_Completed Orders */
      if (event.queryStringParameters?.dcFor) {
        const ids = parseIds(event.queryStringParameters.dcFor);
        if (!ids.length) {
          return { statusCode: 200, headers: CORS, body: JSON.stringify({ success:true, orderNumbers: [] }) };
        }
        // Query matching completed records, rather than billing a document get
        // for every not-yet-completed order on every background poll. The station
        // now asks about every open order on each sweep (100 ids a request), so the
        // ten-id queries run five at a time rather than one after another; the
        // answer keeps the order of the ids asked about.
        const groups=[];
        for(let i=0;i<ids.length;i+=10)groups.push(ids.slice(i,i+10));
        const found=new Array(groups.length);let next=0;
        const worker=async()=>{while(next<groups.length){const g=next++;const snap=await col(COMPLETED_COLL).where(admin.firestore.FieldPath.documentId(),"in",groups[g]).get();found[g]=snap.docs.map(d=>d.id);}};
        await Promise.all(Array.from({length:Math.min(5,groups.length)},worker));
        const present=[].concat(...found);
        return {
          statusCode: 200, headers: CORS,
          body: JSON.stringify({ success:true, orderNumbers: present, now: Date.now() })
        };
      }

      /* ?staffNotesFor=rid1,rid2 → which of these Brites_Orders have a non-empty "Staff Note" */
      if (event.queryStringParameters?.staffNotesFor) {
        const ids = parseIds(event.queryStringParameters.staffNotesFor);
        if (!ids.length) {
          return { statusCode: 200, headers: CORS, body: JSON.stringify({ success:true, orderNumbers: [] }) };
        }
        // Every sweep of the station now asks about all of its open orders (100 ids a
        // request): ten-id queries, five at a time, return only the orders that have a
        // record at all, instead of a billed document get per order.
        const uniq = [...new Set(ids.map(String))], groups = [];
        for (let i = 0; i < uniq.length; i += 10) groups.push(uniq.slice(i, i + 10));
        const noted = new Set(); let next = 0;
        const worker = async () => {
          while (next < groups.length) {
            const g = groups[next++];
            const snap = await db.collection(PREFIX + "Brites_Orders").where(admin.firestore.FieldPath.documentId(), "in", g).get();
            snap.docs.forEach(d => { const note = ((d.data() || {})["Staff Note"] ?? "").toString().trim(); if (note) noted.add(d.id); });
          }
        };
        await Promise.all(Array.from({ length: Math.min(5, groups.length) }, worker));
        const withNotes = ids.filter(id => noted.has(String(id)));
        return {
          statusCode: 200, headers: CORS,
          body: JSON.stringify({ success:true, orderNumbers: withNotes, now: Date.now() })
        };
      }

      /* ?rtFor=rid1,rid2 → lock state only for these ids in REALTIME_COLL */
      if (event.queryStringParameters?.rtFor) {
        const ids = parseIds(event.queryStringParameters.rtFor);
        if (!ids.length) {
          return { statusCode: 200, headers: CORS, body: JSON.stringify({ success:true, locks: {} }) };
        }
        const refs = ids.map(id => col(REALTIME_COLL).doc(String(id)));
        const snaps = await Promise.all(refs.map(r => r.get()));
        const locks = {}, claims = {};
        snaps.forEach((snap, i) => {
          if (!snap.exists) return;
          const v = snap.data() || {};
          if (v.selected === true) locks[ids[i]] = v;
          if (v.claimed === true) claims[ids[i]] = { claimedBy: v.claimedBy || "sorter", run: v.claimRun || null, atMs: (v.claimAt?.toMillis?.() || 0) };
        });
        return {
          statusCode: 200, headers: CORS,
          body: JSON.stringify({ success:true, locks, claims, now: Date.now() })
        };
      }
      
      /* ?rtSince=NUMBER(ms) → delta since watermark (server-accurate boundary)
         Returns: { locks:{id:{selectedBy,page,atMs}}, unlocks:[id], now:Number } */
      const qSince = event.queryStringParameters?.rtSince;
      if (qSince) {
        const sinceMs = Number(qSince);
        if (!Number.isFinite(sinceMs)) {
          return { statusCode: 400, headers: CORS, body: JSON.stringify({ error:"bad rtSince" }) };
        }
        const sinceTs = admin.firestore.Timestamp.fromMillis(sinceMs);
        const snap = await col(REALTIME_COLL)
          .where("at", ">=", sinceTs)
          .get();

        const locks   = {};
        const unlocks = [];
        const claims  = {};
        const unclaims = [];
        snap.forEach(d=>{
          const v = d.data() || {};
          if (v.selected === true) {
            locks[d.id] = {
              selectedBy: v.selectedBy || null,
              page     : v.page || null,
              atMs     : (v.at?.toMillis?.() || Date.now())
            };
          } else if (v.selected === false) {
            unlocks.push(d.id);
          }
          if (v.claimed === true) claims[d.id] = { claimedBy: v.claimedBy || "sorter", run: v.claimRun || null, atMs: (v.claimAt?.toMillis?.() || Date.now()) };
          else if (v.claimed === false) unclaims.push(d.id);
        });

        return {
          statusCode: 200,
          headers: CORS,
          body: JSON.stringify({ success:true, locks, unlocks, claims, unclaims, now: Date.now() })
        };
      }

      /* ?rt=1 → current active locks only (selected == true) */
      if (event.queryStringParameters?.rt === "1") {
        const snap = await col(REALTIME_COLL).where("selected","==",true).get();
        const locks = {};
        snap.forEach(d => { locks[d.id] = d.data(); });
        const csnap = await col(REALTIME_COLL).where("claimed","==",true).get();
        const claims = {};
        csnap.forEach(d => { const v = d.data() || {}; claims[d.id] = { claimedBy: v.claimedBy || "sorter", run: v.claimRun || null, atMs: (v.claimAt?.toMillis?.() || 0) }; });
        return {
          statusCode: 200,
          headers: CORS,
          body: JSON.stringify({ success: true, locks, claims })
        };
      }

       /* ?dcSince=NUMBER(ms) → completed IDs changed since watermark */
     if (event.queryStringParameters?.dcSince) {
       const sinceMs = Number(event.queryStringParameters.dcSince);
       if (!Number.isFinite(sinceMs)) {
         return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: "bad dcSince" }) };
       }
       const sinceTs = admin.firestore.Timestamp.fromMillis(sinceMs);
       const snap = await col(COMPLETED_COLL)
         .where("completedAt", ">=", sinceTs)
         .select()
         .get();
       return {
         statusCode: 200,
         headers: CORS,
         body: JSON.stringify({
           success: true,
           orderNumbers: snap.docs.map(d => d.id),
           now: Date.now()
         })
       };
     }

      /* ?designCompleted=1 → list of completed receipt IDs from Design_Completed Orders */
      if (event.queryStringParameters?.designCompleted === "1") {
        const snap = await col(COMPLETED_COLL).select().get();
        return {
          statusCode: 200,
          headers: CORS,
          body: JSON.stringify({
            success      : true,
            orderNumbers : snap.docs.map((d) => d.id)
          })
        };
      }

      /* ?staffNotes=1 → array of order IDs with a Staff Note in Brites_Orders */
      if (event.queryStringParameters?.staffNotes === "1") {
        const snap = await db
          .collection(PREFIX + "Brites_Orders")
          .where("Staff Note", "!=", "")
          .select()
          .get();
        return {
          statusCode: 200,
          headers: CORS,
          body: JSON.stringify({
            success      : true,
            orderNumbers : snap.docs.map((d) => d.id)
          })
        };
      }

      /* Single-order fetch (legacy path) */
      const { orderId } = event.queryStringParameters || {};
      if (!orderId) {
        return {
          statusCode: 400,
          headers: CORS,
          body: JSON.stringify({ success: false, msg: "orderId required" })
        };
      }

      /* The stations' Employee Number list (Brites_Orders/"Employee Numbers": every number next to its name) is not served to a
         page any more: a login asks the server ({ pinLogin }, _stationPinLogin.js) and is given one name. This old read answers
         only a request carrying the manager passcode (X-Manager-Key); any other is refused before anything is read. It holds
         for the sandbox copy as well. */
      if (require("./_stationPinLogin").isRosterId(orderId)) {
        const refused = await require("./_stationPinLogin").rosterGate(db, event);
        if (refused) return { statusCode: refused.statusCode, headers: Object.assign({}, CORS, refused.headers), body: JSON.stringify(refused.body) };
      }

      const docSnap = await db.collection(PREFIX + "Brites_Orders").doc(String(orderId)).get();

      if (!docSnap.exists) {
        return {
          statusCode: 200,
          headers: CORS,
          body: JSON.stringify({ success: false, notFound: true })
        };
      }

      return {
        statusCode: 200,
        headers: CORS,
        body: JSON.stringify({ success: true, data: docSnap.data() })
      };
    }

    /* Fallback */
    return {
      statusCode: 405,
      headers: CORS,
      body: JSON.stringify({ error: "Method Not Allowed" })
    };
  } catch (error) {
    console.error("Error in firebaseOrders function:", error);
    return {
      statusCode: 500,
      headers: CORS,
      body: JSON.stringify({ error: error.message })
    };
  }
};
