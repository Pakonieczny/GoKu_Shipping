/* _charmNestPush.js — LIVE PUSH, the server's half (Paul, 28 Oct 2026: "I want a change made on one computer to appear on the others at once, with no idle
   polling"; plan: the local-first design, section 5.5 (a), phase 5).

   A computer's leader tab listens, read-only, to ONE tiny Firestore document through the web SDK (charm-nest-push.js), and a change in it sends it to ask the
   changes feed (charmNestLibrary.js op getChanges) at once. This is the document's other end:
       Charm_Nest_Push/counter           { n, at }    the real world
       Sandbox_Charm_Nest_Push/counter   { n, at }    the sandbox (a family: charm-nest-sandbox-families.js, its reset wipes it)
   n is a count that only goes up (FieldValue.increment) and at the server's time; NOTHING else is ever written there: no order id, no piece id, no list of names
   (the names stay behind charmNestLibrary: the feed is asked for them). Paul's published rule lets a browser read exactly this document and write nothing:
       match /Charm_Nest_Push/{docId} { allow read: if docId == 'counter'; allow write: if false; }   (and Sandbox_Charm_Nest_Push the same)
   The functions write it with the Admin SDK, which the rules do not bind.

   WHEN: after every bumpRev (charmNestLibrary.js: one per write that moved any feed shard, and the seal pass), and after a Team message raised an order's
   Order_Rev (_orderTimeline.js touchNow: that is what an open order window watches). AFTER the shards, never beside them: a computer that hears the counter
   move asks the feed at once and must find the names there.

   THE ONE-WRITE-A-SECOND LIMIT (Firestore sustains about one write a second on one document; a busy shop hour writes more than that, and every write is a
   nudge). A serverless instance freezes the moment it has answered, so nothing may be left to a timer after the response. So:
     · ONE write in flight per world and instance, and a nudge that comes while it is in the air does not start another: it shares the NEXT one (a dirty flag
       made of a promise). The next one begins only after the one in flight is done, so it carries every nudge that came meanwhile: the last nudge of a burst is
       written by the request that made it (it awaits the write that covers it, inside the request), never dropped and never left to a later request;
     · at least GAP_MS between the starts of two writes (a stream of requests one after another on one instance is held to 4 a second, each waiting out the rest
       of the gap, which is awaited in the request as well);
     · best effort, like the feed's shards (feedBump): it never throws, a failed write (or one that takes longer than TRY_MS) is tried once more after a short random
       wait and then given up (the next write's nudge, or the page's ten-minute check, covers it), and no request waits for it longer than WAIT_MS.
   A write that is slow or fails costs the people on other computers a later look, never the person's own write.

   nudge(db, FV, prefix) → Promise<boolean>   prefix "" or "Sandbox_"; true when the write that covers this nudge landed
   stats                                      { asked, writes, shared, retried, failed, slow }: the contention test reads them (tests/perf/server-scale.cjs) */
"use strict";
const COLL = "Charm_Nest_Push", DOC = "counter";
const GAP_MS = 250, WAIT_MS = 2500, TRY_MS = 1500, RETRY_MIN_MS = 40, RETRY_SPREAD_MS = 160;
const stats = { asked: 0, writes: 0, shared: 0, retried: 0, failed: 0, slow: 0 };
const sleep = ms => new Promise(r => setTimeout(r, ms));
const worlds = new WeakMap();   // db → Map(prefix → { flying: the write in the air, next: the write that has not begun (it carries every nudge that comes before it does), last: when the last began })

function stateOf(db, prefix) {
  let m = worlds.get(db); if (!m) worlds.set(db, m = new Map());
  let st = m.get(prefix); if (!st) m.set(prefix, st = { flying: null, next: null, last: 0 });
  return st;
}
/** One write of the counter, each try given TRY_MS (a write that hangs must not hold back the nudges behind it), with one more try after a short random wait. Never throws. */
async function writeOnce(db, FV, prefix, st) {
  st.last = Date.now();
  for (let tries = 0; tries < 2; tries++) {
    try {
      stats.writes++;
      let timer = 0;
      const wrote = db.collection(prefix + COLL).doc(DOC).set({ n: FV.increment(1), at: typeof FV.serverTimestamp === "function" ? FV.serverTimestamp() : Date.now() }, { merge: true });
      try { await Promise.race([wrote, new Promise((_, no) => { timer = setTimeout(() => no(new Error("the counter write took longer than " + TRY_MS + " ms")), TRY_MS); })]); } finally { clearTimeout(timer); }
      return true;
    } catch (e) {
      if (!tries) { stats.retried++; await sleep(RETRY_MIN_MS + Math.random() * RETRY_SPREAD_MS); continue; }
      stats.failed++; console.warn("[charmNestPush] counter not written (" + (prefix || "production") + "):", e && e.message);
    }
  }
  return false;
}
/** A write moved the feed: raise the counter, sharing the write with every nudge that is waiting for the next one. Never throws, never waits longer than WAIT_MS. */
function nudge(db, FV, prefix) {
  try {
    prefix = prefix || ""; stats.asked++;
    const st = stateOf(db, prefix);
    let p;
    if (st.next) { stats.shared++; p = st.next; }   // (a write that has not begun carries this nudge too)
    else {
      const behind = st.flying || Promise.resolve();
      p = st.next = behind.then(() => { const w = st.last + GAP_MS - Date.now(); return w > 0 ? sleep(w) : null; }).then(() => {
        st.next = null;   // (from here a nudge belongs to the write after this one)
        const f = st.flying = writeOnce(db, FV, prefix, st);
        f.then(() => { if (st.flying === f) st.flying = null; });
        return f;
      });
    }
    let timer = 0;
    return Promise.race([p, new Promise(r => { timer = setTimeout(() => { stats.slow++; r(false); }, WAIT_MS); })]).then(ok => { clearTimeout(timer); return ok; }, () => { clearTimeout(timer); return false; });
  } catch (_) { return Promise.resolve(false); }   // (never the person's write)
}
module.exports = { COLL, DOC, GAP_MS, WAIT_MS, nudge, stats };
