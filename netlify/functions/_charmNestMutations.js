/* _charmNestMutations.js — a write sent twice is written once (Paul, 28 Oct 2026: "every press to feel like a native app": nothing he presses waits for
   the cloud; plan: the local-first design, phase 3, the outbox).

   The page now keeps every press it sends in an outbox on its own computer (charm-nest-store.js mutate) and sends it from there (charm-nest-sync-worker.js
   drain). A press whose answer never came back (the page closed mid-request, the network dropped, a laptop slept) is sent again, by this page or by the
   next one after a reload. Each carries its mutation id, `mid` ("<this computer's id>-<a count>"), and the first time an op sees it the answer is kept here,
   Charm_Nest_Mutations/<mid> (Sandbox_Charm_Nest_Mutations in the sandbox: charm-nest-sandbox-families.js names it, a reset wipes it):
     · begin(mid): a document is CREATED (it fails when one is there: two sends of one press never both run). Found done: its kept answer is the answer
       (replayed, nothing written). Found running: the first send is still under way, the second is told to come back (inFlight); one left running longer
       than STALE_MS (an instance that died mid-op) is taken over and run again (every op the outbox sends is safe to run twice: each writes what it says,
       by key);
     · end(answer): the answer is kept (≤ 20 KB of it; a bigger one keeps its ok, error and status), a refusal too (the same refusal comes back);
     · drop(): an op that threw (a 500) keeps nothing, so the resend runs it again.
   expireAt: 7 days on (TTL_MS). Firestore deletes it by itself once the collection's TTL policy on `expireAt` is on (Firebase console: Firestore → TTL,
   collection group Charm_Nest_Mutations and Sandbox_Charm_Nest_Mutations, field expireAt); until then the documents stay, small and never read again
   past their week.
   Cost: one create and one write per press sent through the outbox; a replay is one read. */
"use strict";
const COLL = "Charm_Nest_Mutations", TTL_MS = 7 * 86400000, STALE_MS = 120000, KEEP_BYTES = 20000;
const MID_FORM = /^[\w.:-]{6,120}$/;
const idOf = mid => String(mid).replace(/[^\w.:-]/g, "_").slice(0, 120);
const exists = e => /ALREADY_EXISTS|already exists/i.test(String((e && (e.message || e.code)) || "")) || (e && e.code === 6);

/** The answer as kept: whole when it is small, else what a resend needs to know (ok, error, status, the record's revision and the feed's count). */
function keepable(out) {
  const s = JSON.stringify(out == null ? {} : out);
  if (s.length <= KEEP_BYTES) return s;
  const o = out && typeof out === "object" ? out : {};
  return JSON.stringify(Object.fromEntries(["ok", "error", "status", "refused", "rev", "n", "id", "overwrote", "mid", "feed"].filter(k => o[k] !== undefined).map(k => [k, o[k]])));
}

/** db: the Firestore; prefix: "" or "Sandbox_"; Timestamp: firebase-admin's (expireAt). → { ref } (run the op), { replay: { status, out } } or { busy: true }. */
async function begin(db, prefix, mid, o = {}) {
  if (!MID_FORM.test(String(mid || ""))) return { ref: null };
  const ref = db.collection((prefix || "") + COLL).doc(idOf(mid)), now = Date.now();
  const doc = { mid: String(mid).slice(0, 120), op: String(o.op || "").slice(0, 60), by: String(o.by || "").slice(0, 80), state: "running", at: now, expireAt: o.Timestamp && o.Timestamp.fromMillis ? o.Timestamp.fromMillis(now + TTL_MS) : now + TTL_MS };
  try { await ref.create(doc); return { ref, first: true }; }
  catch (e) { if (!exists(e)) { console.warn("[mutations] not kept:", e && e.message); return { ref: null }; } }
  const snap = await ref.get(), d = snap.exists ? snap.data() || {} : null;
  if (d && d.state === "done") { let out = {}; try { out = JSON.parse(d.out || "{}"); } catch (_) { out = {}; } return { replay: { status: +d.status || 200, out } }; }
  if (d && now - (+d.at || 0) < STALE_MS) return { busy: true };
  await ref.set(Object.assign({}, doc, { takeover: ((d && +d.takeover) || 0) + 1 }), { merge: true });
  return { ref, takeover: true };
}
async function end(ledger, status, out) {
  if (!ledger || !ledger.ref) return;
  try { await ledger.ref.set({ state: "done", status: +status || 200, out: keepable(out), doneAt: Date.now() }, { merge: true }); }
  catch (e) { console.warn("[mutations] answer not kept:", e && e.message); }
}
async function drop(ledger) {
  if (!ledger || !ledger.ref) return;
  try { await ledger.ref.delete(); } catch (e) { console.warn("[mutations] not let go:", e && e.message); }
}

/* ── the same field written by two people (Paul, 28 Oct 2026, decision 4: "the later change wins and the person whose change lost is told") ──
   A patch names the fields it writes and what it saw in them (base: field → the value it started from). Where the record now holds something else than
   both that base and the patch (another person wrote it since), the patch still wins, and the answer names who it overwrote (overwrote: [{ field, by, at }])
   so the page says so; the record keeps who wrote each field last (_w: field → { by, at, mid }), so the person whose change was overwritten is told too
   (charm-nest-store.js reads _w of a record it holds an acknowledged write of). */
const blank = v => (v === undefined || v === false ? null : v);   // (a field never written and a mark taken away read the same: releaseFull absent is releaseFull false)
const same = (a, b) => JSON.stringify(blank(a)) === JSON.stringify(blank(b));
/** cur: field → the record's value now; want: field → the patch's; base: field → what the page saw; w: the record's _w. → [{ field, by, at }] */
function overlap(cur, want, base, w) {
  const out = [];
  if (!base || typeof base !== "object") return out;
  for (const f of Object.keys(want || {})) {
    if (!Object.prototype.hasOwnProperty.call(base, f)) continue;
    if (same(cur[f], base[f]) || same(cur[f], want[f])) continue;
    const who = w && w[f] && typeof w[f] === "object" ? w[f] : {};
    out.push({ field: f, by: String(who.by || ""), at: +who.at || 0 });
  }
  return out;
}
/** The record's _w after this write: each field written named under who wrote it now (at most 400 names, the newest kept). */
function stampFields(w, fields, by, mid, now) {
  const next = Object.assign({}, w && typeof w === "object" ? w : {});
  for (const f of fields) next[String(f).slice(0, 120)] = { by: String(by || "").slice(0, 80), at: now || Date.now(), mid: String(mid || "").slice(0, 120) };
  const keys = Object.keys(next);
  if (keys.length > 400) for (const k of keys.sort((a, b) => (+next[a].at || 0) - (+next[b].at || 0)).slice(0, keys.length - 400)) delete next[k];
  return next;
}

module.exports = { COLL, TTL_MS, STALE_MS, MID_FORM, begin, end, drop, keepable, overlap, stampFields, same };
