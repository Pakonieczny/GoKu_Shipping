/*  netlify/functions/_charmNestReviewSettled.js — REVIEW'S "RESOLVED" DECISIONS, KEPT IN THE CLOUD (S2, Paul 21 Nov 2026: "staff keep FULL use of every input, saved in the cloud,
 *  seen on every computer").
 *
 *  Review.remove (charm-nest-bridge.js) puts each decision a person answered at the top of Review.settled, the list Resolved draws. It came back from the browser's workspace; in cloud
 *  mode "on" a page is a viewer and keeps no workspace, so the list was lost at every reload and differed from one computer to the next. charm-nest-review-core.js (CLOUD 5) planned a
 *  cloud list written where Review.remove writes: this is it.
 *
 *    reviewSettledPut { rec | recs: [rec], by, dev }   (PRESS: a person's answer)
 *        rec { key (the decision's key), kind, why, answer (the answer in words), by, t (ms, when answered: the record's id with key), orders [rid], lines, pieces, rowKey, how }
 *        kept in ONE document (Charm_Nest_Review_Settled/list, the sandbox's own under its prefix), as JSON text: the newest 500, none older than 14 days; a record sent again
 *        (same key and t) is written once. Every page hears it at once through the changes feed (g:review-settled).
 *    reviewSettledGet {}                               (READ) → { settled: [rec] newest first, at, n }
 *  The cloud world (_charmNestWorld.js) reads the same list into the Review core's snapshot (settled), so Resolved's cards and counts are the same on every computer.
 *  Nothing here moves a piece, a sheet or a line.                                                                                                                                          */
"use strict";

const COLL = "Charm_Nest_Review_Settled", DOC = "list", FEED = "g:review-settled";
const MAX = 500, KEEP_MS = 14 * 86400000, BATCH = 20;
const num = v => (Number.isFinite(+v) ? +v : 0);
const str = (v, n) => (v == null ? "" : String(v)).slice(0, n || 300);
const idOf = r => `${r.key}\u0001${r.t}`;

/** A record as kept: only the fields Resolved and the Review core read, each bounded. null when it names no decision. */
function clean(r, now) {
  if (!r || typeof r !== "object" || Array.isArray(r)) return null;
  const key = str(r.key, 300); if (!key || /^(eng|held):/.test(key)) return null;
  let t = Math.round(num(r.t)); if (!(t > 0)) t = now; if (t > now + 60000) t = now;
  const orders = [...new Set((Array.isArray(r.orders) ? r.orders : []).map(x => str(x, 40).trim()).filter(Boolean))].slice(0, 20);
  const out = { key, kind: str(r.kind, 40), why: str(r.why, 300), answer: str(r.answer, 300), by: str(r.by, 80), t, orders, lines: Math.max(0, Math.round(num(r.lines))), pieces: Math.max(0, Math.round(num(r.pieces))), rowKey: str(r.rowKey, 80) };
  if (r.how != null && r.how !== "") out.how = str(r.how, 40);
  return out;
}
const parse = d => { try { const x = JSON.parse((d && d.j) || "[]"); return Array.isArray(x) ? x.filter(r => r && typeof r === "object" && r.key) : []; } catch (_) { return []; } };
/** The list kept: newest first, MAX at most, nothing older than KEEP_MS, one record per key and time. */
function trim(list, now) {
  const seen = new Set(), out = [];
  for (const r of list.slice().sort((a, b) => (num(b.t) - num(a.t)) || (a.key < b.key ? -1 : a.key > b.key ? 1 : 0))) {
    if (num(r.t) < now - KEEP_MS) continue;
    const id = idOf(r); if (seen.has(id)) continue; seen.add(id); out.push(r);
    if (out.length >= MAX) break;
  }
  return out;
}

module.exports = function (d) {
  const { db, col, revTouch } = d;
  const ref = () => col(COLL).doc(DOC);
  async function op_reviewSettledPut(b) {
    const now = Date.now(), by = str(b.by, 80), dev = str(b.dev, 60);
    const recs = (Array.isArray(b.recs) ? b.recs : b.rec ? [b.rec] : []).slice(0, BATCH).map(r => clean(Object.assign({ by }, r), now)).filter(Boolean);
    if (!recs.length) return { error: "a decision (rec { key, t }) is required" };
    const out = await db.runTransaction(async tx => {
      const s = await tx.get(ref()), x = s.exists ? s.data() || {} : {}, had = parse(x), ids = new Set(had.map(idOf));
      const fresh = recs.filter(r => !ids.has(idOf(r)));
      if (!fresh.length) return { ok: true, wrote: 0, n: had.length, at: num(x.at) };
      const list = trim(had.concat(fresh), now);
      tx.set(ref(), { j: JSON.stringify(list), at: now, n: (num(x.n) || 0) + 1, size: list.length, by, dev });
      return { ok: true, wrote: fresh.length, n: list.length, at: now };
    });
    if (out && out.ok && out.wrote) revTouch(b, [FEED]);
    return out;
  }
  async function op_reviewSettledGet() {
    const s = await ref().get(), x = s.exists ? s.data() || {} : {};
    return { ok: true, settled: trim(parse(x), Date.now()), at: num(x.at), n: num(x.n) };
  }
  return { COLL, FEED, ops: { reviewSettledPut: op_reviewSettledPut, reviewSettledGet: op_reviewSettledGet } };
};
module.exports.COLL = COLL; module.exports.FEED = FEED; module.exports.clean = clean; module.exports.trim = trim; module.exports.MAX = MAX; module.exports.KEEP_MS = KEEP_MS;
