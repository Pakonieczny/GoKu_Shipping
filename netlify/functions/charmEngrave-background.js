/*  netlify/functions/charmEngrave-background.js
 *  The two Claude calls of the engraving pipeline (design §7.1 and §7.4), through _charmNestAgent with modes
 *  engraveIntent (reads the customer's words, strict JSON schema, high effort) and engraveReview (looks at the
 *  rendered back once, answers legibility and taste). Started by charmNestLibrary op=startAgent; the sorter polls
 *  op=getAgent. Same job record and parked payload as charmNestAgent-background. A sandbox job (sandbox:true in the
 *  kick) is kept in Sandbox_Charm_Nest_Agent, and its reading is saved once to the sandbox's cache of paid readings
 *  (never overwritten), which answers the same words in every later copy of the order (charmNestLibrary op_startAgent). */
"use strict";
const admin = require("./firebaseAdmin");
const agent = require("./_charmNestAgent");
const { parseBody } = require("./_charmNestAuth");
const db = admin.firestore();
const FV = admin.firestore.FieldValue;
const COLL = "Charm_Nest_Agent";

exports.handler = async (event) => {
  const body = parseBody(event);
  const id = String(body.id || "").replace(/[^\w\-]/g, "").slice(0, 80);
  const mode = ["engraveIntent", "engraveReview"].includes(body.mode) ? body.mode : null;
  if (!id || !mode) return { statusCode: 400, body: "bad payload" };
  const sandbox = body.sandbox === true;
  const ref = db.collection((sandbox ? "Sandbox_" : "") + COLL).doc(id);
  const snap = await ref.get();
  if (!snap.exists) return { statusCode: 404, body: "unknown job" };
  if (snap.data().status !== "pending") return { statusCode: 200, body: "already handled" };
  await ref.set({ status: "running", startedAt: FV.serverTimestamp(), updatedAt: FV.serverTimestamp() }, { merge: true });
  const payloadPath = snap.data().payloadPath;
  try {
    let payload = body.payload;
    if (!payload && payloadPath) { const [buf] = await admin.storage().bucket().file(payloadPath).download(); payload = JSON.parse(buf.toString("utf8")); }
    if (!payload) throw new Error("no payload");
    const out = await agent.run(mode, payload);
    if (payloadPath) admin.storage().bucket().file(payloadPath).delete().catch(() => {});
    await ref.set({ status: "done", result: out, finishedAt: FV.serverTimestamp(), updatedAt: FV.serverTimestamp() }, { merge: true });
    const job = snap.data();
    if (sandbox && mode === "engraveIntent" && /^[0-9a-f]{40}$/.test(String(job.cacheKey || "")) && !out.skipped && !out.error) {
      const cache = db.collection("Sandbox_Charm_Nest_Agent_Cache").doc(job.cacheKey);
      await db.runTransaction(async t => { if (!(await t.get(cache)).exists) t.set(cache, { key: job.cacheKey, mode, order: job.order || null, result: out, jobId: id, createdAt: FV.serverTimestamp() }); })
        .catch(e => console.warn("[charmEngrave] reading not cached:", e.message));
    }
    console.log(`[charmEngrave] ${id} ${mode}: ${out.skipped ? "skipped: " + out.skipped : out.error ? "error: " + out.error : "ok"} · in=${out.usage && out.usage.input_tokens} out=${out.usage && out.usage.output_tokens}`);
  } catch (e) {
    console.error("[charmEngrave]", id, e);
    // a failed job is not run again (a retry parks a new payload): its parked payload goes, its error stays on the record
    if (payloadPath) { try { await admin.storage().bucket().file(payloadPath).delete(); } catch (_) { /* already gone */ } }
    await ref.set({ status: "error", error: String(e && e.message || e), updatedAt: FV.serverTimestamp() }, { merge: true });
  }
  return { statusCode: 200, body: "ok" };
};
