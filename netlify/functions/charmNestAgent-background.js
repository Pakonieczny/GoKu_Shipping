/*  netlify/functions/charmNestAgent-background.js
 *  Runs one Claude job (grouping review, sheet inspection or naming) with the
 *  15-minute background budget and writes the result to Charm_Nest_Agent/{id}.
 *  Started by charmNestLibrary op=startAgent; the page polls op=getAgent.     */
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
  const mode = ["grouping", "layout", "name", "place", "packing", "labelRead", "customRead"].includes(body.mode) ? body.mode : null;
  if (!id || !mode) return { statusCode: 400, body: "bad payload" };
  // a sandbox job (sandbox:true in the kick) is the sandbox's own record, which its reset clears
  const ref = db.collection((body.sandbox === true ? "Sandbox_" : "") + COLL).doc(id);
  const snap = await ref.get();
  if (!snap.exists) return { statusCode: 404, body: "unknown job" };
  if (snap.data().status !== "pending") return { statusCode: 200, body: "already handled" };
  await ref.set({ status: "running", startedAt: FV.serverTimestamp(), updatedAt: FV.serverTimestamp() }, { merge: true });
  const payloadPath = snap.data().payloadPath;
  try {
    // the payload (images) was parked in Storage by startAgent; an inline payload is accepted for tests
    let payload = body.payload;
    if (!payload && payloadPath) { const [buf] = await admin.storage().bucket().file(payloadPath).download(); payload = JSON.parse(buf.toString("utf8")); }
    if (!payload) throw new Error("no payload");
    // (is a line a custom order: its own reader, which keeps every answer per line)
    const out = mode === "customRead" ? await require("./_charmNestCustomRead").run(payload) : await agent.run(mode, payload);
    if (payloadPath) admin.storage().bucket().file(payloadPath).delete().catch(() => {});
    await ref.set({ status: "done", result: out, finishedAt: FV.serverTimestamp(), updatedAt: FV.serverTimestamp() }, { merge: true });
    console.log(`[charmNestAgent] ${id} ${mode}: ${out.skipped ? "skipped: " + out.skipped : out.error ? "error: " + out.error : "ok"} · in=${out.usage && out.usage.input_tokens} out=${out.usage && out.usage.output_tokens}`);
  } catch (e) {
    console.error("[charmNestAgent]", id, e);
    // a failed job is not run again (a retry parks a new payload): its parked payload goes, its error stays on the record
    if (payloadPath) { try { await admin.storage().bucket().file(payloadPath).delete(); } catch (_) { /* already gone */ } }
    await ref.set({ status: "error", error: String(e && e.message || e), updatedAt: FV.serverTimestamp() }, { merge: true });
  }
  return { statusCode: 200, body: "ok" };
};
