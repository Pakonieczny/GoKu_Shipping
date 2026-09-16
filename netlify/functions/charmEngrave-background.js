/*  netlify/functions/charmEngrave-background.js
 *  The two Claude calls of the engraving pipeline (design §7.1 and §7.4), through _charmNestAgent with modes
 *  engraveIntent (reads the customer's words, strict JSON schema, high effort) and engraveReview (looks at the
 *  rendered back once, answers legibility and taste). Started by charmNestLibrary op=startAgent; the sorter polls
 *  op=getAgent. Same job record and parked payload as charmNestAgent-background.                                */
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
  const ref = db.collection(COLL).doc(id);
  const snap = await ref.get();
  if (!snap.exists) return { statusCode: 404, body: "unknown job" };
  if (snap.data().status !== "pending") return { statusCode: 200, body: "already handled" };
  await ref.set({ status: "running", startedAt: FV.serverTimestamp(), updatedAt: FV.serverTimestamp() }, { merge: true });
  try {
    let payload = body.payload;
    const payloadPath = snap.data().payloadPath;
    if (!payload && payloadPath) { const [buf] = await admin.storage().bucket().file(payloadPath).download(); payload = JSON.parse(buf.toString("utf8")); }
    if (!payload) throw new Error("no payload");
    const out = await agent.run(mode, payload);
    if (payloadPath) admin.storage().bucket().file(payloadPath).delete().catch(() => {});
    await ref.set({ status: "done", result: out, finishedAt: FV.serverTimestamp(), updatedAt: FV.serverTimestamp() }, { merge: true });
    console.log(`[charmEngrave] ${id} ${mode}: ${out.skipped ? "skipped: " + out.skipped : out.error ? "error: " + out.error : "ok"} · in=${out.usage && out.usage.input_tokens} out=${out.usage && out.usage.output_tokens}`);
  } catch (e) {
    console.error("[charmEngrave]", id, e);
    await ref.set({ status: "error", error: String(e && e.message || e), updatedAt: FV.serverTimestamp() }, { merge: true });
  }
  return { statusCode: 200, body: "ok" };
};
