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
  const mode = ["grouping", "layout", "name"].includes(body.mode) ? body.mode : null;
  if (!id || !mode || !body.payload) return { statusCode: 400, body: "bad payload" };
  const ref = db.collection(COLL).doc(id);
  const snap = await ref.get();
  if (!snap.exists) return { statusCode: 404, body: "unknown job" };
  if (snap.data().status !== "pending") return { statusCode: 200, body: "already handled" };
  await ref.set({ status: "running", startedAt: FV.serverTimestamp(), updatedAt: FV.serverTimestamp() }, { merge: true });
  try {
    const out = await agent.run(mode, body.payload);
    await ref.set({ status: "done", result: out, finishedAt: FV.serverTimestamp(), updatedAt: FV.serverTimestamp() }, { merge: true });
    console.log(`[charmNestAgent] ${id} ${mode}: ${out.skipped ? "skipped: " + out.skipped : out.error ? "error: " + out.error : "ok"} · in=${out.usage && out.usage.input_tokens} out=${out.usage && out.usage.output_tokens}`);
  } catch (e) {
    console.error("[charmNestAgent]", id, e);
    await ref.set({ status: "error", error: String(e && e.message || e), updatedAt: FV.serverTimestamp() }, { merge: true });
  }
  return { statusCode: 200, body: "ok" };
};
