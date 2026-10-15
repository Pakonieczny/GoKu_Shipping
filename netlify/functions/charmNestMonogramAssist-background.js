/*  netlify/functions/charmNestMonogramAssist-background.js
 *  MONOAI (Paul, 15 Oct 2026): one round of the AI assembler of a monogram that still breaks a rule (_charmNestMonogramAssist.js), run with the background
 *  budget (a vision call can pass the 10 s synchronous limit) and written to Charm_Nest_Agent/{id}, exactly like charmNestAgent-background.
 *  Started by charmNestLibrary op=startAgent mode "monoAssist" (behind the same passcode gate as every Charm Nest call; the payload is parked in Storage);
 *  the page polls op=getAgent (CN.agentCall).                                                                                                          */
"use strict";
const admin = require("./firebaseAdmin");
const { parseBody } = require("./_charmNestAuth");
const db = admin.firestore();
const FV = admin.firestore.FieldValue;
const COLL = "Charm_Nest_Agent";

exports.handler = async (event) => {
  const body = parseBody(event);
  const id = String(body.id || "").replace(/[^\w\-]/g, "").slice(0, 80);
  if (!id || body.mode !== "monoAssist") return { statusCode: 400, body: "bad payload" };
  const sandbox = body.sandbox === true;
  const ref = db.collection((sandbox ? "Sandbox_" : "") + COLL).doc(id);
  const snap = await ref.get();
  if (!snap.exists) return { statusCode: 404, body: "unknown job" };
  if (snap.data().status !== "pending" || snap.data().mode !== "monoAssist") return { statusCode: 200, body: "already handled" };
  await ref.set({ status: "running", startedAt: FV.serverTimestamp(), updatedAt: FV.serverTimestamp() }, { merge: true });
  const payloadPath = snap.data().payloadPath;
  try {
    let payload = body.payload;
    if (!payload && payloadPath) { const [buf] = await admin.storage().bucket().file(payloadPath).download(); payload = JSON.parse(buf.toString("utf8")); }
    if (!payload) throw new Error("no payload");
    const out = await require("./_charmNestMonogramAssist").run(payload, { admin, db, FV, sandbox });
    if (payloadPath) admin.storage().bucket().file(payloadPath).delete().catch(() => {});
    await ref.set({ status: "done", result: out, finishedAt: FV.serverTimestamp(), updatedAt: FV.serverTimestamp() }, { merge: true });
  } catch (e) {
    console.error("[monoAssist]", id, e);
    if (payloadPath) { try { await admin.storage().bucket().file(payloadPath).delete(); } catch (_) { /* already gone */ } }
    await ref.set({ status: "error", error: String(e && e.message || e), updatedAt: FV.serverTimestamp() }, { merge: true });
  }
  return { statusCode: 200, body: "ok" };
};
