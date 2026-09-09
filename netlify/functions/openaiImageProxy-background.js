/* Compatibility for already-open Listing Generator pages that dispatch
 * directly to the background endpoint without supplying a job ID. */
const imageWorker = require("./geminiImageProxy-background");
const { randomUUID } = require("crypto");

exports.handler = async (event) => {
  if (event?.httpMethod !== "POST") return imageWorker.handler(event);
  let body;
  try {
    body = JSON.parse(event.isBase64Encoded
      ? Buffer.from(event.body || "", "base64").toString("utf8") : event.body || "{}");
  } catch (_) { return imageWorker.handler(event); }
  if (["edits", "generations", "charm_postscale"].includes(body?.kind || "edits") && !body.jobId) {
    body.jobId = `lg1_${Date.now()}_${randomUUID()}`;
  }
  return imageWorker.handler({ ...event, body: JSON.stringify(body), isBase64Encoded: false });
};
