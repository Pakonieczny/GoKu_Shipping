/*  netlify/functions/etsyMailRewriteUrls.js
 *
 *  One-shot cleanup endpoint to fix Gmail-watcher-created threads that
 *  have the old `/your/conversations/<id>` URL format on them. Rewrites
 *  the URL to canonical `/messages/<id>` format on:
 *    - EtsyMail_Threads docs (etsyConversationUrl field)
 *    - EtsyMail_Jobs docs   (payload.etsyConversationUrl field)
 *  Then optionally resets thread status to `detected_from_gmail` and
 *  re-enqueues scrape jobs for them.
 *
 *  This is owner-gated and idempotent — running it twice is harmless.
 *
 *  Usage:
 *    POST /.netlify/functions/etsyMailRewriteUrls
 *    Body: { actor: "Paul K", rescrape: true }
 *
 *  Returns: { ok, threadsUpdated, jobsUpdated, jobsEnqueued, errors }
 */

"use strict";

const admin = require("./firebaseAdmin");
const { CORS, requireExtensionAuth } = require("./_etsyMailAuth");
const { requireOwner } = require("./_etsyMailRoles");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

const OLD_URL_PATTERN = /\/your\/conversations\/(\d+)/;

function rewriteUrl(url) {
  if (!url) return null;
  const m = url.match(OLD_URL_PATTERN);
  if (!m) return null;
  return `https://www.etsy.com/messages/${m[1]}`;
}

function json(statusCode, body) {
  return {
    statusCode,
    headers: { ...CORS, "Content-Type": "application/json" },
    body: JSON.stringify(body)
  };
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers: CORS, body: "" };
  if (event.httpMethod !== "POST") return json(405, { error: "POST required" });

  const auth = requireExtensionAuth(event);
  if (!auth.ok) return auth.response;

  let body = {};
  try { body = JSON.parse(event.body || "{}"); }
  catch { return json(400, { error: "Invalid JSON body" }); }

  const { actor, rescrape } = body;
  if (!actor) return json(400, { error: "actor required" });

  const owner = await requireOwner(actor);
  if (!owner.ok) return json(403, { error: "Owner role required", reason: owner.reason });

  const result = {
    threadsUpdated: 0,
    jobsUpdated   : 0,
    jobsEnqueued  : 0,
    errors        : []
  };

  try {
    // ── 1. Rewrite EtsyMail_Threads docs ─────────────────────────────
    const threadsSnap = await db.collection("EtsyMail_Threads").get();

    for (const doc of threadsSnap.docs) {
      try {
        const data = doc.data() || {};
        const oldUrl = data.etsyConversationUrl;
        const newUrl = rewriteUrl(oldUrl);
        if (!newUrl || newUrl === oldUrl) continue;

        const update = {
          etsyConversationUrl: newUrl,
          updatedAt          : FV.serverTimestamp(),
          urlMigratedAt      : FV.serverTimestamp(),
          urlMigratedBy      : actor
        };

        // If rescrape requested AND thread hasn't been successfully
        // scraped (i.e., it has 0 captured messages or status is still
        // detected_from_gmail / etsy_scraped with empty msgs), reset
        // it back to detected_from_gmail so the extension re-claims it.
        const msgCount = Array.isArray(data.messages) ? data.messages.length : 0;
        const status   = data.status || "";
        const isEmpty  = msgCount === 0;

        if (rescrape && isEmpty && (status === "detected_from_gmail" || status === "etsy_scraped")) {
          update.status = "detected_from_gmail";
        }

        await doc.ref.update(update);
        result.threadsUpdated++;
      } catch (e) {
        result.errors.push(`thread ${doc.id}: ${e.message}`);
      }
    }

    // ── 2. Rewrite EtsyMail_Jobs docs ────────────────────────────────
    const jobsSnap = await db.collection("EtsyMail_Jobs").get();
    const jobsToReQueue = [];

    for (const doc of jobsSnap.docs) {
      try {
        const data = doc.data() || {};
        const oldUrl = (data.payload || {}).etsyConversationUrl;
        const newUrl = rewriteUrl(oldUrl);
        if (!newUrl || newUrl === oldUrl) continue;

        const update = {
          "payload.etsyConversationUrl": newUrl,
          updatedAt                     : FV.serverTimestamp(),
          urlMigratedAt                 : FV.serverTimestamp(),
          urlMigratedBy                 : actor
        };

        // If rescrape requested AND the job had completed/failed against
        // the wrong URL, reset it to queued so the extension claims it.
        if (rescrape && (data.status === "completed" || data.status === "failed" || data.status === "claimed")) {
          update.status     = "queued";
          update.attempts   = 0;
          update.claimedBy  = null;
          update.claimedAt  = null;
          update.lastError  = null;
          update.completedAt = null;
          jobsToReQueue.push(doc.id);
        }

        await doc.ref.update(update);
        result.jobsUpdated++;
      } catch (e) {
        result.errors.push(`job ${doc.id}: ${e.message}`);
      }
    }

    result.jobsEnqueued = jobsToReQueue.length;

    // ── 3. Audit row ─────────────────────────────────────────────────
    await db.collection("EtsyMail_Audit").add({
      threadId : null,
      eventType: "gmail_url_migration",
      actor,
      payload  : result,
      createdAt: FV.serverTimestamp()
    }).catch(() => {});

    return json(200, { ok: true, ...result });
  } catch (err) {
    console.error("etsyMailRewriteUrls error:", err);
    return json(500, { error: err.message, partial: result });
  }
};
