/* netlify/functions/geminiImageProxy.js
   Synchronous entry point for the Listing Generator image pipeline.

   WHY THIS FILE IS NO LONGER A ONE-LINE RE-EXPORT
   -----------------------------------------------
   This used to be:

       const impl = require("./geminiImageProxy-background");
       exports.handler = impl.handler;

   which meant a browser POST to /.netlify/functions/geminiImageProxy ran the
   FULL image-model call inline on a *synchronous* function invocation. Gemini /
   OpenAI image edits at 2048x2048 routinely take 30-120s, but a synchronous
   Netlify function is capped (10s by default, 26s maximum), and any proxy in
   front of the request kills an idle connection well before the model answers.
   The caller therefore got an HTML error page instead of JSON:

       POST /.netlify/functions/geminiImageProxy  ->  504
       <HTML><HEAD><TITLE>Inactivity Timeout</TITLE> ... Too much time has passed

   ...which surfaced in the browser as
   "generator returned HTTP 504: <HTML> ... Inactivity Timeout".

   The important part: the synchronous path NEVER returned the finished image
   anyway. On success it returned `{ ok: true, jobId }` (HTTP 202) and the
   client discovered the result by polling Firebase Storage / the job doc. So
   holding the HTTP connection open for the whole model call bought nothing and
   cost every long request a 504.

   WHAT THIS DOES NOW
   ------------------
   Long-running kinds ("edits", "generations", "charm_postscale",
   "run_set_async") are handed to geminiImageProxy-background - a Netlify
   Background Function, which gets a 15 minute budget - and this handler
   returns 202 { ok: true, jobId, accepted: true } immediately. The response is
   a superset of what callers already received, so existing clients (which poll
   for the output) keep working unchanged.

   Everything else (alloc_set, charm_pool_pick, charm_restore, batch_*,
   copy_to_slot, write_manifest, move_set_to_completed, scan_empty_sets,
   files_cleanup, ...) is fast and returns data the caller actually reads, so it
   still runs inline - but wrapped in a deadline so a slow call returns readable
   JSON instead of an HTML gateway page.
*/

const impl = require("./geminiImageProxy-background");

// Kinds whose work is an image-model call. These already answer
// "202 { ok, jobId }" and are resolved by the client via polling, so they are
// safe - and correct - to run out-of-band.
const LONG_RUNNING_KINDS = new Set([
  "edits",
  "generations",
  "charm_postscale",
  "run_set_async",
]);

const BACKGROUND_FN = "geminiImageProxy-background";
const JOBS_COLL = "ListingGenerator1Jobs";

function clampInt(value, min, max, fallback) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.min(max, Math.max(min, Math.round(n)));
}

// Ceiling for work we still run inline. Netlify's synchronous limit is 26s at
// most; stopping just short of it means we emit JSON the browser can parse
// instead of letting the gateway return an HTML timeout page.
const INLINE_DEADLINE_MS = clampInt(process.env.GEMINI_PROXY_INLINE_DEADLINE_MS, 5000, 25000, 24000);

// A Background Function replies 202 as soon as Netlify accepts the invocation,
// so this only needs to cover accept-time, not run-time.
const DISPATCH_TIMEOUT_MS = clampInt(process.env.GEMINI_PROXY_DISPATCH_TIMEOUT_MS, 1000, 20000, 10000);

const CORS_HEADERS = {
  "Content-Type": "application/json",
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type, Authorization",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

function json(statusCode, obj) {
  return { statusCode, headers: CORS_HEADERS, body: JSON.stringify(obj) };
}

function newId() {
  return globalThis.crypto?.randomUUID
    ? globalThis.crypto.randomUUID()
    : require("crypto").randomUUID();
}

function rawBody(event) {
  if (!event || !event.body) return "";
  return event.isBase64Encoded
    ? Buffer.from(event.body, "base64").toString("utf8")
    : String(event.body);
}

function lowerHeaders(event) {
  const out = {};
  for (const [k, v] of Object.entries(event?.headers || {})) {
    out[String(k).toLowerCase()] = v;
  }
  return out;
}

// Resolve this deployment's own origin so we can invoke a sibling function.
// Prefer the incoming Host (correct for custom domains, branch deploys and
// deploy previews alike); fall back to Netlify's build-time env vars.
function selfOrigin(event) {
  const h = lowerHeaders(event);
  const host = h["x-forwarded-host"] || h["host"];
  if (host) {
    const proto = String(h["x-forwarded-proto"] || "https").split(",")[0].trim() || "https";
    return `${proto}://${host}`;
  }
  const envUrl = process.env.DEPLOY_PRIME_URL || process.env.DEPLOY_URL || process.env.URL;
  return envUrl ? String(envUrl).replace(/\/+$/, "") : null;
}

// Record the job as "queued" the instant we accept it, so a client that polls
// the job doc sees the request exists before the background function has had a
// chance to cold-start. Best-effort only: never block or fail dispatch on it.
async function seedQueuedJob({ jobId, kind, model, slotIndex, runId, activeCategory, outputBasePath }) {
  try {
    const admin = require("./firebaseAdmin");
    await admin.firestore().collection(JOBS_COLL).doc(jobId).set(
      {
        status: "queued",
        stage: "queued",
        kind: kind || null,
        model: model || null,
        runId: runId || null,
        slotIndex: typeof slotIndex === "number" ? slotIndex : null,
        activeCategory: activeCategory || null,
        outputBasePath: outputBasePath || null,
        dispatchedTo: BACKGROUND_FN,
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );
    return true;
  } catch (err) {
    console.warn("[geminiImageProxy] could not seed queued job doc:", err?.message || err);
    return false;
  }
}

// Run `promise`, but never let the invocation sit past `ms`. Resolves to a
// sentinel instead of throwing so the caller decides what to return.
function withDeadline(promise, ms) {
  let timer = null;
  const guard = new Promise((resolve) => {
    timer = setTimeout(() => resolve({ __deadline: true }), ms);
    if (typeof timer?.unref === "function") timer.unref();
  });
  return Promise.race([promise, guard]).finally(() => {
    if (timer) clearTimeout(timer);
  });
}

exports.handler = async (event) => {
  if (event?.httpMethod === "OPTIONS") return json(200, { ok: true });
  if (event?.httpMethod && event.httpMethod !== "POST") {
    return json(405, { error: { message: "Method not allowed" } });
  }

  const raw = rawBody(event);
  let body = null;
  try { body = raw ? JSON.parse(raw) : {}; } catch (_) { body = null; }
  if (!body || typeof body !== "object") {
    return json(400, { error: { message: "Invalid JSON body" } });
  }

  const kind = String(body.kind || "edits");

  // ---------------------------------------------------------------
  // Fast path: work that returns data the caller actually reads.
  // Still guarded so we never hand the browser an HTML timeout page.
  // ---------------------------------------------------------------
  if (!LONG_RUNNING_KINDS.has(kind)) {
    const result = await withDeadline(impl.handler(event), INLINE_DEADLINE_MS);
    if (result && result.__deadline) {
      console.warn(`[geminiImageProxy] inline kind "${kind}" exceeded ${INLINE_DEADLINE_MS}ms`);
      return json(504, {
        ok: false,
        timedOut: true,
        kind,
        error: {
          code: "SYNC_TIMEOUT",
          message:
            `"${kind}" did not finish within the ${Math.round(INLINE_DEADLINE_MS / 1000)}s synchronous ` +
            `function limit. It may still be completing server-side - re-check state before retrying, ` +
            `or send this request to /.netlify/functions/${BACKGROUND_FN} instead.`,
        },
      });
    }
    return result;
  }

  // ---------------------------------------------------------------
  // Long path: accept now, generate out-of-band.
  // ---------------------------------------------------------------
  const jobId = String(body.jobId || "").trim() || `lg1_${Date.now()}_${newId().slice(0, 8)}`;
  const payload = { ...body, jobId };
  const forwardedEvent = { ...event, body: JSON.stringify(payload), isBase64Encoded: false };

  await seedQueuedJob({
    jobId,
    kind,
    model: payload.model,
    slotIndex: payload.slotIndex,
    runId: payload.runId,
    activeCategory: payload.activeCategory,
    outputBasePath: payload.output_base_path,
  });

  const origin = selfOrigin(event);
  if (!origin) {
    console.error("[geminiImageProxy] could not resolve self origin; running inline");
    return await impl.handler(forwardedEvent);
  }

  const url = `${origin}/.netlify/functions/${BACKGROUND_FN}`;
  const controller = new AbortController();
  const abortTimer = setTimeout(() => controller.abort(), DISPATCH_TIMEOUT_MS);

  try {
    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
      signal: controller.signal,
    });

    // 404/405 means the background function isn't deployed on this site.
    // Preserve the old behavior rather than failing the request outright.
    if (res.status === 404 || res.status === 405) {
      console.warn(`[geminiImageProxy] ${BACKGROUND_FN} not reachable (HTTP ${res.status}); running inline`);
      return await impl.handler(forwardedEvent);
    }

    if (res.status >= 400) {
      const detail = await res.text().catch(() => "");
      return json(502, {
        ok: false,
        jobId,
        error: {
          code: "DISPATCH_FAILED",
          message:
            `Could not queue "${kind}" on ${BACKGROUND_FN} (HTTP ${res.status}). ` +
            (detail ? detail.slice(0, 300) : "No detail returned."),
        },
      });
    }

    return json(202, {
      ok: true,
      accepted: true,
      background: true,
      jobId,
      kind,
      dispatchedTo: BACKGROUND_FN,
      message: "Generation queued. Poll Storage (or the job doc) for the result.",
    });
  } catch (err) {
    // An abort means Netlify was slow to acknowledge, not that the invocation
    // was lost. Reporting failure here would tempt the client into a retry and
    // pay for the same image twice, so we report acceptance and flag it.
    if (err?.name === "AbortError") {
      console.warn(`[geminiImageProxy] dispatch ack timed out after ${DISPATCH_TIMEOUT_MS}ms; assuming queued`);
      return json(202, {
        ok: true,
        accepted: true,
        background: true,
        dispatchUncertain: true,
        jobId,
        kind,
        dispatchedTo: BACKGROUND_FN,
        message:
          "Generation was sent but the queue acknowledgement timed out. Poll for the output " +
          "before retrying so the same image isn't generated twice.",
      });
    }

    console.error(`[geminiImageProxy] dispatch to ${BACKGROUND_FN} failed:`, err?.message || err);
    return json(502, {
      ok: false,
      jobId,
      error: {
        code: "DISPATCH_FAILED",
        message: `Could not queue "${kind}" on ${BACKGROUND_FN}: ${String(err?.message || err)}`,
      },
    });
  } finally {
    clearTimeout(abortTimer);
  }
};
