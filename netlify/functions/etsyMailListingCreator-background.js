/*  netlify/functions/etsyMailListingCreator-background.js
 *
 *  Closes the Custom Brites sales loop end-to-end. Triggered by
 *  etsyMailListingCreatorCron when it claims a thread that the sales
 *  agent has flagged customerAccepted: true.
 *
 *  Flow per invocation:
 *    1. Load thread + draft, validate inputs, idempotency check
 *    2. Resolve the family's template listing (necklace / huggie / stud)
 *    3. Read template structural defaults (shipping_profile_id, taxonomy_id,
 *       readiness_state_id, etc.) from Etsy
 *    4. Generate title + description + tags with Claude
 *    5. Create a brand-new draft listing on Etsy
 *       (POST /shops/{shop}/listings?legacy=false  —  price in CENTS)
 *    6. Upload customer reference photos as listing images (multipart)
 *       Falls back to the template's image if the customer never sent one
 *    7. Set inventory: SKU, decimal price, quantity, readiness_state_id
 *       (PUT /listings/{id}/inventory?legacy=false  —  price in DECIMAL)
 *    8. Publish the listing (PATCH state=active, x-www-form-urlencoded)
 *    9. Send the live URL to the customer via etsyMailDraftSend.enqueue
 *   10. Write idempotency markers + audit row
 *
 *  Background functions on Netlify get a 15-minute timeout (vs 26s sync),
 *  which is plenty for ~12-15 Etsy API calls including 1-10 image uploads.
 *
 *  Spec source of truth: CUSTOM_LISTING_AUTOMATION_SPEC.md §4–§6
 */

"use strict";

const fetch     = require("node-fetch");
const FormData  = require("form-data");

const admin     = require("./firebaseAdmin");
const {
  etsyFetch,
  getValidEtsyAccessToken,
  SHOP_ID
} = require("./_etsyMailEtsy");
const { callClaudeRaw } = require("./_etsyMailAnthropic");
const { requireExtensionAuth, CORS } = require("./_etsyMailAuth");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

// ─── constants ───────────────────────────────────────────────────────────

const THREADS_COLL  = "EtsyMail_Threads";
const DRAFTS_COLL   = "EtsyMail_Drafts";
const AUDIT_COLL    = "EtsyMail_Audit";
const CONFIG_COLL   = "EtsyMail_Config";
const SALES_COLL    = "EtsyMail_SalesContext";   // v4.3 — reset on completion
const TEMPLATES_DOC = "listingTemplates";

const ALLOWED_FAMILIES = ["necklace", "huggie", "stud"];

const AI_MODEL =
  process.env.ETSYMAIL_LISTING_CREATOR_MODEL ||
  process.env.ETSYMAIL_SALES_MODEL ||
  "claude-opus-4-7";

const MAX_IMAGES_PER_LISTING = 10;   // Etsy's hard cap

// ─── small helpers ───────────────────────────────────────────────────────

/** Resolve the deployed base URL for inter-function calls. Mirrors the
 *  pattern used by etsyMailAutoPipeline-background.js so dev / preview /
 *  production all work without code changes. */
function functionsBase() {
  return process.env.URL
      || process.env.DEPLOY_URL
      || process.env.NETLIFY_BASE_URL
      || "http://localhost:8888";
}

function pickFamily(thread) {
  // Prefer the explicit field if the sales agent wrote it (post-§7.6 deploy),
  // otherwise fall back to the family inside lastResolverResult. Defensive
  // because §2.1 of the spec says the agent writes acceptedQuoteFamily but
  // the §7.6 sales-agent change is described as "one-line"; the production-
  // ready interpretation writes both, so we try both here.
  const direct = String(thread.acceptedQuoteFamily || "").toLowerCase().trim();
  if (direct) return direct;
  const fromResolver = String(
    thread.lastResolverResult && thread.lastResolverResult.family || ""
  ).toLowerCase().trim();
  return fromResolver;
}

function clampStr(s, n) {
  return String(s == null ? "" : s).slice(0, n);
}

// ─── 1. Load thread data ─────────────────────────────────────────────────

async function loadThreadData(threadId) {
  const threadRef = db.collection(THREADS_COLL).doc(threadId);
  const draftRef  = db.collection(DRAFTS_COLL).doc(`draft_${threadId}`);

  const [threadSnap, draftSnap] = await Promise.all([threadRef.get(), draftRef.get()]);
  if (!threadSnap.exists) throw new Error(`Thread not found: ${threadId}`);

  const thread = threadSnap.data();
  const draft  = draftSnap.exists ? draftSnap.data() : {};

  // Validate the bits we depend on. These are "invalid" errors on purpose
  // — isTerminalError() classifies them as terminal so we don't spin.
  if (!thread.customerAccepted)     throw new Error(`Thread not accepted (invalid input): ${threadId}`);
  if (!thread.acceptedQuoteUsd)     throw new Error(`No accepted quote on thread (invalid input): ${threadId}`);
  if (!thread.lastResolverResult)   throw new Error(`No resolver result on thread (invalid input): ${threadId}`);
  if (!thread.etsyConversationUrl)  throw new Error(`No conversation URL on thread (invalid input): ${threadId}`);

  const family = pickFamily(thread);
  if (!family) throw new Error(`No family on thread (invalid input): ${threadId}`);
  if (!ALLOWED_FAMILIES.includes(family)) {
    throw new Error(`Unknown product family (invalid input): ${family}`);
  }

  const referenceAttachments = Array.isArray(draft.referenceAttachments)
    ? draft.referenceAttachments.filter(a => a && a.url && (a.type === "image" || /^image\//.test(a.type || "")))
    : [];

  return { thread, draft, referenceAttachments, family };
}

async function loadThreadContext(threadId) {
  // Last 10 messages, oldest first, for the description-generator prompt.
  const snap = await db.collection(`${THREADS_COLL}/${threadId}/messages`)
    .orderBy("timestamp", "desc")
    .limit(10)
    .get();
  return snap.docs.map(d => {
    const m = d.data();
    return {
      direction: m.direction || "unknown",
      text     : clampStr(m.text, 600)
    };
  }).reverse();
}

// ─── 2. Template resolution (one per family) ────────────────────────────

async function resolveTemplateListingId(family) {
  const cfg = await db.collection(CONFIG_COLL).doc(TEMPLATES_DOC).get();
  if (!cfg.exists) {
    throw new Error("Listing templates not configured (invalid setup): missing EtsyMail_Config/listingTemplates");
  }
  const entry = cfg.data()[family];
  // Accept either schema:
  //   1. Flat string: { necklace: "1094504461" }
  //      — written by the dashboard Settings UI (saveListingTemplates)
  //        and the recommended setup format from SETUP.md.
  //   2. Map: { necklace: { listingId: "1094504461", ... } }
  //      — older schema that supports per-family extra metadata. Kept
  //        for forward compat; nothing currently writes it.
  let listingId = null;
  if (typeof entry === "string") {
    listingId = entry.trim();
  } else if (entry && typeof entry === "object" && entry.listingId) {
    listingId = String(entry.listingId).trim();
  }
  if (!listingId) {
    throw new Error(`No template configured for family (invalid setup): ${family}`);
  }
  return listingId;
}

async function readTemplateListing(templateListingId) {
  // legacy=false so readiness_state_id appears in the inventory payload
  const [listing, inventory] = await Promise.all([
    etsyFetch(`/listings/${templateListingId}`, { query: { legacy: false } }),
    etsyFetch(`/listings/${templateListingId}/inventory`, { query: { legacy: false } })
  ]);

  // Surface missing required fields with a clearer message than Etsy's
  // 400 ("listing.taxonomy_id: must not be null") — the operator sees
  // these in needsOperatorReviewReason on the thread doc.
  if (!listing.taxonomy_id) {
    throw new Error(`Template ${templateListingId} has no taxonomy_id (invalid setup)`);
  }
  if (!listing.shipping_profile_id) {
    throw new Error(`Template ${templateListingId} has no shipping_profile_id (invalid setup)`);
  }
  if (!listing.return_policy_id) {
    throw new Error(`Template ${templateListingId} has no return_policy_id (invalid setup)`);
  }

  // Walk the inventory to find any readiness_state_id (templates with a
  // processing profile expose it on every offering; we just need one).
  let readinessStateId = null;
  for (const p of (inventory.products || [])) {
    for (const o of (p.offerings || [])) {
      const n = Number(o.readiness_state_id);
      if (Number.isFinite(n) && n > 0) { readinessStateId = n; break; }
    }
    if (readinessStateId) break;
  }

  return {
    taxonomyId       : listing.taxonomy_id,
    shippingProfileId: listing.shipping_profile_id,
    returnPolicyId   : listing.return_policy_id,
    shopSectionId    : listing.shop_section_id  || null,
    whoMade          : listing.who_made   || "i_did",
    whenMade         : listing.when_made  || "made_to_order",
    isSupply         : !!listing.is_supply,
    materials        : Array.isArray(listing.materials) ? listing.materials : [],
    readinessStateId
  };
}

async function resolveReadinessStateIdFallback() {
  // Used only if the template doesn't have a readiness_state_id set.
  // Reads any existing processing profile from the shop. We never
  // auto-create one — that's an operator setup problem.
  const defs = await etsyFetch(`/shops/${SHOP_ID}/readiness-state-definitions`);
  const list = Array.isArray(defs.results) ? defs.results
            : Array.isArray(defs)         ? defs
            : [];
  if (!list.length) {
    throw new Error("No readiness_state_id available (invalid setup): configure a processing profile on the template listing");
  }
  const preferred = list.find(d => d.readiness_state === "ready_to_ship") || list[0];
  return Number(preferred.readiness_state_id);
}

// ─── 3. AI-generated title + description + tags ─────────────────────────

const TITLE_DESC_SYSTEM_PROMPT =
`You are generating Etsy listing content for a custom-order that a customer just accepted. Output JSON only with three fields: "title", "description", "tags".

CONSTRAINTS:
- title: max 140 characters, plain text, no emojis. Should be specific to this custom order (e.g., "Custom 10mm Sterling Silver Necklace Charm with Baseball Design, 16in Chain").
- description: 200-500 words, plain text, no markdown. Describe what was ordered (the spec), shipping/processing info, materials, customization details from the conversation. Mention this is a custom order made for a specific buyer.
- tags: array of up to 13 strings, each max 20 characters, lowercase, single words or short phrases (e.g., "custom necklace", "sterling silver", "baseball charm").

VOICE RULES:
- No em-dashes or en-dashes anywhere.
- "We" not "I" (Custom Brites is a shop, not a one-person operation).
- No service-script clichés ("absolutely", "happy to", "say the word", "lock it in").
- No specific timeframes for delivery.

Output ONLY valid JSON, no other text, no markdown fences.`;

function extractTextFromClaudeResponse(resp) {
  const blocks = Array.isArray(resp.content) ? resp.content : [];
  const out = [];
  for (const b of blocks) {
    if (b && b.type === "text" && typeof b.text === "string") out.push(b.text);
  }
  return out.join("").trim();
}

async function generateListingContent({ family, lastResolverResult, threadContext }) {
  const userPrompt =
`Product family: ${family}

Accepted spec (structured):
${JSON.stringify(lastResolverResult, null, 2)}

Recent conversation (last 10 messages, oldest first):
${threadContext.map(m => `[${m.direction}] ${m.text}`).join("\n")}

Output the JSON object now.`;

  const resp = await callClaudeRaw({
    model      : AI_MODEL,
    maxTokens  : 1500,
    system     : TITLE_DESC_SYSTEM_PROMPT,
    messages   : [{ role: "user", content: userPrompt }],
    useThinking: false   // single-shot generation, no need for thinking
  });

  const raw = extractTextFromClaudeResponse(resp);
  // Strip accidental markdown fences just in case
  const cleaned = raw
    .replace(/^```(?:json)?\s*/i, "")
    .replace(/\s*```\s*$/i, "")
    .trim();

  let parsed;
  try { parsed = JSON.parse(cleaned); }
  catch (e) {
    throw new Error(`Claude returned non-JSON listing content (invalid response): ${cleaned.slice(0, 200)}`);
  }

  // Validate + clamp to Etsy limits
  const title = clampStr(parsed.title, 140).trim();
  if (!title) throw new Error("Generated title is empty (invalid response)");

  const description = clampStr(parsed.description, 102400).trim();
  if (!description) throw new Error("Generated description is empty (invalid response)");

  // Etsy V3 tag rules: letters, numbers, hyphens, and spaces only;
  // ≤20 chars; ≤13 tags. We strip anything else so a stray "necklace!"
  // or "20%-off" doesn't get rejected at createDraftListing time.
  const tags = (Array.isArray(parsed.tags) ? parsed.tags : [])
    .map(t => String(t == null ? "" : t)
                .toLowerCase()
                .replace(/[^a-z0-9\- ]+/g, "")
                .replace(/\s+/g, " ")
                .trim()
                .slice(0, 20))
    .filter(Boolean)
    .slice(0, 13);

  return { title, description, tags };
}

// ─── 4. Create the draft listing ────────────────────────────────────────

async function createDraftListing({ title, description, tags, priceUsd, template }) {
  const priceCents = Math.round(Number(priceUsd) * 100);
  if (!Number.isFinite(priceCents) || priceCents <= 0) {
    throw new Error(`Invalid price (invalid input): ${priceUsd}`);
  }

  let readinessStateId = template.readinessStateId;
  if (!readinessStateId) readinessStateId = await resolveReadinessStateIdFallback();

  const body = {
    title,
    description,
    price             : priceCents,        // INTEGER cents for createDraftListing
    quantity          : 1,
    who_made          : template.whoMade  || "i_did",
    when_made         : template.whenMade || "made_to_order",
    taxonomy_id       : template.taxonomyId,           // required, validated upstream
    shipping_profile_id: template.shippingProfileId,   // required, validated upstream
    return_policy_id  : template.returnPolicyId,       // required, validated upstream
    is_supply         : !!template.isSupply,
    materials         : template.materials || [],
    tags              : Array.isArray(tags) ? tags : [],
    is_personalizable : false,             // custom listings: details live in the description
    state             : "draft",
    should_auto_renew : false,
    readiness_state_id: readinessStateId
  };
  // shop_section_id is optional — only include if the template defines one
  if (template.shopSectionId) body.shop_section_id = template.shopSectionId;

  const created = await etsyFetch(
    `/shops/${SHOP_ID}/listings`,
    { method: "POST", query: { legacy: false }, body }
  );

  const listingId = created && (created.listing_id || created.results?.[0]?.listing_id);
  if (!listingId) {
    throw new Error(`createDraftListing returned no listing_id: ${JSON.stringify(created).slice(0, 300)}`);
  }
  return String(listingId);
}

// ─── 5. Upload reference photos ──────────────────────────────────────────

async function uploadOneImage({ accessToken, listingId, imageUrl, rank, altText, filename }) {
  const imgRes = await fetch(imageUrl);
  if (!imgRes.ok) {
    throw new Error(`Image fetch failed (${imgRes.status}): ${imageUrl}`);
  }
  const buf = Buffer.from(await imgRes.arrayBuffer());

  const form = new FormData();
  form.append("image", buf, { filename: filename || `ref_${rank}.jpg` });
  form.append("rank", String(rank));
  if (altText) form.append("alt_text", clampStr(altText, 250));

  const url =
    `https://api.etsy.com/v3/application/shops/${SHOP_ID}/listings/${encodeURIComponent(listingId)}/images`;

  const res = await fetch(url, {
    method : "POST",
    headers: {
      Authorization: `Bearer ${accessToken}`,
      "x-api-key"  : `${process.env.CLIENT_ID}:${process.env.CLIENT_SECRET || process.env.ETSY_SHARED_SECRET}`,
      ...form.getHeaders()
    },
    body: form
  });

  const text = await res.text();
  if (!res.ok) {
    throw new Error(`Etsy image upload ${res.status}: ${text.slice(0, 300)}`);
  }
}

async function getTemplateImageUrls(templateListingId) {
  const data = await etsyFetch(`/listings/${templateListingId}/images`);
  const results = Array.isArray(data.results) ? data.results : [];
  return results
    .map(r => ({
      url     : r.url_fullxfull || r.url_570xN || r.url_300x300,
      type    : "image",
      source  : "template",
      filename: `template_${r.rank || 1}.jpg`
    }))
    .filter(x => x.url);
}

async function uploadReferenceImages(listingId, referenceAttachments, templateListingId) {
  // CRITICAL: Etsy refuses to publish a listing with zero images. If the
  // customer never sent a photo, fall back to the template's image so the
  // listing is at least purchasable. Operator can hot-swap later.
  let images = referenceAttachments;
  let usedFallback = false;
  if (!images.length) {
    console.warn("[listingCreator] No reference photos. Using template image fallback.");
    images = await getTemplateImageUrls(templateListingId);
    usedFallback = true;
  }
  if (!images.length) {
    throw new Error("No images available (invalid setup): template has no images and customer sent none");
  }

  const queued = images.slice(0, MAX_IMAGES_PER_LISTING);
  let success = 0;
  let lastError = null;

  for (let i = 0; i < queued.length; i++) {
    const img  = queued[i];
    const rank = i + 1;
    try {
      // Refresh the token per-image so a long upload sequence (10 images,
      // each multi-MB) doesn't run out of token mid-loop. The token cache
      // makes this cheap when no refresh is needed.
      const accessToken = await getValidEtsyAccessToken();
      await uploadOneImage({
        accessToken,
        listingId,
        imageUrl: img.url,
        rank,
        altText : img.filename || "Custom order reference",
        filename: img.filename
      });
      success++;
    } catch (e) {
      lastError = e;
      console.error(`[listingCreator] Image upload failed (rank ${rank}):`, e.message);
      // Don't throw — partial-success > total-failure. We only abort if 0 succeed.
    }
  }

  if (success === 0) {
    throw new Error(`All image uploads failed: ${lastError ? lastError.message : "unknown"}`);
  }

  return { uploaded: success, attempted: queued.length, usedFallback };
}

// ─── 6. Set inventory (SKU + decimal price + readiness_state_id) ────────

function buildSku(thread, threadId) {
  const family = String(pickFamily(thread) || "x").charAt(0).toUpperCase();
  const price  = String(Math.round(Number(thread.acceptedQuoteUsd))).padStart(3, "0");
  const tail   = String(threadId).slice(-6);
  return `CUSTOM-${family}-${price}-${tail}`;
}

async function setInventory(listingId, { priceUsd, readinessStateId, sku }) {
  // Read current inventory (createDraftListing's `price` field already
  // initialized a single product/offering — we sanitize and add SKU).
  const inv = await etsyFetch(`/listings/${listingId}/inventory`, { query: { legacy: false } });
  const srcProducts = Array.isArray(inv.products) ? inv.products : [];
  if (!srcProducts.length) {
    throw new Error("Listing has no products (invalid state): createDraftListing did not initialize inventory");
  }

  // Money object on GET → decimal on PUT. updateListingInventory.js does
  // the same dance — that's the closest reference for this conversion.
  const toDecimal = (price) => {
    if (price == null) return Number(priceUsd);
    if (typeof price === "object" && price.amount != null) {
      const div = Number(price.divisor || 100);
      return Number(price.amount) / (div > 0 ? div : 100);
    }
    const n = Number(price);
    return Number.isFinite(n) ? n : Number(priceUsd);
  };

  const products = srcProducts.map(p => {
    const offerings = (Array.isArray(p.offerings) && p.offerings.length ? p.offerings : [{}]).map(o => {
      const decimal = toDecimal(o.price);
      const offering = {
        price      : Number(decimal.toFixed(2)),
        quantity   : 1,
        is_enabled : true
      };
      if (readinessStateId != null) offering.readiness_state_id = Number(readinessStateId);
      return offering;
    });

    return {
      sku            : sku,
      property_values: [],   // custom listings have no variations
      offerings
    };
  });

  await etsyFetch(`/listings/${listingId}/inventory`, {
    method: "PUT",
    query : { legacy: false },
    body  : { products }
  });
}

// ─── 7. Publish the listing ──────────────────────────────────────────────

async function publishListing(listingId) {
  // PATCH /shops/{shop_id}/listings/{id} expects x-www-form-urlencoded.
  // The shared etsyFetch() helper sends JSON, so we go direct here.
  // (Same approach used by updateListing.js in the reference project.)
  const accessToken = await getValidEtsyAccessToken();
  const url =
    `https://api.etsy.com/v3/application/shops/${SHOP_ID}/listings/${encodeURIComponent(listingId)}`;

  const form = new URLSearchParams();
  form.append("state", "active");

  const res = await fetch(url, {
    method : "PATCH",
    headers: {
      Authorization : `Bearer ${accessToken}`,
      "x-api-key"   : `${process.env.CLIENT_ID}:${process.env.CLIENT_SECRET || process.env.ETSY_SHARED_SECRET}`,
      "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
      Accept        : "application/json"
    },
    body: form.toString()
  });

  const text = await res.text();
  let data;
  try { data = JSON.parse(text); } catch { data = { raw: text }; }

  if (!res.ok) {
    throw new Error(`publishListing ${res.status}: ${text.slice(0, 400)}`);
  }

  // The PATCH response usually includes `url`. If for some reason it doesn't,
  // do one extra GET — better than failing the whole flow on a missing field.
  if (!data.url) {
    try {
      const fresh = await etsyFetch(`/listings/${listingId}`);
      if (fresh && fresh.url) data.url = fresh.url;
    } catch (e) {
      console.warn("[listingCreator] post-publish GET to recover url failed:", e.message);
    }
  }

  if (!data.url) {
    throw new Error(`Published listing has no url field (invalid response): ${JSON.stringify(data).slice(0, 200)}`);
  }
  return data;
}

// ─── 8. Send the URL to the customer (via the existing send pipeline) ───

function buildListingDeliveryMessage(family, listingUrl) {
  // Voice rules per CUSTOM_LISTING_AUTOMATION_SPEC.md §6.7 / sales.md:
  //   - "we" not "I"
  //   - no em / en dashes
  //   - no specific timeframes
  //   - no service-script clichés ("absolutely", "happy to", "lock it in")
  //   - plain text, no markdown
  const f = ALLOWED_FAMILIES.includes(family) ? family : "custom";
  return (
    `Here's the custom listing for your ${f}: ${listingUrl}

Once you check out, we'll get to work on your order. Let us know if you have any questions.`
  );
}

async function sendListingUrlToCustomer({ threadId, etsyConversationUrl, listingUrl, family, listingId }) {
  const text = buildListingDeliveryMessage(family, listingUrl);

  const res = await fetch(`${functionsBase()}/.netlify/functions/etsyMailDraftSend`, {
    method : "POST",
    headers: {
      "Content-Type": "application/json",
      // Forward the shared secret if it's set. enqueue itself doesn't
      // require it (per etsyMailDraftSend.js), but other ops do, and
      // including it is harmless.
      ...(process.env.ETSYMAIL_EXTENSION_SECRET
        ? { "X-EtsyMail-Secret": process.env.ETSYMAIL_EXTENSION_SECRET }
        : {})
    },
    body: JSON.stringify({
      op                  : "enqueue",
      threadId,
      etsyConversationUrl,
      text,
      employeeName        : "system:listing-creator",
      // The message text is a static template (built from buildListingDeliveryMessage),
      // not LLM output, so generatedByAI:false is the correct semantic.
      // The bg fn DOES use Claude for title/description/tags upstream, but
      // that's listing content — separate from the customer-facing message.
      // Recording the model anyway is useful forensics for "which deploy
      // generated this listing's content".
      aiMeta              : {
        generatedByAI: false,
        model        : AI_MODEL,
        source       : "listing_creator",
        listingId    : String(listingId),
        listingUrl
      },
      force               : true   // overwrite any prior draft on this thread
    })
  });

  const responseText = await res.text();
  if (!res.ok) {
    throw new Error(`enqueue send failed (${res.status}): ${responseText.slice(0, 300)}`);
  }
}

// ─── 9. Idempotency markers ──────────────────────────────────────────────

async function markSuccess({ threadId, listingId, listingUrl, generated, imagesUploaded, salesSynopsis, isResume }) {
  const threadRef = db.collection(THREADS_COLL).doc(threadId);

  await threadRef.update({
    // Listing-pipeline fields
    customListingId         : String(listingId),
    customListingUrl        : listingUrl,
    customListingCreatedAt  : FV.serverTimestamp(),
    customListingStatus     : "created",
    customListingError      : FV.delete(),
    customListingErrorAt    : FV.delete(),
    customListingErrorCount : FV.delete(),

    // v4.3 — TERMINAL SALES STATUS. The sales agent treats "sales_completed"
    // as a terminal status (TERMINAL_THREAD_STATUSES in the agent code) and
    // skips processing on threads in this state. The dashboard's "Completed
    // Sales" menu queries threads where status == "sales_completed".
    status                  : "sales_completed",
    salesCompletedAt        : FV.serverTimestamp(),

    // v4.3 — Operator-facing synopsis. Generated at completion so the next
    // person reviewing the thread sees the full sale at a glance instead of
    // scrolling the entire conversation. Falls back to a structured summary
    // if the Claude call fails — markSuccess never blocks on synopsis.
    salesSynopsis           : salesSynopsis,

    needsOperatorReview     : false,
    needsOperatorReviewReason: null,
    updatedAt               : FV.serverTimestamp()
  });

  // v4.3 — RESET SalesContext.stage so a future inbound on this thread
  // (e.g. "thanks, when will it ship?") doesn't re-route to the sales
  // agent. The autoPipeline's path (a) STATEFUL keys off
  // ACTIVE_SALES_STAGES = {discovery, spec, quote, revision,
  // pending_close_approval}. We set stage to "completed" — outside that
  // set — so loadActiveSalesContextStage returns null, and the follow-up
  // falls through to the regular customer-service draft pipeline. The
  // rest of SalesContext (accumulated spec, quote history) is preserved
  // for forensic value.
  //
  // Wrapped in try/catch because a SalesContext write failure shouldn't
  // unwind the whole completion — the listing is live, the customer has
  // the URL, the synopsis is saved. At worst the operator sees one stray
  // sales-agent reply on a follow-up.
  try {
    await db.collection(SALES_COLL).doc(threadId).set({
      stage           : "completed",
      stageCompletedAt: FV.serverTimestamp(),
      completionReason: "sale_closed",
      completedListingId: String(listingId),
      updatedAt       : FV.serverTimestamp()
    }, { merge: true });
  } catch (e) {
    console.warn(`[listingCreator] SalesContext reset failed for ${threadId} (non-fatal):`, e.message);
  }

  await db.collection(AUDIT_COLL).add({
    threadId,
    eventType : "custom_listing_created",
    actor     : "listing-creator",
    payload   : {
      listingId      : String(listingId),
      listingUrl,
      titlePreview   : generated ? clampStr(generated.title, 140) : "(resumed — no fresh title)",
      imagesUploaded : imagesUploaded.uploaded,
      imagesAttempted: imagesUploaded.attempted,
      usedTemplateImageFallback: imagesUploaded.usedFallback,
      resumed        : !!isResume,
      synopsisChars  : (salesSynopsis || "").length
    },
    createdAt: FV.serverTimestamp()
  }).catch(e => console.warn("[listingCreator] audit write failed (non-fatal):", e.message));
}

// ─── 9b. Mid-flow persistence (resumability across crashes) ─────────────

/** Persist customListingId IMMEDIATELY after Etsy creates the draft. The
 *  reason: every subsequent step (image upload, inventory, publish, send)
 *  can take seconds-to-minutes, and any crash in that window leaves a
 *  draft listing on Etsy that the next retry can't match to its source
 *  thread. Once this write lands, the worker's resume path on the next
 *  attempt sees customListingId populated and skips createDraftListing.
 *
 *  Trade-off: if THIS Firestore write fails, we still have an orphan
 *  draft on Etsy (the listing exists, the ID isn't saved). That's the
 *  same failure mode we had before this fix — no regression. The win
 *  is that for the much more common Etsy-API-call-fails-mid-flow case,
 *  we no longer create a second listing on retry. */
async function persistListingIdEarly(threadId, listingId) {
  await db.collection(THREADS_COLL).doc(threadId).update({
    customListingId            : String(listingId),
    customListingDraftCreatedAt: FV.serverTimestamp(),
    updatedAt                  : FV.serverTimestamp()
  });
}

/** Persist that image upload completed. Used by the resume path so a
 *  retry doesn't re-upload duplicates onto an existing draft listing. */
async function persistImagesUploaded(threadId, imagesUploaded) {
  await db.collection(THREADS_COLL).doc(threadId).update({
    customListingImagesAt     : FV.serverTimestamp(),
    customListingImagesCount  : Number(imagesUploaded.uploaded || 0),
    customListingUsedFallback : !!imagesUploaded.usedFallback,
    updatedAt                 : FV.serverTimestamp()
  });
}

// ─── 9c. Sales synopsis (operator-facing summary at completion) ─────────

const SYNOPSIS_SYSTEM_PROMPT =
`You are writing a brief synopsis of a customer-service conversation for the next employee who needs to review this thread. The sale just completed and the listing was sent to the customer.

Cover, in this order:
1. Who the customer is and what they wanted.
2. Key clarifications, negotiation points, or unusual requests during the conversation.
3. What was finalized: the spec accepted and the price.
4. Anything notable for follow-up: special instructions, customer sentiment, edge cases the next person should know about.

Length: 150 to 300 words. Plain text, no markdown, no bullet points, no headers. Third person. Matter-of-fact. No em-dashes or en-dashes. No service-script clichés ("happy to", "absolutely", "lock it in"). Refer to the shop as "we", not "I". No specific shipping or delivery timeframes.

Output the synopsis text only, with no preamble.`;

async function generateSalesSynopsis({ thread, family, listingId, listingUrl, fullThreadContext }) {
  const customer = thread.customerName || thread.buyerName || thread.etsyUsername || "the customer";
  const userPrompt =
`Customer: ${customer}
Etsy username: ${thread.etsyUsername || "n/a"}
Conversation subject: ${thread.subject || "n/a"}

Product family: ${family}
Final accepted price (USD): ${thread.acceptedQuoteUsd}
Listing created: ${listingUrl} (id ${listingId})
Reference photos provided: ${(thread.customListingImagesCount != null) ? thread.customListingImagesCount : "n/a"}

Accepted spec (structured):
${JSON.stringify(thread.lastResolverResult || {}, null, 2)}

Full conversation (oldest first, last 30 messages):
${fullThreadContext.map(m => `[${m.direction}] ${m.text}`).join("\n")}

Write the synopsis now.`;

  try {
    const resp = await callClaudeRaw({
      model      : AI_MODEL,
      maxTokens  : 800,
      system     : SYNOPSIS_SYSTEM_PROMPT,
      messages   : [{ role: "user", content: userPrompt }],
      useThinking: false
    });
    const text = extractTextFromClaudeResponse(resp).trim();
    if (text) return text;
  } catch (e) {
    console.warn("[listingCreator] synopsis generation failed:", e.message);
  }

  // Fallback: structured summary from known fields. We never want a missing
  // synopsis to block markSuccess — operator visibility is nice-to-have.
  const lr = thread.lastResolverResult || {};
  const lineSummary = Array.isArray(lr.lineItems)
    ? lr.lineItems.map(li => `${li.qty || 1}x ${li.name || li.description || JSON.stringify(li)}`).join("; ")
    : "(see lastResolverResult)";
  return (
    `Sale completed for ${customer} (Etsy: ${thread.etsyUsername || "n/a"}). ` +
    `Family: ${family}. Final price: $${thread.acceptedQuoteUsd}. ` +
    `Spec: ${lineSummary}. ` +
    `Listing: ${listingUrl}. ` +
    `Reference photos: ${thread.customListingImagesCount != null ? thread.customListingImagesCount : "unknown"}. ` +
    `(Auto-generated synopsis fallback — Claude synopsis call failed; review the full thread for context.)`
  );
}

/** Wider context for the synopsis (last 30 messages, oldest first).
 *  Distinct from loadThreadContext, which only pulls 10 — that's tuned
 *  for the listing-content prompt, not a complete conversation summary. */
async function loadFullThreadContext(threadId) {
  const snap = await db.collection(`${THREADS_COLL}/${threadId}/messages`)
    .orderBy("timestamp", "desc")
    .limit(30)
    .get();
  return snap.docs.map(d => {
    const m = d.data();
    return {
      direction: m.direction || "unknown",
      text     : clampStr(m.text, 800)
    };
  }).reverse();
}

// ─── 10. Failure tracking ────────────────────────────────────────────────

function isTerminalError(err) {
  const msg = String((err && err.message) || err).toLowerCase();
  // Retryable: transient network and rate-limit signals
  if (msg.includes("etimedout") || msg.includes("econnreset") || msg.includes("enotfound")) return false;
  if (msg.includes("rate limit") || msg.includes(" 429") || msg.includes("429:")) return false;
  if (msg.includes(" 502") || msg.includes(" 503") || msg.includes(" 504")) return false;
  // Terminal: validation, bad input, auth, missing config
  if (msg.includes("invalid input") || msg.includes("invalid setup") || msg.includes("invalid response")) return true;
  if (msg.includes("invalid state")) return true;
  if (msg.includes(" 401") || msg.includes(" 403") || msg.includes("not found")) return true;
  // Default: terminal — better to escalate than burn API quota in a loop.
  return true;
}

async function markFailure({ threadId, err }) {
  const errMsg = clampStr((err && err.message) || err, 500);
  const terminal = isTerminalError(err);
  console.error(`[listingCreator] failed ${threadId}: ${errMsg}`, err && err.stack ? err.stack.split("\n").slice(0, 5).join("\n") : "");

  try {
    await db.collection(THREADS_COLL).doc(threadId).update({
      // "queued" → eligible for cron retry on the next tick.
      // "failed" → stuck pending operator (needsOperatorReview=true).
      // We don't use null for retryable — the cron's primary query keys
      // off customListingStatus == "queued", and using null would orphan
      // the thread out of the indexed query path.
      customListingStatus      : terminal ? "failed" : "queued",
      customListingError       : errMsg,
      customListingErrorAt     : FV.serverTimestamp(),
      customListingErrorCount  : FV.increment(1),

      // v4.3 — CRITICAL: clear customListingStartedAt so the cron's
      // tryClaim can distinguish "worker exited with error" from
      // "worker still running". Without this clear, a re-acceptance
      // turn that overwrites customListingStatus from "creating" back
      // to "queued" while the worker is mid-flow would race with the
      // cron's claim — see SETUP.md "race protection". With this
      // clear, a fresh startedAt unambiguously means "worker still
      // in-flight", and the cron's "queued"-path freshness check
      // safely skips.
      customListingStartedAt   : FV.delete(),

      needsOperatorReview      : terminal,
      needsOperatorReviewReason: terminal ? `listing_creation_failed: ${errMsg}` : null,
      updatedAt                : FV.serverTimestamp()
    });
  } catch (e) {
    console.error("[listingCreator] markFailure write failed:", e.message);
  }

  try {
    await db.collection(AUDIT_COLL).add({
      threadId,
      eventType: "custom_listing_failed",
      actor    : "listing-creator",
      payload  : { error: errMsg, terminal },
      createdAt: FV.serverTimestamp()
    });
  } catch (e) {
    console.warn("[listingCreator] audit write failed (non-fatal):", e.message);
  }
}

// ─── handler ─────────────────────────────────────────────────────────────

exports.handler = async function (event) {
  const tStart = Date.now();
  let threadId = null;

  // CORS preflight (consistency with the rest of the codebase — even
  // though browsers shouldn't be hitting this endpoint, the Chrome
  // extension and inbox UI assume CORS is enabled on every fn).
  if (event && event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers: CORS, body: "ok" };
  }

  // Auth: require the shared extension secret. The cron forwards it via
  // X-EtsyMail-Secret. Operators invoking manually for debug also need
  // it. Without this, anyone who knows or guesses a thread id could
  // POST here and force-trigger listing creation. requireExtensionAuth
  // fails closed in production but warns-only in dev/preview.
  const auth = requireExtensionAuth(event || {});
  if (!auth.ok) return auth.response;

  try {
    const body = event && event.body ? JSON.parse(event.body) : {};
    threadId = String(body.threadId || "").trim();
    if (!threadId || !/^etsy_conv_\d+$/.test(threadId)) {
      return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: "Invalid threadId" }) };
    }

    // 1. Load thread + draft + family
    const { thread, referenceAttachments, family } = await loadThreadData(threadId);

    const attempts = Number(thread.customListingAttempts || 0);
    if (attempts > 1) {
      console.warn(
        `[listingCreator] Retry detected for ${threadId} (attempt #${attempts}, ` +
        `errorCount=${Number(thread.customListingErrorCount || 0)}).`
      );
    }

    // 2. TERMINAL idempotency check. If the thread is already in the
    //    "sales_completed" terminal status, the listing is fully done.
    //    Bail safely (defense in depth — cron's tryClaim should have
    //    cleaned up before firing us, but if we got here anyway, don't
    //    re-process). customListingStatus="created" is the same signal
    //    in the listing-pipeline state machine.
    if (thread.status === "sales_completed" || thread.customListingStatus === "created") {
      console.log(`[listingCreator] thread ${threadId} already terminal (status=${thread.status}, customListingStatus=${thread.customListingStatus}), bailing.`);
      // Best-effort: align customListingStatus to "created" if it drifted.
      if (thread.customListingStatus !== "created") {
        await db.collection(THREADS_COLL).doc(threadId).update({
          customListingStatus: "created",
          updatedAt          : FV.serverTimestamp()
        }).catch(() => { /* non-fatal */ });
      }
      return {
        statusCode: 200,
        headers   : CORS,
        body: JSON.stringify({ ok: true, alreadyCreated: true, listingId: thread.customListingId || null })
      };
    }

    // 3. RESUME detection. v4.3 — customListingId is now persisted right
    //    after createDraftListing succeeds, so a retry can resume from
    //    where it left off instead of creating a duplicate listing.
    //
    //    State signals:
    //      thread.customListingId          → step 5 (createDraftListing) done
    //      thread.customListingImagesAt    → step 6 (uploadReferenceImages) done
    //
    //    Steps 7 (inventory PUT), 8 (publish PATCH), and 9 (enqueue send)
    //    are idempotent on Etsy's side (PUT replaces, PATCH state=active
    //    is no-op if already active, enqueue uses deterministic draftId
    //    with force=true). So we always run those — no resume sentinel
    //    needed for them.
    const resumeListingId = thread.customListingId ? String(thread.customListingId) : null;
    const resumeImagesAt  = thread.customListingImagesAt || null;
    const isResume        = !!resumeListingId;

    if (isResume) {
      console.warn(
        `[listingCreator] Resuming for ${threadId}: ` +
        `listingId=${resumeListingId}, imagesAlreadyUploaded=${!!resumeImagesAt}. ` +
        `Skipping createDraftListing${resumeImagesAt ? " and uploadReferenceImages" : ""}.`
      );
    }

    // 4. Resolve template (always — needed for inventory readiness_state_id
    //    and as the image fallback source).
    const templateListingId = await resolveTemplateListingId(family);
    const template = await readTemplateListing(templateListingId);

    // 5. Generate AI listing content + create draft (skipped on resume).
    let newListingId, generated;
    if (isResume) {
      newListingId = resumeListingId;
      generated    = null;   // not needed downstream — only the title was used (for audit)
    } else {
      const threadContext = await loadThreadContext(threadId);
      generated = await generateListingContent({
        family,
        lastResolverResult: thread.lastResolverResult,
        threadContext
      });
      newListingId = await createDraftListing({
        title      : generated.title,
        description: generated.description,
        tags       : generated.tags,
        priceUsd   : thread.acceptedQuoteUsd,
        template
      });
      console.log(`[listingCreator] draft created for ${threadId}: listing=${newListingId}`);

      // CRITICAL — persist the listing id immediately. If anything below
      // fails or crashes, the next retry sees customListingId set and
      // resumes instead of creating a duplicate.
      await persistListingIdEarly(threadId, newListingId);
    }

    // 6. Upload reference photos (skipped on resume if already done).
    let imagesUploaded;
    if (resumeImagesAt) {
      imagesUploaded = {
        uploaded    : Number(thread.customListingImagesCount || 0),
        attempted   : Number(thread.customListingImagesCount || 0),
        usedFallback: !!thread.customListingUsedFallback,
        resumed     : true
      };
    } else {
      imagesUploaded = await uploadReferenceImages(
        newListingId,
        referenceAttachments,
        templateListingId
      );
      // Mark images done so a later crash doesn't re-upload duplicates.
      await persistImagesUploaded(threadId, imagesUploaded);
    }

    // 7. Set inventory (idempotent — always run).
    const sku = buildSku(thread, threadId);
    let readinessStateId = template.readinessStateId;
    if (!readinessStateId) readinessStateId = await resolveReadinessStateIdFallback();
    await setInventory(newListingId, {
      priceUsd        : thread.acceptedQuoteUsd,
      readinessStateId,
      sku
    });

    // 8. Publish (idempotent — PATCH state=active is a no-op if already active).
    const published   = await publishListing(newListingId);
    const listingUrl  = published.url;
    console.log(`[listingCreator] published ${threadId}: ${listingUrl}`);

    // 9. Send the URL to the customer (idempotent on draftId+force=true).
    await sendListingUrlToCustomer({
      threadId,
      etsyConversationUrl: thread.etsyConversationUrl,
      listingUrl,
      family,
      listingId: newListingId
    });

    // 9b. Generate the sales synopsis. Re-load the thread to pick up the
    //     customListingImagesCount we just wrote, so the synopsis includes
    //     accurate reference-photo counts even on a resume path.
    const freshThread = (await db.collection(THREADS_COLL).doc(threadId).get()).data() || thread;
    const fullThreadContext = await loadFullThreadContext(threadId);
    const salesSynopsis = await generateSalesSynopsis({
      thread: freshThread, family, listingId: newListingId, listingUrl, fullThreadContext
    });

    // 10. Success markers + audit. This write also flips thread.status to
    //     "sales_completed" — a terminal status the sales agent honors,
    //     and the dashboard's "Completed Sales" menu filters on.
    await markSuccess({
      threadId,
      listingId     : newListingId,
      listingUrl,
      generated,
      imagesUploaded,
      salesSynopsis,
      isResume
    });

    return {
      statusCode: 200,
      headers   : CORS,
      body: JSON.stringify({
        ok        : true,
        threadId,
        listingId : String(newListingId),
        listingUrl,
        resumed   : isResume,
        synopsisChars: (salesSynopsis || "").length,
        elapsedMs : Date.now() - tStart
      })
    };

  } catch (err) {
    if (threadId) await markFailure({ threadId, err });
    return {
      statusCode: 500,
      headers   : CORS,
      body: JSON.stringify({ ok: false, error: clampStr(err.message, 500), threadId })
    };
  }
};
