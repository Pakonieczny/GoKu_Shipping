// netlify/functions/etsyUpdateListingInventoryProxy.js
// Listing inventory writer. Two modes, selected by request body:
//
// LEGACY MODE — body: { listing_id, items:[{product_id, sku}] }
//   Unchanged behavior for the SKU console: applies Variant 1's SKU to all
//   variants and PUTs the document back. Do not remove.
//
// PRICING CONSOLE MODE — body: {
//   listing_id, inventory, expected_snapshot_hash?, auto_on_property? }
//
//   Safe, verified, reversible write sequence:
//     1. VALIDATE the draft server-side (structure, prices, quantities,
//        duplicates, Etsy variation limits). Reject with the exact problem.
//     2. READ the live inventory and compute its canonical snapshot hash.
//        If expected_snapshot_hash is present and differs → 409
//        { code: "STALE_INVENTORY" } and NOTHING is written. This prevents
//        clobbering edits made in Etsy's own UI since the console loaded.
//     3. PUT the sanitized inventory with *_on_property maps derived from the
//        matrix (auto, default) or taken from the payload (manual). Etsy's
//        exact error status + body are passed through on failure.
//     4. READ BACK the live inventory and verify every combination's price,
//        quantity, SKU and enabled state against what was intended. The
//        response reports verified:true/false with exact differences.
//     5. REVERSIBILITY: the response includes previous_inventory (the full
//        pre-write document) and previous_snapshot_hash so the operator can
//        restore the prior state by re-submitting it through this same
//        endpoint.
//
//   Success response: { ok, verified, listing_id, previous_inventory,
//     previous_snapshot_hash, fresh: { inventory, snapshot_hash,
//     pricing_health, fetched_at } }

const { etsyFetch } = require("./etsyRateLimiter");
const {
  toDecimalPrice, snapshotHash, pricingHealth,
  deriveOnProperty, verifyAgainst, comboKey, productPropertyOrder
} = require("./_etsyInventoryCanonical");

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type,Authorization,Access-Token,access-token",
  "Access-Control-Allow-Methods": "POST,OPTIONS"
};

// Inventories returned to the console must carry decimal prices (the editor
// edits plain numbers); Etsy GET returns Money objects.
function decimalizeInventory(inv) {
  return {
    ...inv,
    products: (inv.products || []).map(p => ({
      ...p,
      offerings: (p.offerings || []).map(o => ({ ...o, price: toDecimalPrice(o.price) }))
    }))
  };
}

const MAX_PROPERTIES = 2;      // Etsy live-listing limit
const MAX_OPTIONS_PER_PROP = 70; // Etsy per-variation option limit


// Etsy requires "keystring:shared_secret" in x-api-key for these endpoints
// when a shared secret exists (same pattern as etsyShopListingsProxy).
function apiKey() {
  const clientId = process.env.CLIENT_ID;
  const secret = process.env.CLIENT_SECRET;
  if (!clientId) return null;
  return secret ? `${clientId}:${secret}` : clientId;
}

function json(statusCode, body) {
  return { statusCode, headers: CORS, body: JSON.stringify(body) };
}

async function parseJson(resp) {
  const text = await resp.text();
  if (!text) return {};
  try { return JSON.parse(text); }
  catch { return { error: text.slice(0, 1000) }; }
}

function etsyHeaders(accessToken, clientId) {
  return {
    Authorization: `Bearer ${accessToken}`,
    "x-api-key": clientId,
    "Content-Type": "application/json"
  };
}

async function getInventory(listingId, headers) {
  const url = `https://openapi.etsy.com/v3/application/listings/${listingId}/inventory`;
  const resp = await etsyFetch(url, { headers }, { bucket: "etsy-listing-console" });
  const payload = await parseJson(resp);
  return { resp, payload };
}

/* ---------- Server-side draft validation (exact, actionable messages) ---------- */

function validateDraft(inventory) {
  const products = Array.isArray(inventory?.products) ? inventory.products : [];
  if (!products.length) return "The draft contains no product combinations.";

  const propOptions = new Map(); // property_id -> Set(values)
  const seen = new Set();

  for (let i = 0; i < products.length; i++) {
    const p = products[i];
    const key = comboKey(p) || "base product";
    if (seen.has(key)) return "Duplicate combination in draft: " + key;
    seen.add(key);

    const offerings = Array.isArray(p.offerings) ? p.offerings : [];
    if (!offerings.length) return "Combination " + key + " has no offering.";
    const o = offerings[0];

    const price = toDecimalPrice(o.price);
    if (!Number.isFinite(price) || price < 0.2) {
      return "Combination " + key + " has an invalid price (" + o.price + "). Etsy requires at least $0.20.";
    }
    const qty = Number(o.quantity);
    if (!Number.isInteger(qty) || qty < 0) {
      return "Combination " + key + " has an invalid quantity (" + o.quantity + ").";
    }
    if (String(p.sku || "").length > 32) {
      return "Combination " + key + " has a SKU longer than Etsy's 32-character limit.";
    }

    for (const v of (p.property_values || [])) {
      const id = Number(v.property_id);
      if (!Number.isFinite(id)) return "Combination " + key + " has a property without a numeric property_id.";
      if (!Array.isArray(v.values) || !v.values.length) {
        return "Combination " + key + " has a property with no value.";
      }
      if (!propOptions.has(id)) propOptions.set(id, new Set());
      for (const val of v.values) propOptions.get(id).add(String(val));
    }
  }

  if (propOptions.size > MAX_PROPERTIES) {
    return "Draft uses " + propOptions.size + " variation dropdowns; live Etsy listings support at most " + MAX_PROPERTIES + ".";
  }
  for (const [id, values] of propOptions) {
    if (values.size > MAX_OPTIONS_PER_PROP) {
      return "Variation " + id + " has " + values.size + " options; Etsy allows at most " + MAX_OPTIONS_PER_PROP + ".";
    }
  }

  const enabled = products.filter(p => (p.offerings?.[0]?.is_enabled) !== false).length;
  if (!enabled) return "Every offering in the draft is disabled; Etsy requires at least one enabled offering.";

  return null;
}

/* ---------- PUT body sanitization ---------- */

function sanitizeForPut(inventory, onProperty, fallbackReadinessId) {
  const products = (inventory.products || []).map(p => ({
    sku: String(p.sku || "").trim(),
    property_values: (p.property_values || []).map(v => {
      const values = (v.values || []).map(String);
      const valueIds = Array.isArray(v.value_ids) && v.value_ids.length === values.length
        ? v.value_ids
        : [];
      return {
        property_id: Number(v.property_id),
        property_name: v.property_name != null ? String(v.property_name) : undefined,
        scale_id: v.scale_id ?? null,
        value_ids: valueIds,
        values
      };
    }),
    offerings: (p.offerings || []).slice(0, 1).map(o => {
      const enabled = o.is_enabled !== false;
      const out = {
        price: toDecimalPrice(o.price),
        // Etsy's canonical "unavailable" representation is quantity 0 +
        // disabled; with stock > 0 Etsy force-enables the offering, which
        // makes is_enabled:false silently fail. Zero the stock on every
        // disabled offering so the disable actually holds.
        quantity: enabled ? Number(o.quantity ?? o.available_quantity ?? 0) : 0,
        is_enabled: enabled
      };
      // Etsy requires a readiness (processing) state on every offering.
      // Keep the offering's own; new rows inherit the listing's profile.
      const rid = o.readiness_state_id ?? fallbackReadinessId;
      if (rid != null) out.readiness_state_id = Number(rid);
      return out;
    })
  }));

  return {
    products,
    price_on_property: onProperty.price_on_property,
    quantity_on_property: onProperty.quantity_on_property,
    sku_on_property: onProperty.sku_on_property
  };
}

/* ---------- Legacy mode (SKU console) ---------- */

async function legacySkuUpdate(listingId, items, headers) {
  const { resp: getResp, payload: inv } = await getInventory(listingId, headers);
  if (!getResp.ok) return json(getResp.status, inv);

  const list = Array.isArray(inv.products) ? inv.products : [];
  if (!list.length) return json(400, { error: "No variants found for this listing" });

  const targetPid = Number(list[0].product_id);
  const requested = items.find(i => Number(i.product_id) === targetPid);
  if (!requested) {
    return json(400, { error: "Missing Variant 1 in items (product_id of first variant required)" });
  }
  const newSku = String(requested.sku || "").trim();

  const products = list.map(p => ({
    sku: newSku,
    property_values: Array.isArray(p.property_values) ? p.property_values.map(v => ({
      property_id: v.property_id,
      property_name: v.property_name ?? undefined,
      scale_id: v.scale_id ?? null,
      value_ids: Array.isArray(v.value_ids) ? v.value_ids : [],
      values: Array.isArray(v.values) ? v.values : []
    })) : [],
    offerings: Array.isArray(p.offerings) ? p.offerings.map(o => {
      const out = {
        quantity: Number(o.quantity ?? o.available_quantity ?? 0),
        is_enabled: o.is_enabled !== false,
        price: toDecimalPrice(o.price)
      };
      if (o.readiness_state_id != null) out.readiness_state_id = Number(o.readiness_state_id);
      return out;
    }) : []
  }));

  const putUrl = `https://openapi.etsy.com/v3/application/listings/${listingId}/inventory`;
  const putResp = await etsyFetch(putUrl, {
    method: "PUT",
    headers,
    body: JSON.stringify({
      products,
      price_on_property: inv.price_on_property || [],
      quantity_on_property: inv.quantity_on_property || [],
      sku_on_property: inv.sku_on_property || []
    })
  }, { bucket: "etsy-listing-console" });
  const result = await parseJson(putResp);
  if (!putResp.ok) return json(putResp.status, result);

  return json(200, { ok: true, listing_id: listingId });
}

/* ---------- Handler ---------- */

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 200, headers: CORS, body: "ok" };
  if (event.httpMethod !== "POST") return json(405, { error: "Method not allowed" });

  try {
    const accessToken = event.headers["access-token"] || event.headers["Access-Token"];
    const clientId = apiKey();
    if (!accessToken) return json(400, { error: "Missing access token" });
    if (!clientId) return json(500, { error: "Missing CLIENT_ID" });

    let body;
    try { body = JSON.parse(event.body || "{}"); }
    catch { return json(400, { error: "Request body is not valid JSON." }); }

    const listingId = String(body.listing_id || "").trim();
    if (!/^\d+$/.test(listingId)) return json(400, { error: "Missing or invalid listing_id" });

    const headers = etsyHeaders(accessToken, clientId);

    // ---- Legacy SKU-console mode
    if (Array.isArray(body.items)) {
      if (!body.items.length) return json(400, { error: "No items provided" });
      return legacySkuUpdate(listingId, body.items, headers);
    }

    // ---- Pricing-console mode
    const inventory = body.inventory;
    if (!inventory || typeof inventory !== "object") {
      return json(400, { error: "Missing inventory payload (or legacy items array)." });
    }

    // 1) Validate before touching Etsy.
    const problem = validateDraft(inventory);
    if (problem) return json(400, { error: problem, code: "INVALID_DRAFT" });

    // 2) Staleness check against the live document.
    const { resp: preResp, payload: preInv } = await getInventory(listingId, headers);
    if (!preResp.ok) return json(preResp.status, preInv);

    const previousInventory = {
      products: preInv.products || [],
      price_on_property: preInv.price_on_property || [],
      quantity_on_property: preInv.quantity_on_property || [],
      sku_on_property: preInv.sku_on_property || []
    };
    const previousHash = snapshotHash(previousInventory);
    const expected = String(body.expected_snapshot_hash || "").trim();
    if (expected && expected !== previousHash) {
      return json(409, {
        error: "The live Etsy inventory changed after this listing was loaded. Nothing was written.",
        code: "STALE_INVENTORY",
        current_snapshot_hash: previousHash
      });
    }

    // 3) Dependency maps: derive from the matrix (default) or honor manual maps.
    const auto = body.auto_on_property !== false;
    const onProperty = auto ? deriveOnProperty(inventory) : {
      price_on_property: (inventory.price_on_property || []).map(Number),
      quantity_on_property: (inventory.quantity_on_property || []).map(Number),
      sku_on_property: (inventory.sku_on_property || []).map(Number)
    };
    // Etsy requires these arrays in the same order the properties appear in
    // the products. Enforce for both derived and manual maps.
    const propOrder = productPropertyOrder(inventory);
    const inProductOrder = ids => propOrder.filter(id => ids.includes(id));
    onProperty.price_on_property = inProductOrder(onProperty.price_on_property);
    onProperty.quantity_on_property = inProductOrder(onProperty.quantity_on_property);
    onProperty.sku_on_property = inProductOrder(onProperty.sku_on_property);

    // Most common readiness_state_id on the live listing — inherited by any
    // draft row (e.g. newly added combinations) that doesn't carry one.
    const readinessCounts = new Map();
    for (const p of previousInventory.products) {
      const rid = p?.offerings?.[0]?.readiness_state_id;
      if (rid != null) readinessCounts.set(rid, (readinessCounts.get(rid) || 0) + 1);
    }
    const fallbackReadinessId = [...readinessCounts.entries()].sort((a, b) => b[1] - a[1])[0]?.[0] ?? null;

    const putBody = sanitizeForPut(inventory, onProperty, fallbackReadinessId);
    const putUrl = `https://openapi.etsy.com/v3/application/listings/${listingId}/inventory`;
    const putResp = await etsyFetch(putUrl, {
      method: "PUT",
      headers,
      body: JSON.stringify(putBody)
    }, { bucket: "etsy-listing-console" });
    const putResult = await parseJson(putResp);
    if (!putResp.ok) {
      // Surface Etsy's exact rejection; nothing partially applied per Etsy semantics.
      return json(putResp.status, {
        error: putResult.error || "Etsy rejected the inventory update.",
        etsy_status: putResp.status,
        etsy_response: putResult,
        code: "ETSY_PUT_REJECTED"
      });
    }

    // 4) Read back and verify.
    const { resp: postResp, payload: postInv } = await getInventory(listingId, headers);
    if (!postResp.ok) {
      return json(200, {
        ok: true,
        verified: false,
        verification_error: "Etsy accepted the update, but the verification read failed (HTTP " + postResp.status + "). Refresh the listing to confirm the live state.",
        listing_id: Number(listingId),
        previous_inventory: decimalizeInventory(previousInventory),
        previous_snapshot_hash: previousHash
      });
    }
    let freshInventory = {
      products: postInv.products || [],
      price_on_property: postInv.price_on_property || [],
      quantity_on_property: postInv.quantity_on_property || [],
      sku_on_property: postInv.sku_on_property || []
    };
    let check = verifyAgainst({ ...putBody }, freshInventory);
    let autoCorrected = false;

    // Known Etsy behavior: is_enabled=false often does not take effect on
    // offerings created for the first time in the same PUT that creates
    // them — it only sticks once the combination already exists with a
    // real product_id/offering_id. If EVERY mismatch after the first write
    // is purely an enabled-state mismatch (never price/quantity/SKU), the
    // combinations now exist, so one corrective PUT targeting their live
    // state should apply the disable/enable correctly. This never
    // fabricates data — it resubmits the same operator-approved draft.
    const onlyEnabledMismatches = check.differences.length > 0 &&
      check.detail.every(d => d.field === "is_enabled");

    if (!check.verified && onlyEnabledMismatches) {
      // Precondition guarantees price/quantity/SKU already match, so the
      // safe corrective action is to resend the EXACT same body \u2014 no
      // reconstruction, no risk of corrupting a field that was already
      // correct. Only the target-side state (now-existing product IDs)
      // differs between this call and the first.
      const correctivePutResp = await etsyFetch(putUrl, {
        method: "PUT",
        headers,
        body: JSON.stringify(putBody)
      }, { bucket: "etsy-listing-console" });
      const correctiveResult = await parseJson(correctivePutResp);

      if (correctivePutResp.ok) {
        const { resp: reResp, payload: rePayload } = await getInventory(listingId, headers);
        if (reResp.ok) {
          freshInventory = {
            products: rePayload.products || [],
            price_on_property: rePayload.price_on_property || [],
            quantity_on_property: rePayload.quantity_on_property || [],
            sku_on_property: rePayload.sku_on_property || []
          };
          check = verifyAgainst({ ...putBody }, freshInventory);
          autoCorrected = true;
        }
      }
    }

    return json(200, {
      ok: true,
      verified: check.verified,
      auto_corrected: autoCorrected,
      verification_error: check.verified ? null :
        "Live Etsy inventory does not match the submitted draft" + (autoCorrected ? " even after an automatic corrective retry" : "") + ": " + check.differences.slice(0, 5).join(" · ") +
        (check.differences.length > 5 ? " · +" + (check.differences.length - 5) + " more" : ""),
      differences: check.differences,
      listing_id: Number(listingId),
      previous_inventory: decimalizeInventory(previousInventory),
      previous_snapshot_hash: previousHash,
      fresh: {
        listing_id: Number(listingId),
        inventory: decimalizeInventory(freshInventory),
        snapshot_hash: snapshotHash(freshInventory),
        pricing_health: pricingHealth(freshInventory),
        fetched_at: new Date().toISOString()
      }
    });
  } catch (err) {
    return json(500, { error: err.message });
  }
};
