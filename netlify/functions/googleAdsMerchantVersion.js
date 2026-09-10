"use strict";

// Merchant API writes ProductInput, while products.get reads the processed
// Product (feed rules and supplemental data may change it). Never manufacture a
// restorable input baseline from that processed result. Only a trusted, saved
// ProductInput write receipt can establish the input fields below.
// https://developers.google.com/merchant/api/guides/products/overview
// https://developers.google.com/merchant/api/reference/rest/products_v1/accounts.productInputs/patch
const crypto = require("crypto");
const FIELDS = Object.freeze(["title", "description", "imageLink", "additionalImageLinks"]);
const ORIGIN = "https://merchantapi.googleapis.com";
const SCHEMA = 1;
function fail(code, message, extra) { return Object.assign(new Error(message), { code }, extra || {}); }
function canonical(value) {
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === "object") { const ordered = {}; for (const key of Object.keys(value).sort()) if (value[key] !== undefined) ordered[key] = canonical(value[key]); return ordered; }
  return value;
}
function hash(value) { return crypto.createHash("sha256").update(JSON.stringify(canonical(value))).digest("hex"); }
function clone(value) { return JSON.parse(JSON.stringify(value)); }
function fieldsOf(attributes) {
  const a = attributes || {}, fields = {};
  for (const field of FIELDS) fields[field] = a[field] == null ? null : clone(a[field]);
  // Empty repeated fields are omitted by protobuf JSON; these are equivalent.
  if (!fields.additionalImageLinks || !fields.additionalImageLinks.length) fields.additionalImageLinks = [];
  return fields;
}
function identityOf(input, account) {
  const i = input || {};
  const merchantId = String(account || i.merchantId || "");
  if (!/^\d+$/.test(merchantId) || (i.merchantId && String(i.merchantId) !== merchantId)) throw fail("INVALID_IDENTITY", "Merchant account does not match this product.");
  const offerId = i.offerId, contentLanguage = i.contentLanguage, feedLabel = i.feedLabel;
  if (typeof offerId !== "string" || !offerId || offerId !== offerId.trim() || offerId.length > 1024 || /[\x00-\x1f]/.test(offerId) || typeof contentLanguage !== "string" || !/^[a-z]{2}$/.test(contentLanguage) || typeof feedLabel !== "string" || !/^[A-Z0-9_-]{1,20}$/.test(feedLabel)) throw fail("INVALID_IDENTITY", "An exact offer ID, two-letter language and feed label are required.");
  if (i.legacyLocal != null && typeof i.legacyLocal !== "boolean") throw fail("INVALID_IDENTITY", "Invalid local product identity.");
  return { merchantId, offerId, contentLanguage, feedLabel, legacyLocal: i.legacyLocal === true };
}
function namesFor(identity) {
  const i = identityOf(identity), raw = (i.legacyLocal ? "local~" : "") + [i.contentLanguage, i.feedLabel, i.offerId].join("~");
  // Google recommends this form for all IDs, including offer IDs with /, % or ~.
  const encoded = Buffer.from(raw, "utf8").toString("base64url"), account = "accounts/" + i.merchantId;
  return { productName: account + "/products/" + encoded, inputName: account + "/productInputs/" + encoded };
}
function matchesIdentity(resource, expected) {
  if (!resource || resource.offerId !== expected.offerId || resource.contentLanguage !== expected.contentLanguage || resource.feedLabel !== expected.feedLabel || (resource.legacyLocal === true) !== expected.legacyLocal) return false;
  const name = String(resource.name || resource.product || ""), match = name.match(/^accounts\/(\d+)\/(?:products|productInputs)\/(.+)$/);
  if (!match || match[1] !== expected.merchantId) return false;
  const raw = (expected.legacyLocal ? "local~" : "") + [expected.contentLanguage, expected.feedLabel, expected.offerId].join("~");
  return match[2] === raw || match[2] === Buffer.from(raw, "utf8").toString("base64url");
}
function validSource(name, identity) { return typeof name === "string" && new RegExp("^accounts/" + identity.merchantId + "/dataSources/\\d+$").test(name); }
function sourceFingerprint(source) {
  return hash({ name: source.name, input: source.input || null, primary: source.primaryProductDataSource || null, supplemental: source.supplementalProductDataSource || null });
}
function inputHash(identity, dataSource, fields) { return hash({ identity, dataSource, fields: fieldsOf(fields) }); }
function receiptFor(identity, dataSource, productInput, now) {
  if (!matchesIdentity(productInput, identity)) throw fail("INPUT_RECEIPT_MISMATCH", "Google returned a different product input.");
  const fields = fieldsOf(productInput.productAttributes);
  return { schema: SCHEMA, origin: "merchant.productInputs.write-response", identity: clone(identity), dataSource, fields, inputHash: inputHash(identity, dataSource, fields), confirmedAt: now };
}
function validReceipt(receipt, identity, dataSource) {
  return receipt && receipt.schema === SCHEMA && receipt.origin === "merchant.productInputs.write-response" && hash(receipt.identity) === hash(identity) && receipt.dataSource === dataSource && Number.isFinite(receipt.confirmedAt) && receipt.fields && receipt.inputHash === inputHash(identity, dataSource, receipt.fields);
}
function validatedChanges(changes, restore) {
  if (!changes || typeof changes !== "object" || Array.isArray(changes) || !Object.keys(changes).length) throw fail("INVALID_CHANGES", "Choose at least one product field to change.");
  const result = {};
  for (const [field, value] of Object.entries(changes)) {
    if (!FIELDS.includes(field)) throw fail("UNSUPPORTED_FIELD", "Only product title, description and image fields can be changed here.");
    if (value === null && restore) { result[field] = null; continue; }
    if (field === "additionalImageLinks") {
      if (!Array.isArray(value) || value.length > 10 || value.some(v => typeof v !== "string")) throw fail("INVALID_IMAGES", "Use at most ten additional image URLs.");
      for (const url of value) validateImageUrl(url);
      if (new Set(value).size !== value.length) throw fail("INVALID_IMAGES", "Additional image URLs must be distinct.");
      result[field] = value.slice();
    } else if (field === "imageLink") { validateImageUrl(value); result[field] = value; }
    else {
      const max = field === "title" ? 150 : 5000;
      if (typeof value !== "string" || !value.trim() || [...value].length > max || /[\x00-\x08\x0b\x0c\x0e-\x1f]/.test(value)) throw fail("INVALID_TEXT", "Product " + field + " must contain 1–" + max + " characters.");
      result[field] = value;
    }
  }
  return result;
}
function validateImageUrl(value) {
  let url;
  try { url = new URL(value); } catch (_) { throw fail("INVALID_IMAGE_URL", "Product images require complete HTTP or HTTPS URLs."); }
  if (typeof value !== "string" || value.length > 2000 || !["https:", "http:"].includes(url.protocol) || !url.hostname || url.username || url.password || url.hash) throw fail("INVALID_IMAGE_URL", "Product images require complete HTTP or HTTPS URLs without credentials or fragments.");
}

function createMerchantVersionService({ fetch, mintMerchantToken, merchantCenterId, env = {}, readInputReceipt, now = Date.now, timeoutMs = 20000 } = {}) {
  if (typeof fetch !== "function" || typeof mintMerchantToken !== "function" || typeof merchantCenterId !== "function") throw new TypeError("Merchant service requires fetch, mintMerchantToken and merchantCenterId.");
  const budgetMs = Math.max(100, Math.min(30000, Number(timeoutMs) || 20000));
  async function bounded(work, deadline, code, onTimeout) {
    const remaining = deadline - Date.now();
    if (remaining <= 0) throw fail(code || "MERCHANT_TIMEOUT", "Merchant Center did not finish within the request deadline.");
    let timer;
    try {
      return await Promise.race([Promise.resolve().then(work), new Promise((_, reject) => { timer = setTimeout(() => { if (onTimeout) onTimeout(); reject(fail(code || "MERCHANT_TIMEOUT", "Merchant Center did not finish within the request deadline.")); }, remaining); })]);
    } finally { clearTimeout(timer); }
  }
  async function connection(deadline) {
    const [token, id] = await bounded(() => Promise.all([mintMerchantToken(), merchantCenterId()]), deadline);
    if (!token) throw fail("MERCHANT_CONNECTION_REQUIRED", "Connect Merchant Center to inspect this product's source.");
    if (!/^\d+$/.test(String(id || ""))) throw fail("INVALID_IDENTITY", "The Merchant Center account is unavailable.");
    return { token, merchantId: String(id) };
  }
  async function request(path, conn, deadline, method = "GET", body) {
    const controller = new AbortController();
    try {
      return await bounded(async () => {
        const response = await fetch(ORIGIN + path, { method, redirect: "error", signal: controller.signal, headers: { Authorization: "Bearer " + conn.token, "Content-Type": "application/json" }, ...(body ? { body: JSON.stringify(body) } : {}) });
        const data = await response.json().catch(() => null);
        if (!response.ok) throw fail(method === "PATCH" && response.status >= 500 ? "MERCHANT_WRITE_UNKNOWN" : "MERCHANT_HTTP_ERROR", "Merchant Center returned HTTP " + response.status + ".", { httpStatus: response.status, providerStatus: data && data.error && data.error.status || null, writeOutcome: method === "PATCH" && response.status >= 500 ? "unknown" : "rejected" });
        if (!data || typeof data !== "object" || Array.isArray(data)) throw fail(method === "PATCH" ? "MERCHANT_WRITE_UNKNOWN" : "INVALID_RESPONSE", "Merchant Center returned an unreadable response.", { writeOutcome: method === "PATCH" ? "unknown" : undefined });
        return data;
      }, deadline, method === "PATCH" ? "MERCHANT_WRITE_UNKNOWN" : "MERCHANT_TIMEOUT", () => controller.abort());
    } catch (error) {
      // A sent PATCH may have completed even if its response was lost. Callers
      // must store an unknown outcome and reconcile it; never auto-retry it.
      if (method === "PATCH" && !error.code) throw fail("MERCHANT_WRITE_UNKNOWN", "The product update response was lost. Check Merchant Center before retrying.", { writeOutcome: "unknown" });
      if (!error.code) throw fail("MERCHANT_UNAVAILABLE", "Merchant Center could not be reached.");
      if (method === "PATCH" && error.code === "MERCHANT_WRITE_UNKNOWN") error.writeOutcome = "unknown";
      throw error;
    }
  }
  async function inspectWith(input, conn, deadline) {
    const identity = identityOf(input, conn.merchantId), names = namesFor(identity);
    const product = await request("/products/v1/" + names.productName, conn, deadline);
    if (!matchesIdentity(product, identity)) throw fail("PRODUCT_MISMATCH", "Merchant Center returned a different product.");
    const dataSource = product.dataSource || null, processedFields = fieldsOf(product.productAttributes);
    const result = { schema: SCHEMA, identity, ...names, dataSource, inspectedAt: now(), processedFields, processedAt: product.productStatus && product.productStatus.lastUpdateDate || null, status: clone(product.productStatus || {}), archived: product.archived === true, editable: false, fields: null, inputHash: null, inputReceipt: null, source: null, sourceHash: null, snapshotBasis: "processed_product_only", reasonCode: null, reason: null };
    const advisory = (reasonCode, reason) => Object.assign(result, { reasonCode, reason });
    if (!validSource(dataSource, identity)) return advisory("OWNER_UNAVAILABLE", "The owning product feed could not be verified. Review this product in Merchant Center.");
    const source = await request("/datasources/v1/" + dataSource, conn, deadline);
    if (source.name !== dataSource) throw fail("SOURCE_MISMATCH", "Merchant Center returned a different product data source.");
    result.source = { name: dataSource, displayName: String(source.displayName || ""), input: source.input || null };
    result.sourceHash = sourceFingerprint(source);
    if (result.archived) return advisory("PRODUCT_ARCHIVED", "This product is archived. Restore its availability in the owning feed before changing its advertising details.");
    const configured = String(env.GMC_ADS_MANAGED_DATA_SOURCE || "").trim();
    if (!configured || configured !== dataSource) return advisory("OWNER_REVIEW_REQUIRED", "This product is managed by " + (result.source.displayName || "its existing feed") + ". Apply approved product changes in that source and let it sync to Merchant Center. Direct updates are available only for an explicitly configured app-managed feed.");
    if (source.input !== "API" || !source.primaryProductDataSource) return advisory("SOURCE_NOT_SUPPORTED", "This product is not in an app-managed primary API feed. Update the owning source and let it sync.");
    const refs = source.primaryProductDataSource.defaultRule && source.primaryProductDataSource.defaultRule.takeFromDataSources;
    if (Array.isArray(refs) && (!refs.length || refs.some(ref => !ref || ref.self !== true))) return advisory("FEED_RULE_OVERRIDE", "Supplemental feed rules can override this product. Review the owning feeds before applying a direct update.");
    const receipt = typeof readInputReceipt === "function" ? await bounded(() => readInputReceipt(clone(identity)), deadline) : null;
    if (!validReceipt(receipt, identity, dataSource)) return advisory("INPUT_BASELINE_UNAVAILABLE", "A confirmed raw product-input version is not available. Merchant Center exposes the processed product, which may include feed rules. Review changes in the owning feed until an exact restorable input version is available.");
    result.inputReceipt = clone(receipt); result.fields = fieldsOf(receipt.fields); result.inputHash = receipt.inputHash; result.snapshotBasis = "confirmed_input_receipt";
    if (hash(result.fields) !== hash(processedFields)) return advisory("INPUT_PROCESSING_OR_OVERRIDE", "The latest processed product differs from the saved input version. Wait for processing or resolve feed overrides before applying another change.");
    return Object.assign(result, { editable: true, reasonCode: "APP_MANAGED_INPUT", reason: "Changes can update this existing Merchant Center product input after approval. Google may take several minutes to process them." });
  }
  async function inspect(input) {
    const deadline = Date.now() + budgetMs;
    return inspectWith(input, await connection(deadline), deadline);
  }
  function createPatch(snapshot, changes, reason, restoring) {
    if (!snapshot || !snapshot.editable || snapshot.snapshotBasis !== "confirmed_input_receipt") throw fail(snapshot && snapshot.reasonCode || "INPUT_BASELINE_UNAVAILABLE", snapshot && snapshot.reason || "An exact, editable product-input version is required.");
    const identity = identityOf(snapshot.identity);
    if (!validSource(snapshot.dataSource, identity) || !validReceipt(snapshot.inputReceipt, identity, snapshot.dataSource) || snapshot.inputHash !== inputHash(identity, snapshot.dataSource, snapshot.fields)) throw fail("INVALID_SNAPSHOT", "The saved input version is not valid.");
    const clean = validatedChanges(changes, restoring), before = fieldsOf(snapshot.fields), after = Object.assign({}, before, clean);
    const changedFields = FIELDS.filter(field => JSON.stringify(before[field]) !== JSON.stringify(after[field]));
    if (!changedFields.length) throw fail("NO_CHANGES", "The proposed product fields already match this version.");
    const selected = {}; for (const field of changedFields) selected[field] = after[field];
    const patch = { schema: SCHEMA, provider: "merchant", kind: restoring ? "restore" : "update", identity, ...namesFor(identity), dataSource: snapshot.dataSource, expectedInputHash: snapshot.inputHash, expectedSourceHash: snapshot.sourceHash, before, after: fieldsOf(after), changes: selected, changedFields, updateMask: changedFields.map(field => "productAttributes." + field).join(","), reason: String(reason || "").slice(0, 2000), createdAt: now() };
    patch.patchHash = hash(patch);
    return patch;
  }
  function buildPatch({ snapshot, changes, reason } = {}) { return createPatch(snapshot, changes, reason, false); }
  function buildRestore({ snapshot, targetSnapshot, reason } = {}) {
    if (!targetSnapshot || !validReceipt(targetSnapshot.inputReceipt, identityOf(snapshot.identity), snapshot.dataSource) || targetSnapshot.inputHash !== inputHash(identityOf(snapshot.identity), snapshot.dataSource, targetSnapshot.fields)) throw fail("RESTORE_BASELINE_UNAVAILABLE", "That version has no confirmed input fields for this exact product and source.");
    return createPatch(snapshot, fieldsOf(targetSnapshot.fields), reason || "Restore previously recorded product input fields", true);
  }
  async function publish({ patch, approved } = {}) {
    // This internal helper does not replace the engine's persisted approval and
    // per-product claim. Call only after both have been checked atomically there.
    if (approved !== true) throw fail("APPROVAL_REQUIRED", "Approve this exact product update before applying it.");
    if (!patch || patch.schema !== SCHEMA || patch.provider !== "merchant" || !["update", "restore"].includes(patch.kind)) throw fail("INVALID_PATCH", "The approved product update is invalid.");
    const unhashed = { ...patch }; delete unhashed.patchHash;
    if (patch.patchHash !== hash(unhashed)) throw fail("INVALID_PATCH", "The product update changed after it was prepared.");
    const deadline = Date.now() + budgetMs, conn = await connection(deadline), identity = identityOf(patch.identity, conn.merchantId);
    const current = await inspectWith(identity, conn, deadline);
    if (!current.editable) throw fail(current.reasonCode, current.reason);
    if (current.inputHash !== patch.expectedInputHash || current.sourceHash !== patch.expectedSourceHash || hash(current.fields) !== hash(patch.before)) throw fail("STALE_PRODUCT_VERSION", "The product or its source changed after analysis. Analyze the current version before approving an update.");
    const rebuilt = createPatch(current, patch.changes, patch.reason, patch.kind === "restore");
    if (rebuilt.updateMask !== patch.updateMask || hash(rebuilt.after) !== hash(patch.after) || rebuilt.inputName !== patch.inputName || rebuilt.dataSource !== patch.dataSource) throw fail("INVALID_PATCH", "The approved product fields do not match the prepared update.");
    const attributes = {}; for (const field of rebuilt.changedFields) if (rebuilt.after[field] !== null) attributes[field] = rebuilt.after[field];
    const query = new URLSearchParams({ dataSource: rebuilt.dataSource, updateMask: rebuilt.updateMask });
    const response = await request("/products/v1/" + rebuilt.inputName + "?" + query, conn, deadline, "PATCH", { name: rebuilt.inputName, productAttributes: attributes });
    let inputReceipt;
    try {
      inputReceipt = receiptFor(identity, rebuilt.dataSource, response, now());
      if (hash(inputReceipt.fields) !== hash(rebuilt.after)) throw fail("INPUT_RECEIPT_MISMATCH", "The returned product input does not match the approved fields.");
    } catch (error) { throw fail("MERCHANT_WRITE_UNKNOWN", "Google accepted the update but its returned product input could not be verified. Reconcile this update before retrying.", { writeOutcome: "unknown", reasonCode: error.code }); }
    const result = { provider: "merchant", status: "input_confirmed", writeOutcome: "confirmed", sameProduct: true, identity, dataSource: rebuilt.dataSource, patchHash: patch.patchHash, changedFields: rebuilt.changedFields, inputReceipt, beforeInputReceipt: current.inputReceipt, inputConfirmedAt: inputReceipt.confirmedAt, processedStatus: "pending", processedFields: null, processedError: null, message: "Google confirmed the updated product input. Merchant Center processing and Shopping review can take several minutes." };
    try {
      const processed = await request("/products/v1/" + rebuilt.productName, conn, deadline);
      if (!matchesIdentity(processed, identity) || processed.dataSource !== rebuilt.dataSource) throw fail("PRODUCT_MISMATCH", "The processed product identity or source changed.");
      result.processedFields = fieldsOf(processed.productAttributes);
      if (hash(result.processedFields) === hash(rebuilt.after)) { result.processedStatus = "confirmed"; result.message = "Google confirmed the updated input and processed product fields. Serving still depends on Merchant Center eligibility and review."; }
    } catch (error) { result.processedError = { code: error.code || "MERCHANT_UNAVAILABLE", message: error.message }; }
    return result;
  }
  return { inspect, buildPatch, buildRestore, publish };
}

module.exports = { createMerchantVersionService };
