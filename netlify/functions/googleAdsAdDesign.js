// Ad Design workspaces: durable source selection, generation checkpoints and
// exact reviewed outputs. This module never publishes a Google Ads mutation.
const crypto = require("crypto");
const clean = v => JSON.parse(JSON.stringify(v));
const sha = v => crypto.createHash("sha256").update(typeof v === "string" ? v : JSON.stringify(v)).digest("hex");
const token = value => /^[a-zA-Z0-9_-]{1,100}$/.test(String(value || ""));
const active = job => job && Number(job.leaseUntil) > Date.now() && ["queued", "running"].includes(job.phase);
const MAX_UPLOAD = 4 * 1024 * 1024;
const FORMATS = [
  { key: "square", label: "Square", width: 1024, height: 1024, requestSize: "1024x1024", fieldType: "SQUARE_MARKETING_IMAGE", required: true },
  { key: "landscape", label: "Landscape", width: 1956, height: 1024, requestSize: "1968x1024", fieldType: "MARKETING_IMAGE", required: true },
  { key: "portrait", label: "Portrait", width: 1024, height: 1280, requestSize: "1024x1280", fieldType: "PORTRAIT_MARKETING_IMAGE", required: true }
];
function settingsFor(raw, products, groups, references, formats) {
  const settings = raw || {}, product = products.find(row => String(row.id) === String(settings.productId || "")) || (settings.productId ? null : products[0]);
  if (!product) throw new Error("No verified product is available for this design.");
  const group = groups.find(row => row.ref === settings.groupRef) || (settings.groupRef ? null : groups[0]);
  if (!group) throw new Error("Choose an ad or asset group before designing.");
  if (Array.isArray(product.eligibleGroupRefs) && !product.eligibleGroupRefs.includes(group.ref)) throw new Error("This product has not been verified for the selected ad group's destination. Refresh its product sources before designing.");
  if ((group.productIds || []).length && !(group.productIds || []).map(String).includes(String(product.id).split("/").pop())) throw new Error("Choose a product belonging to this ad group's verified selection.");
  if ((group.itemIds || []).length) { const productId = String(product.id).split("/").pop(), allowed = (group.itemIds || []).some(id => String(id) === String(product.itemId || product.offerId || "") || (String(id).match(/^shopify_[^_]+_(\d+)_/i) || [])[1] === productId); if (!allowed) throw new Error("Choose a product included in this asset group's verified product selection."); }
  const uploadedSource = references.find(row => row.id === settings.sourceImageId && row.role === "product" && String(row.productId) === String(product.id));
  const image = (product.images || []).find(row => row.id === settings.sourceImageId) || uploadedSource || (settings.sourceImageId ? null : (product.images || [])[0]);
  if (!image) throw new Error("The selected source photograph does not belong to this product.");
  const referenceIds = [...new Set((settings.referenceIds || []).map(String))];
  if (referenceIds.length > 8 || referenceIds.some(id => !references.some(row => row.id === id))) throw new Error("Choose up to eight saved reference images from this workspace.");
  const direction = String(settings.direction || "").trim(); if (direction.length > 2400) throw new Error("Keep the art direction under 2,400 characters.");
  const style = {}; for (const key of ["background", "border", "font", "scale"]) style[key] = String((settings.style || {})[key] || ({ background: "warm ivory", border: "none", font: "Montserrat", scale: "product-led" })[key]).slice(0, 100);
  const requested = Array.isArray(settings.formats) ? settings.formats : formats.filter(row => row.required).map(row => row.key);
  if (requested.some(key => !formats.some(row => row.key === key))) throw new Error("Choose supported Google Ads image formats.");
  return { productId: String(product.id), groupRef: group.ref, sourceImageId: image.id, referenceIds, direction, style,
    formats: [...new Set([...formats.filter(row => row.required).map(row => row.key), ...requested])] };
}
function responseText(result) {
  if (typeof result.output_text === "string") return result.output_text;
  return (result.output || []).flatMap(row => row.content || []).filter(row => row.type === "output_text").map(row => row.text || "").join("\n");
}
function buildVersionDesignPayload({ workspaceId, jobId, workspace, group, product, result, customerId }) {
  const snapshot = workspace.sourceSnapshot, campaignId = String(workspace.context.campaignId || ""), copy = result.copy || {}, components = snapshot && snapshot.components;
  if (!snapshot || !snapshot.complete || String(snapshot.campaignId) !== campaignId || !Number.isSafeInteger(workspace.sourceVersion) || !/^[a-f0-9]{64}$/.test(workspace.snapshotHash || "")) throw new Error("The design must be tied to a complete saved version of this campaign.");
  if (!Array.isArray(copy.headlines) || !Array.isArray(copy.descriptions) || copy.headlines.length < 3 || copy.headlines.length > 15 || copy.descriptions.length < 2 || copy.descriptions.length > 5) throw new Error("The generated copy is incomplete.");
  if (copy.headlines.some(text => typeof text !== "string" || !text.trim() || text.length > 30) || copy.descriptions.some(text => typeof text !== "string" || !text.trim() || text.length > 90)) throw new Error("The generated copy does not meet Google Ads text limits.");
  const prefix = "customers/" + customerId + "/", operations = [], generatedAssets = [], changes = [], notes = []; let nextText = -970001, nextImage = -980001;
  const imageDescriptor = key => {
    const asset = result.assets && result.assets[key];
    if (!asset || !/^Brites_GAds_Creative\/[a-zA-Z0-9_-]+\/[a-zA-Z0-9_-]+\.jpg$/.test(asset.path || "") || !/^[a-f0-9]{64}$/.test(asset.hash || "") || !Number.isFinite(asset.width) || !Number.isFinite(asset.height) || asset.width < 1 || asset.height < 1 || Number(asset.bytes) <= 0 || Number(asset.bytes) > 5120000) throw new Error("Every required image format must be saved and verified before a version proposal is created.");
    const tempResourceName = prefix + "assets/" + nextImage--; generatedAssets.push({ tempResourceName, asset: clean(asset) }); return { tempResourceName, asset };
  };
  if (group.channel === "search") {
    const ad = components.searchAds.find(row => row.resourceName === group.ref);
    if (!ad || !ad.resourceName.startsWith(prefix + "ads/")) throw new Error("The selected Search ad is outside the saved campaign.");
    if (copy.descriptions.length > 4) throw new Error("Search ads support at most four descriptions.");
    const preservePins = field => {
      const prior = ad.responsiveSearchAd && ad.responsiveSearchAd[field] || [];
      if (prior.some(row => row.pinnedField && !["UNKNOWN", "UNSPECIFIED"].includes(row.pinnedField) && !copy[field].includes(row.text))) throw new Error("A pinned Search text must be kept unchanged in this design. Review a concept that preserves the existing pinned " + field + ".");
      return copy[field].map(text => { const pinned = prior.find(row => row.text === text && row.pinnedField && !["UNKNOWN", "UNSPECIFIED"].includes(row.pinnedField)); return { text, ...(pinned ? { pinnedField: pinned.pinnedField } : {}) }; });
    };
    operations.push({ adOperation: { update: { resourceName: ad.resourceName, responsiveSearchAd: { headlines: preservePins("headlines"), descriptions: preservePins("descriptions") } }, updateMask: "responsive_search_ad.headlines,responsive_search_ad.descriptions" } });
    changes.push({ category: "Search copy", target: group.ref, field: "responsiveSearchAd", before: ad.responsiveSearchAd, after: copy, reason: result.brief.rationale });
    if (!Array.isArray(components.searchImageLinks)) throw new Error("The Search ad group's image links must be captured before its images can be updated.");
    const oldImages = components.searchImageLinks.filter(row => row.adGroup === ad.adGroup && row.fieldType === "IMAGE");
    oldImages.forEach(row => operations.push({ adGroupAssetOperation: { remove: row.resourceName } }));
    for (const key of ["square", "landscape"]) { const image = imageDescriptor(key); operations.push({ adGroupAssetOperation: { create: { adGroup: ad.adGroup, asset: image.tempResourceName, fieldType: "IMAGE" } } }); }
    changes.push({ category: "Search images", before: oldImages.map(row => ({ asset: row.asset, imageUrl: row.imageUrl || null })), after: generatedAssets.map(row => ({ path: row.asset.path, hash: row.asset.hash, width: row.asset.width, height: row.asset.height })), reason: "Attach the exact reviewed square and landscape photographs to this existing Search ad group." });
    notes.push("The Search ad and ad-group IDs stay the same. Square and landscape images are attached to the existing ad group; portrait is a design preview because Search image assets do not use that format. Google controls whether an image serves.");
  } else if (group.channel === "pmax") {
    if (!components.assetGroups.some(row => row.resourceName === group.ref) || !String(group.ref).startsWith(prefix + "assetGroups/")) throw new Error("The selected asset group is outside the saved campaign.");
    const old = components.assetLinks.filter(row => row.assetGroup === group.ref), textFields = [["headlines", "HEADLINE"], ["longHeadlines", "LONG_HEADLINE"], ["descriptions", "DESCRIPTION"]];
    if (!Array.isArray(copy.longHeadlines) || copy.longHeadlines.length < 1 || copy.longHeadlines.length > 5 || copy.longHeadlines.some(text => typeof text !== "string" || !text.trim() || text.length > 90)) throw new Error("Performance Max long headlines are incomplete.");
    for (const [key, fieldType] of textFields) {
      old.filter(row => row.fieldType === fieldType).forEach(row => operations.push({ assetGroupAssetOperation: { remove: row.resourceName } }));
      for (const text of copy[key]) { const resourceName = prefix + "assets/" + nextText--; operations.push({ assetOperation: { create: { resourceName, textAsset: { text } } } }, { assetGroupAssetOperation: { create: { assetGroup: group.ref, asset: resourceName, fieldType } } }); }
      changes.push({ category: key === "descriptions" ? "Descriptions" : "Headlines", target: group.ref, field: key, before: old.filter(row => row.fieldType === fieldType).map(row => row.text || row.asset), after: copy[key], reason: result.brief.rationale });
    }
    for (const format of FORMATS) {
      const { tempResourceName, asset } = imageDescriptor(format.key);
      old.filter(row => row.fieldType === format.fieldType).forEach(row => operations.push({ assetGroupAssetOperation: { remove: row.resourceName } }));
      operations.push({ assetGroupAssetOperation: { create: { assetGroup: group.ref, asset: tempResourceName, fieldType: format.fieldType } } });
      changes.push({ category: "Images", target: group.ref, field: format.fieldType, before: old.filter(row => row.fieldType === format.fieldType).map(row => ({ asset: row.asset, imageUrl: row.imageUrl || null })), after: { format: format.key, path: asset.path, hash: asset.hash, width: asset.width, height: asset.height }, reason: "Use the reviewed " + format.label.toLowerCase() + " photograph of " + product.title + " on the same asset group." });
    }
    notes.push("Google keeps the existing campaign and asset-group IDs. New immutable image/text assets are linked in the same approved update; Merchant feed product photos stay unchanged.");
  } else throw new Error("Choose a Search ad or Performance Max asset group.");
  const versionChange = { campaignId, sourceVersion: workspace.sourceVersion, proposedVersion: workspace.sourceVersion + 1, changes, identityPreserved: true, notes, merchantChanges: false,
    rationale: result.brief.rationale, hypothesis: result.brief.hypothesis, successMetric: result.brief.successMetric, measurementPlan: result.brief.measurementPlan || null };
  return { mutateOperations: operations, generatedAssets, versionGuard: { campaignId, expectedVersion: workspace.sourceVersion, snapshotHash: workspace.snapshotHash }, versionChange,
    adDesign: { workspaceId, jobId, productId: String(product.id), sourceImageId: workspace.settings.sourceImageId, settings: workspace.settings, evidenceHash: result.evidence && result.evidence.hash || null, sourceIds: result.sourceIds || [], quality: result.quality, previewOnlyFormats: group.channel === "search" ? ["portrait"] : [], generatedAt: Date.now() },
    meta: { existingCampaignId: campaignId, adDesignWorkspaceId: workspaceId, adDesignId: jobId, operation: "reviewedAdDesign" } };
}
function createAdDesignService(deps) {
  const f = () => { const value = deps.fb(); if (!value) throw new Error("Ad Design storage is unavailable."); return value; };
  const refFor = id => { if (!token(id)) throw new Error("Invalid design workspace."); return f().db.collection(deps.COL.state).doc("adDesign").collection("workspaces").doc(id); };
  const formats = () => deps.formats || FORMATS;
  const provider = () => ({ available: !!deps.env.OPENAI_API_KEY, label: "GPT Image 2.5 Sunburst", quality: "high", referenceLimit: 16, formats: formats(), ...deps.provider, model: "gpt-image-2.5-sunburst" });
  function settle(job, key, output) {
    const reservations = job.reservations || [], reservation = reservations.find(row => row.requestId === output.requestId) || [...reservations].reverse().find(row => row.key === key && !row.settled);
    if (!reservation) return;
    reservation.settled = true;
    if (output.costEstimated === false && Number.isFinite(Number(output.estimatedUsd)) && Number(output.estimatedUsd) >= 0) reservation.actualUsd = Number(output.estimatedUsd);
    const usage = { key, requestId: reservation.requestId, at: Date.now(), usage: output.usage || {}, providerModel: output.providerModel || null, reservedUsd: reservation.reservedUsd,
      estimatedUsd: reservation.actualUsd == null ? reservation.reservedUsd : reservation.actualUsd, costEstimated: reservation.actualUsd == null };
    job.usage = (job.usage || []).filter(row => row.requestId !== reservation.requestId).concat(usage);
  }
  async function read(id) { const s = await refFor(id).get(); if (!s.exists) throw new Error("Ad Design workspace was not found."); return s.data(); }
  async function productsFor(ref, workspace) { if (!workspace) { const s = await ref.get(); workspace = s.exists && s.data(); } if (!workspace || !token(workspace.sourceSetId)) throw new Error("Saved product sources are unavailable."); const result = await ref.collection("sourceSets").doc(workspace.sourceSetId).collection("products").get(); return result.docs.map(row => row.data()).sort((a, b) => (a.position || 0) - (b.position || 0)); }
  async function status({ workspaceId } = {}) {
    const ref = refFor(workspaceId), workspace = await read(workspaceId), products = await productsFor(ref), job = workspace.job || {}, references = [];
    for (const reference of workspace.references || []) references.push({ ...reference, url: await deps.signAsset(reference.asset) });
    const result = job.result ? clean(job.result) : null;
    if (result) for (const asset of Object.values(result.assets || {})) asset.url = await deps.signAsset(asset);
    if (result && result.logo) result.logo.url = await deps.signAsset(result.logo);
    let hasReceipt = false;
    if (job.inFlight && !active(job)) { const receipt = await ref.collection("outputs").doc(job.id + "_" + job.inFlight.key).get(); hasReceipt = !!(receipt.exists && (receipt.data().stageResult || receipt.data().rawResponse || receipt.data().asset)); }
    const approvalId = job.approvalId || workspace.context.approvalId || null, review = approvalId && deps.reviewStatus ? await deps.reviewStatus(approvalId) : null;
    return { ok: true, workspaceId, sourceVersion: workspace.sourceVersion || null, snapshotHash: workspace.snapshotHash || null,
      context: workspace.context, products, references, settings: workspace.settings, status: job.phase || "draft", phase: job.phase || "draft",
      progress: job.progress || { pct: 0, label: "Choose your product, reference images and direction" }, result,
      approvalId, review, error: job.error || null,
      canRetry: !active(job) && (!job.inFlight || hasReceipt) && ["paused", "needs_attention", "queued", "running"].includes(job.phase), needsNewRequestApproval: !!job.inFlight && !active(job) && !hasReceipt,
      provider: provider(), formats: formats(), usage: job.usage || [], generatedAt: job.completedAt || null,
      controlsMeaning: "Background and scale guide the photograph. Font and border preview the composition; Google chooses responsive typography and layout." };
  }
  async function workspace(input = {}) {
    if (input.workspaceId) return status({ workspaceId: input.workspaceId });
    const contextKey = { campaignId: String(input.campaignId || ""), approvalId: String(input.approvalId || ""), handle: String(input.handle || ""), groupRef: String(input.groupRef || ""),
      itemIds: input.campaignId || input.approvalId ? [] : [...new Set((input.itemIds || []).map(String))].sort(), feedLabel: input.campaignId || input.approvalId ? "" : String(input.feedLabel || "").toUpperCase() };
    if (!contextKey.campaignId && !contextKey.approvalId && !contextKey.handle) throw new Error("Open Ad Design from a campaign, draft or product opportunity.");
    const id = "design_" + sha(JSON.stringify(contextKey)).slice(0, 32), ref = refFor(id), saved = await ref.get();
    if (saved.exists && !input.force) return status({ workspaceId: id });
    if (saved.exists && (active(saved.data().job) || saved.data().job && saved.data().job.inFlight)) throw new Error("Resolve the current or unconfirmed design request before refreshing its source context.");
    const loaded = await deps.loadContext(input), products = loaded.products || [], groups = loaded.context.groups || [];
    if (!products.length) throw new Error("No verified product photographs were found for this context.");
    const sourceSetId = crypto.randomUUID();
    await Promise.all(products.map((product, position) => ref.collection("sourceSets").doc(sourceSetId).collection("products").doc(sha(String(product.id)).slice(0, 32)).set(clean({ ...product, position }))));
    const prior = saved.exists ? saved.data() : null;
    const initialGroup = groups.find(group => group.ref === input.groupRef) || groups[0], initialProduct = products.find(product => (product.images || []).length && (!(initialGroup.productIds || []).length || initialGroup.productIds.map(String).includes(String(product.id).split("/").pop()))) || products.find(product => (product.images || []).length) || products[0];
    const references = prior && prior.references || [], settings = settingsFor({ productId: initialProduct.id, groupRef: input.groupRef }, products, groups, references, formats());
    await f().db.runTransaction(async tx => { const latest = await tx.get(ref), current = latest.exists ? latest.data() : null;
      if (!!current !== !!prior || current && (Number(current.revision) !== Number(prior.revision) || (current.job && current.job.id || null) !== (prior.job && prior.job.id || null) || active(current.job) || current.job && current.job.inFlight)) throw new Error("The workspace changed while its sources were being refreshed. The current design was preserved.");
      if (current && current.job) tx.set(ref.collection("history").doc(current.job.id), current.job);
      tx.set(ref, clean({ schema: 1, workspaceId: id, context: loaded.context, sourceVersion: loaded.sourceVersion || null, snapshotHash: loaded.snapshotHash || null,
        sourceSnapshot: loaded.snapshot || null, sourceSetId, productsIds: products.map(row => row.id), settings, references, job: null, revision: Number(prior && prior.revision || 0) + 1, createdAt: prior && prior.createdAt || Date.now(), updatedAt: Date.now() }));
    });
    return status({ workspaceId: id });
  }
  async function save(input = {}) {
    const ref = refFor(input.workspaceId), products = await productsFor(ref);
    await f().db.runTransaction(async tx => { const s = await tx.get(ref); if (!s.exists) throw new Error("Workspace was not found."); const value = s.data();
      if (active(value.job)) throw new Error("Wait for the current design to finish before changing its direction.");
      const settings = settingsFor({ ...value.settings, ...input }, products, value.context.groups || [], value.references || [], formats());
      if (sha(settings) === sha(value.settings)) return;
      if (value.job && value.job.inFlight) throw new Error("The previous paid request has an unconfirmed outcome. Resolve it before changing this workspace.");
      if (value.job) tx.set(ref.collection("history").doc(value.job.id), value.job);
      tx.update(ref, { settings, job: null, revision: Number(value.revision || 0) + 1, updatedAt: Date.now() });
    }); return status({ workspaceId: input.workspaceId });
  }
  async function upload({ workspaceId, fileName, mimeType, dataBase64, role = "inspiration" } = {}) {
    if (!["image/jpeg", "image/png", "image/webp"].includes(mimeType) || !["inspiration", "product"].includes(role)) throw new Error("Upload a JPEG, PNG or WebP product or inspiration image.");
    if (typeof dataBase64 !== "string" || dataBase64.length > Math.ceil(MAX_UPLOAD * 4 / 3) + 4 || !/^[A-Za-z0-9+/]*={0,2}$/.test(dataBase64)) throw new Error("Reference images must be no larger than 4 MiB.");
    const original = Buffer.from(dataBase64, "base64"); if (!original.length || original.length > MAX_UPLOAD) throw new Error("Reference images must be no larger than 4 MiB.");
    const ref = refFor(workspaceId), workspace = await read(workspaceId); if (active(workspace.job)) throw new Error("Wait for the current design before adding a reference.");
    const normalized = await deps.normalizeUpload(original), id = "ref_" + sha([role, role === "product" ? workspace.settings.productId : null, sha(normalized.bytes.toString("base64"))]).slice(0, 24);
    const asset = await deps.saveAsset(workspaceId, normalized.bytes, id, { width: normalized.width, height: normalized.height, kind: role });
    await f().db.runTransaction(async tx => { const s = await tx.get(ref); if (!s.exists || active(s.data().job) || role === "product" && s.data().settings.productId !== workspace.settings.productId) throw new Error("The workspace changed while the upload was being saved."); const refs = s.data().references || [];
      if (refs.some(row => row.id === id)) return; if (refs.length >= 12) throw new Error("A workspace can hold up to 12 reference images.");
      tx.update(ref, { references: refs.concat({ id, fileName: String(fileName || "Reference image").slice(0, 160), role, productId: s.data().settings.productId, asset, uploadedAt: Date.now() }), revision: Number(s.data().revision || 0) + 1, updatedAt: Date.now() });
    }); return { ok: true, workspaceId, id, role, url: await deps.signAsset(asset), asset };
  }
  async function start({ workspaceId, expectedVersion, snapshotHash, retry = false } = {}) {
    const ref = refFor(workspaceId), value = await read(workspaceId), capability = provider();
    if (value.job && (value.job.phase === "ready" || active(value.job))) return { ok: true, workspaceId, jobId: value.job.id, cached: true, queued: false };
    if (!capability.available) throw new Error(capability.reason || "Image generation is not configured.");
    if (value.context.generationAllowed === false) throw new Error("This source context is not ready for generation. Choose an eligible product and current ad or draft.");
    if (deps.verifyContext) await deps.verifyContext(value);
    if (value.context.campaignId) await deps.verifyBasis({ campaignId: value.context.campaignId, expectedVersion: expectedVersion == null ? value.sourceVersion : Number(expectedVersion), snapshotHash: snapshotHash || value.snapshotHash });
    if (!value.context.campaignId && !value.context.approvalId && !(value.context.itemIds || []).length) throw new Error("Choose a current product opportunity with exact eligible Merchant offers before generating a new campaign draft.");
    let jobId, cached = false;
    await f().db.runTransaction(async tx => { const s = await tx.get(ref); if (!s.exists) throw new Error("Workspace was not found."); const current = s.data(), previous = current.job;
      if (current.revision !== value.revision || sha(current.settings) !== sha(value.settings)) throw new Error("The selected product or design direction changed before generation began. Review it and start again.");
      if (active(previous)) { jobId = previous.id; cached = true; return; }
      if (previous && previous.phase === "ready") { jobId = previous.id; cached = true; return; }
      let recovered = null, recordedOutput = false;
      if (previous && previous.inFlight) { const receipt = await tx.get(ref.collection("outputs").doc(previous.id + "_" + previous.inFlight.key)); if (receipt.exists) { recovered = receipt.data().stageResult || null; recordedOutput = !!(recovered || receipt.data().rawResponse || receipt.data().asset); } }
      if (previous && previous.inFlight && !recordedOutput && !retry) throw new Error("A previous paid request has an unknown result. Saved outputs are retained; explicitly allow a new request only if you want to retry that unfinished step.");
      const job = previous ? clean(previous) : { id: crypto.randomUUID(), sourceVersion: current.sourceVersion, snapshotHash: current.snapshotHash, settingsHash: sha(current.settings), phase: "queued", stages: {}, assets: {}, usage: [], requests: 0, createdAt: Date.now() };
      if (job.settingsHash !== sha(current.settings)) throw new Error("The direction changed. Save a new design revision first.");
      if (job.inFlight) { if (recovered) { job.stages[job.inFlight.key] = recovered; settle(job, job.inFlight.key, recovered); } else if (!recordedOutput) job.unknownRequests = (job.unknownRequests || []).concat({ ...job.inFlight, retriedAt: Date.now(), explicitRetry: true }); job.inFlight = null; }
      job.phase = "queued"; job.owner = null; job.error = null; job.leaseUntil = 0; job.progress = { pct: Number(job.progress && job.progress.pct) || 0, label: "Design queued; saved work will be reused" }; jobId = job.id;
      tx.update(ref, { job, updatedAt: Date.now() });
    }); return { ok: true, workspaceId, jobId, queued: !cached, cached };
  }
  async function run({ workspaceId, jobId } = {}) {
    const ref = refFor(workspaceId), owner = crypto.randomUUID(), started = Date.now(); let value, job;
    await f().db.runTransaction(async tx => { const s = await tx.get(ref); if (!s.exists) throw new Error("Workspace was not found."); value = s.data(); job = value.job;
      if (!job || job.id !== jobId) throw new Error("This generation job is no longer current.");
      if (job.phase === "ready") return; if (active(job)) throw new Error("This design is already running."); if (job.inFlight) throw new Error("The last paid request requires explicit resolution before another request.");
      job = { ...job, phase: "running", owner, leaseUntil: Date.now() + 850000 }; tx.update(ref, { job });
    });
    if (job.phase === "ready") return { ok: true, cached: true, workspaceId };
    const saveJob = async patch => { Object.assign(job, patch, { updatedAt: Date.now() }); await f().db.runTransaction(async tx => { const s = await tx.get(ref); if (!s.exists || !s.data().job || s.data().job.id !== jobId || s.data().job.owner !== owner) throw new Error("The design job lost its active lease."); tx.update(ref, { job: clean(job), updatedAt: Date.now() }); }); };
    const writeReceipt = async (key, output) => { await f().db.runTransaction(async tx => { const s = await tx.get(ref); if (!s.exists || !s.data().job || s.data().job.id !== jobId || s.data().job.owner !== owner) throw new Error("The design job lost its lease before its output could be attached."); tx.set(ref.collection("outputs").doc(jobId + "_" + key), clean({ ...output, requestId: output.requestId || job.inFlight && job.inFlight.requestId || null }), { merge: true }); }); };
    const progress = (pct, label) => saveJob({ progress: { pct: Math.max(Number(job.progress && job.progress.pct) || 0, pct), label } });
    const paid = async (key, operation) => {
      if (job.stages[key]) return job.stages[key];
      const stored = await ref.collection("outputs").doc(jobId + "_" + key).get();
      if (stored.exists && stored.data().stageResult) { job.stages[key] = stored.data().stageResult; settle(job, key, job.stages[key]); await saveJob({ inFlight: null }); return job.stages[key]; }
      if (stored.exists && (stored.data().rawResponse || stored.data().asset)) {
        const output = await operation(stored.data().requestId); output.requestId = stored.data().requestId;
        await writeReceipt(key, { stageResult: output, receivedAt: Date.now() }); job.stages[key] = clean(output); settle(job, key, output); await saveJob({ inFlight: null }); return output;
      }
      if (deps.verifyContext) await deps.verifyContext(value);
      if (typeof deps.reserveCost !== "function") throw new Error("The provider's request cost allowance is unavailable; no generation request was sent.");
      const quote = await deps.reserveCost({ key, provider: provider(), workspace: value, job }), reserve = Number(quote && typeof quote === "object" ? quote.reservedUsd : quote);
      if (!Number.isFinite(reserve) || reserve <= 0) throw new Error("The provider's request cost allowance could not be verified.");
      const ctrl = await deps.control(), allowance = Math.max(1, Math.min(30, Number(ctrl.creativeBudgetUsd) || 8)), spent = (job.reservations || []).reduce((sum, row) => sum + Number(row.actualUsd == null ? row.reservedUsd : row.actualUsd), 0);
      if (job.requests >= 24 || spent + reserve > allowance) throw new Error("The remaining creative allowance cannot cover this request. Saved outputs and uncertain-request reservations are retained; adjust the allowance to finish.");
      const requestId = crypto.randomUUID(); job.reservations = (job.reservations || []).concat({ key, requestId, reservedUsd: reserve, rationale: String(quote && quote.rationale || "Planning reservation; actual provider token usage is recorded separately.").slice(0, 400), at: Date.now(), settled: false });
      await saveJob({ requests: Number(job.requests || 0) + 1, inFlight: { key, at: Date.now(), requestId } });
      let output;
      try { output = await operation(job.inFlight.requestId); }
      catch (error) { if (error.definiteResponse) await saveJob({ inFlight: null }); throw error; }
      // Callers save raw images or Responses results before returning. A receipt
      // survives a crash between provider completion and the workspace update.
      output.requestId = requestId; await writeReceipt(key, { stageResult: output, receivedAt: Date.now() });
      job.stages[key] = clean(output); settle(job, key, output);
      await saveJob({ inFlight: null }); return output;
    };
    try {
      if (value.context.generationAllowed === false) throw new Error("This source context is not eligible for generation.");
      if (deps.verifyContext) await deps.verifyContext(value);
      if (value.context.campaignId) await deps.verifyBasis({ campaignId: value.context.campaignId, expectedVersion: job.sourceVersion, snapshotHash: job.snapshotHash });
      const products = await productsFor(ref), product = products.find(row => String(row.id) === value.settings.productId), group = (value.context.groups || []).find(row => row.ref === value.settings.groupRef);
      if (!product || !group) throw new Error("The selected product or ad group is no longer available.");
      settingsFor(value.settings, products, value.context.groups || [], value.references || [], formats());
      const uploadedProducts = (value.references || []).filter(row => row.role === "product" && String(row.productId) === String(product.id));
      for (const reference of uploadedProducts) product.images.push({ id: reference.id, url: await deps.signAsset(reference.asset), alt: reference.fileName, width: reference.asset.width, height: reference.asset.height, uploadedAsset: reference.asset });
      const primary = (product.images || []).find(image => image.id === value.settings.sourceImageId);
      if (!primary) throw new Error("The selected source photograph was not found.");
      const references = (value.references || []).filter(row => value.settings.referenceIds.includes(row.id) && row.id !== primary.id), limit = Number(provider().referenceLimit) || 16;
      const productImages = [primary, ...(product.images || []).filter(image => image.id !== primary.id)], selectedImages = productImages.slice(0, Math.max(1, limit - references.length));
      await progress(4, "Saving the exact source photographs and inspiration");
      const inputs = selectedImages.map(image => ({ id: image.id, role: "product", url: image.url, asset: image.uploadedAsset || null })).concat(references.map(reference => ({ id: reference.id, role: reference.role, asset: reference.asset })));
      job.inputAssets = job.inputAssets || [];
      for (const [index, input] of inputs.entries()) {
        if (job.inputAssets.some(saved => saved.id === input.id)) continue;
        let asset = input.asset;
        if (!asset) { const original = await deps.sourceBytes(input.url), normalized = await deps.normalizeUpload(original); asset = await deps.saveAsset(workspaceId, normalized.bytes, jobId.replace(/-/g, "") + "_source_" + index, { width: normalized.width, height: normalized.height, kind: "source" }); }
        job.inputAssets.push({ id: input.id, role: input.role, asset }); await saveJob({});
      }
      const sourceFiles = await Promise.all(inputs.map(input => deps.loadAsset(job.inputAssets.find(saved => saved.id === input.id).asset)));
      job.inputCoverage = { availableProductImages: productImages.length, usedProductImages: selectedImages.length, usedInspirationImages: references.length, sourceImageIds: selectedImages.map(image => image.id), referenceIds: references.map(row => row.id), sourceAssets: job.inputAssets.map(input => ({ id: input.id, hash: input.asset.hash })), note: productImages.length > selectedImages.length ? "The full gallery is available. This request uses the selected hero and the remaining photos in listing order within the image provider's reference limit." : "Every available photo of the selected product was included." }; await saveJob({});
      await progress(6, "Researching this product, buyer intent and ad history");
      const savedCopy = await ref.collection("outputs").doc(jobId + "_copy").get();
      if (!job.evidence || !job.stages.copy && !savedCopy.exists && Date.now() - Number(job.evidence.researchCompletedAt || 0) > 600000) { job.evidence = await deps.research.collect({ campaignId: value.context.campaignId || null, sourceVersion: value.sourceVersion, snapshot: value.sourceSnapshot, range: value.context.range, group, selectedProducts: [product], settings: value.settings, deadlineMs: 90000 }); await saveJob({}); }
      const copy = await paid("copy", async requestId => {
        const receipt = ref.collection("outputs").doc(jobId + "_copy"), stored = await receipt.get();
        const prior = stored.exists && stored.data().rawResponse;
        const request = prior ? null : deps.research.buildRequest({ evidence: job.evidence, feedback: value.settings.direction, currentCreative: group.original || {}, style: value.settings.style, sourceImageDataUrl: "data:image/jpeg;base64," + sourceFiles[0].toString("base64"), referenceImages: references.slice(0, 3).map((reference, index) => ({ id: reference.id, dataUrl: "data:image/jpeg;base64," + sourceFiles[selectedImages.length + index].toString("base64") })) });
        const result = prior || await deps.responses(request, requestId);
        await writeReceipt("copy", { rawResponse: result, receivedAt: Date.now() });
        let output; try { output = deps.research.validateResult({ output: JSON.parse(responseText(result)), evidence: job.evidence, channel: group.channel, group }); }
        catch (error) { error.definiteResponse = true; throw error; }
        return { ...output, usage: result.usage || {}, responseId: result.id || null, providerModel: result.model || request && request.model || "gpt-6-astra", estimatedUsd: result.estimatedUsd == null ? 1 : result.estimatedUsd, costEstimated: result.costEstimated !== false };
      });
      await progress(25, "Copy and visual direction saved; composing each required format");
      const wanted = formats().filter(format => value.settings.formats.includes(format.key));
      for (let i = 0; i < wanted.length; i++) {
        const format = wanted[i];
        if (Date.now() - started > 540000) { await saveJob({ phase: "paused", progress: { pct: job.progress.pct, label: "Saved all completed work; continuing the remaining formats" }, leaseUntil: 0 }); return { ok: true, paused: true, dispatch: true, workspaceId, jobId }; }
        await progress(30 + Math.floor(i / wanted.length * 45), "Designing " + format.label.toLowerCase() + " — " + format.width + " × " + format.height);
        const output = await paid("image_" + format.key, async requestId => {
          const receipt = ref.collection("outputs").doc(jobId + "_image_" + format.key), stored = await receipt.get();
          if (stored.exists && stored.data().asset) return stored.data();
          const image = await deps.generateImage({ requestId, provider: provider(), format, references: sourceFiles, product, brief: copy.brief, imageDirections: copy.imageDirections, settings: value.settings, inputCoverage: job.inputCoverage });
          const asset = await deps.saveAsset(workspaceId, image.bytes, jobId.replace(/-/g, "") + "_" + format.key, { width: format.width, height: format.height, kind: format.key, providerModel: provider().model });
          const saved = { asset, usage: image.usage || {}, estimatedUsd: image.estimatedUsd == null ? 1 : image.estimatedUsd, costEstimated: image.costEstimated !== false, providerModel: provider().model };
          await writeReceipt("image_" + format.key, saved); return saved;
        }); job.assets[format.key] = output.asset; await saveJob({});
      }
      if (Date.now() - started > 620000) { await saveJob({ phase: "paused", leaseUntil: 0, progress: { pct: job.progress.pct, label: "Every format is saved; continuing the quality review" } }); return { ok: true, paused: true, dispatch: true, workspaceId, jobId }; }
      await progress(82, "Checking product fidelity, framing and mobile readability");
      const quality = await paid("quality", async requestId => {
        const files = await Promise.all(wanted.map(format => deps.loadAsset(job.assets[format.key]))), output = await deps.reviewImages(sourceFiles[0], files, { ...copy.brief, copy: copy.copy, keywords: group.keywords || product.keywords || [], settings: value.settings, product: product.title, inputCoverage: job.inputCoverage }, sourceFiles, requestId);
        await writeReceipt("quality", { stageResult: output, receivedAt: Date.now() });
        return { ...output, estimatedUsd: output.estimatedUsd == null ? 1 : output.estimatedUsd, costEstimated: output.costEstimated !== false };
      });
      if (quality.pass !== true || quality.productFaithful !== true || quality.mobileReadable !== true || Number(quality.score) < 85) throw new Error("The generated design needs changes to product fidelity, framing or mobile clarity before it can enter Approvals.");
      job.result = { brief: copy.brief, copy: copy.copy, assets: job.assets, evidence: job.evidence, keywords: group.keywords || product.keywords || [], quality, learningApplications: copy.learningApplications || [], sourceIds: copy.sourceIds || [], inputCoverage: job.inputCoverage, designSettings: value.settings, previewOnlyFormats: group.channel === "search" ? ["portrait"] : [] }; await saveJob({});
      await progress(94, "Saving the exact copy and images for your approval");
      if (!job.approvalId) { const result = await deps.finish({ workspaceId, jobId, owner, workspace: value, group, product, result: job.result }); job.approvalId = result.approvalId; if (!job.approvalId) throw new Error("The finished design could not be added to Approvals."); await saveJob({}); }
      await saveJob({ phase: "ready", completedAt: Date.now(), leaseUntil: 0, inFlight: null, error: null, progress: { pct: 100, label: "Ready in Approvals — review the exact copy and every image" } });
      return { ok: true, workspaceId, approvalId: job.approvalId };
    } catch (error) {
      await saveJob({ phase: "needs_attention", error: String(error.message || error).slice(0, 900), leaseUntil: 0, progress: { pct: Number(job.progress && job.progress.pct) || 0, label: "Saved work retained — review the unfinished step" } }); throw error;
    }
  }
  return { workspace, save, upload, start, status, run };
}
module.exports = { createAdDesignService, buildVersionDesignPayload, FORMATS, settingsFor, responseText, MAX_UPLOAD };
