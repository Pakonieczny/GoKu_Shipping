// Saved editable Google Ads state. Pure planning only: this module never calls
// Google or writes a document. A restore must pass the normal approval pipeline.
const crypto = require("crypto");
const SCHEMA = 1;
const COMPONENTS = ["searchAds", "searchImageLinks", "keywords", "campaignNegatives", "assetGroups", "assetLinks", "listingGroups"];
function stable(v) {
  if (Array.isArray(v)) return v.map(stable);
  if (v && typeof v === "object") return Object.keys(v).sort().reduce((out, key) => { if (v[key] !== undefined) out[key] = stable(v[key]); return out; }, {});
  return v;
}
const json = v => JSON.stringify(stable(v));
const equal = (a, b) => json(a) === json(b);
function snapshotHash(snapshot) {
  if (!snapshot || snapshot.schema !== SCHEMA) return null;
  // Timestamps and image preview URLs are observations, not editable ad state.
  const components = {};
  COMPONENTS.forEach(key => { components[key] = (snapshot.components[key] || []).map(row => {
    const { imageUrl, ...editable } = row; return editable;
  }).sort((a, b) => String(a.resourceName).localeCompare(String(b.resourceName))); });
  return crypto.createHash("sha256").update(json({ schema: SCHEMA, campaignId: String(snapshot.campaignId), channel: snapshot.channel, components })).digest("hex");
}
function restorationEligibility(snapshot) {
  if (!snapshot || snapshot.schema !== SCHEMA) return { restorable: false, restorationReason: "This version has a change log only; its exact editable settings were not saved." };
  if (!snapshot.complete) return { restorable: false, restorationReason: "Some settings could not be saved completely. A partial snapshot cannot be restored." };
  if (!["SEARCH", "PERFORMANCE_MAX"].includes(snapshot.channel)) return { restorable: false, restorationReason: "Restoration is available for Search and Performance Max creative." };
  if(snapshot.channel==="SEARCH"&&!Array.isArray((snapshot.components||{}).searchImageLinks))return {restorable:false,restorationReason:"This older snapshot did not save Search image links and cannot restore the complete creative exactly."};
  return { restorable: true, restorationReason: "Saved copy, links, keywords and product selection can be reviewed for restoration on the existing campaign and ads. Budgets, schedules and Google's learned bidding state are not rolled back." };
}
function assertComplete(snapshot) {
  const state = restorationEligibility(snapshot); if (!state.restorable) throw new Error(state.restorationReason);
  COMPONENTS.forEach(key => { if (key==="searchImageLinks"&&snapshot.channel!=="SEARCH")return;if (!Array.isArray(snapshot.components[key])) throw new Error("Saved settings are missing the " + key + " component."); });
}
function assertIdentity(rows, expected, kind) {
  const current = new Set(rows.map(x => x.resourceName)), target = new Set(expected.map(x => x.resourceName));
  if (current.size !== rows.length || target.size !== expected.length) throw new Error("Duplicate " + kind + " identities require reconciliation before restoration.");
  if (current.size !== target.size || [...target].some(ref => !current.has(ref))) throw new Error("The " + kind + " set has changed or an original resource was removed. This version cannot be restored while preserving its existing IDs.");
}
function resourceScope(snapshot) {
  const refs = COMPONENTS.flatMap(key => (snapshot.components[key]||[]).flatMap(row => [row.resourceName, row.adGroup, row.adGroupAdResourceName, row.assetGroup, row.asset, row.campaign, row.parentListingGroupFilter].filter(Boolean)));
  const ids = new Set(refs.map(ref => String(ref).match(/^customers\/(\d+)\//)).filter(Boolean).map(m => m[1]));
  if (ids.size !== 1 || refs.some(ref => !/^customers\/\d+\/[A-Za-z]+\/\d+(?:~[A-Z_0-9]+)*$/.test(String(ref)))) throw new Error("Saved settings contain an invalid or cross-account resource.");
  return [...ids][0];
}
function semanticKeyword(row) { return json([row.adGroup || row.campaign, !!row.negative, row.keyword]); }
function listingTree(rows) {
  const byRef = new Map(rows.map(row => [row.resourceName, row]));
  const path = (row, seen = new Set()) => {
    if (seen.has(row.resourceName)) throw new Error("Saved product selection contains a cycle.");
    seen.add(row.resourceName);
    if (row.parentListingGroupFilter && !byRef.has(row.parentListingGroupFilter)) throw new Error("Saved product selection is missing a parent.");
    if (!["SUBDIVISION", "UNIT_INCLUDED", "UNIT_EXCLUDED"].includes(row.type) || row.listingSource !== "SHOPPING") throw new Error("This saved product selection uses an unsupported listing type.");
    return (row.parentListingGroupFilter ? path(byRef.get(row.parentListingGroupFilter), seen) + "/" : "") + json([row.type, row.caseValue || null]);
  };
  return rows.map(row => ({ row, path: path(row), depth: (() => { let n = 0, r = row; while (r.parentListingGroupFilter) { n++; r = byRef.get(r.parentListingGroupFilter); } return n; })() }));
}
function buildRestoreOperations(current, target) {
  assertComplete(current); assertComplete(target);
  if (String(current.campaignId) !== String(target.campaignId) || current.channel !== target.channel) throw new Error("A version can only be restored to its original campaign and channel.");
  const customer = resourceScope(current); if (resourceScope(target) !== customer) throw new Error("A version can only be restored in its original account.");
  const c = current.components, t = target.components, operations = [], changes = [], notes = [];
  const add = (category, before, after, reason) => changes.push({ category, before, after, reason: reason || "Restore the exact settings recorded in the selected version." });
  const summary = rows => rows.map(row => ({ ...row }));
  if (current.channel === "SEARCH") {
    assertIdentity(c.searchAds, t.searchAds, "Search ad");
    for (const ad of t.searchAds) {
      const prior = c.searchAds.find(x => x.resourceName === ad.resourceName);
      if (ad.adGroup !== prior.adGroup || ad.adGroupAdResourceName !== prior.adGroupAdResourceName) throw new Error("The original Search ad group identity no longer matches.");
      const fields = ["finalUrls", "finalMobileUrls", "responsiveSearchAd"], changed = fields.filter(key => !equal(prior[key], ad[key]));
      if (changed.length) {
        const update = { resourceName: ad.resourceName };
        changed.forEach(key => { update[key] = ad[key]; });
        const mask = changed.flatMap(key => key === "responsiveSearchAd" ? ["responsive_search_ad.headlines", "responsive_search_ad.descriptions", "responsive_search_ad.path1", "responsive_search_ad.path2"] : key === "finalUrls" ? ["final_urls"] : ["final_mobile_urls"]);
        operations.push({ adOperation: { update, updateMask: mask.join(",") } });
        add("Search copy and links", { resourceName: ad.resourceName, ...Object.fromEntries(changed.map(key => [key, prior[key]])) }, { resourceName: ad.resourceName, ...Object.fromEntries(changed.map(key => [key, ad[key]])) });
      }
      if (ad.status !== prior.status) {
        if (!["ENABLED", "PAUSED"].includes(ad.status)) throw new Error("A removed ad cannot be restored under its former Google ID.");
        operations.push({ adGroupAdOperation: { update: { resourceName: ad.adGroupAdResourceName, status: ad.status }, updateMask: "status" } });
        add("Ad status", prior.status, ad.status);
      }
    }
    const imageKey = row => json([row.adGroup, row.asset, row.fieldType]);
    const imageLinks = components => {
      const groups = new Set([...components.searchAds, ...components.keywords].map(row => row.adGroup)), links = new Map();
      for (const row of components.searchImageLinks) {
        if (row.fieldType !== "IMAGE") throw new Error("Only saved Search image assets can be restored by this version format.");
        if (!["ENABLED", "PAUSED"].includes(row.status)) throw new Error("Saved Search image status is missing or unsupported; it cannot be guessed during restoration.");
        const groupId = String(row.adGroup).match(/^customers\/\d+\/adGroups\/(\d+)$/), assetId = String(row.asset).match(/^customers\/\d+\/assets\/(\d+)$/);
        if (!groupId || !assetId || row.resourceName !== `customers/${customer}/adGroupAssets/${groupId[1]}~${assetId[1]}~IMAGE`) throw new Error("Saved Search image link identity does not match its original ad group and asset.");
        if (!groups.has(row.adGroup)) throw new Error("A saved Search image belongs to an ad group whose original editable state is unavailable.");
        const key = imageKey(row);
        if (links.has(key)) throw new Error("Duplicate Search image links require reconciliation before restoration.");
        links.set(key, row);
      }
      return links;
    };
    const oldImages = imageLinks(c), wantedImages = imageLinks(t);
    for(const [key,row]of oldImages)if(!wantedImages.has(key))operations.push({adGroupAssetOperation:{remove:row.resourceName}});
    for(const [key,row]of wantedImages){
      if(!oldImages.has(key))operations.push({adGroupAssetOperation:{create:{adGroup:row.adGroup,asset:row.asset,fieldType:"IMAGE",status:row.status}}});
      else if(oldImages.get(key).status!==row.status)operations.push({adGroupAssetOperation:{update:{resourceName:oldImages.get(key).resourceName,status:row.status},updateMask:"status"}});
    }
    if(operations.some(o=>o.adGroupAssetOperation))add("Search images",summary(c.searchImageLinks),summary(t.searchImageLinks),"Relink the saved square and landscape image assets to the same ad groups. Google controls whether an image is served.");
  } else {
    assertIdentity(c.assetGroups, t.assetGroups, "asset group");
    for (const group of t.assetGroups) {
      const prior = c.assetGroups.find(x => x.resourceName === group.resourceName), fields = ["finalUrls", "finalMobileUrls", "status"], changed = fields.filter(key => !equal(prior[key], group[key]));
      if (!changed.length) continue;
      if (!["ENABLED", "PAUSED"].includes(group.status)) throw new Error("A removed asset group cannot be restored under its former ID.");
      operations.push({ assetGroupOperation: { update: { resourceName: group.resourceName, ...Object.fromEntries(changed.map(key => [key, group[key]])) }, updateMask: changed.map(key => key === "finalUrls" ? "final_urls" : key === "finalMobileUrls" ? "final_mobile_urls" : key).join(",") } });
      add("Asset group links and status", { resourceName: group.resourceName, ...Object.fromEntries(changed.map(key => [key, prior[key]])) }, { resourceName: group.resourceName, ...Object.fromEntries(changed.map(key => [key, group[key]])) });
    }
    const key = row => json([row.assetGroup, row.asset, row.fieldType]);
    const oldLinks = new Map(c.assetLinks.map(row => [key(row), row])), wanted = new Map(t.assetLinks.map(row => [key(row), row]));
    for (const [k, row] of oldLinks) if (!wanted.has(k)) operations.push({ assetGroupAssetOperation: { remove: row.resourceName } });
    for (const [k, row] of wanted) {
      if (!oldLinks.has(k)) operations.push({ assetGroupAssetOperation: { create: { assetGroup: row.assetGroup, asset: row.asset, fieldType: row.fieldType, status: row.status || "ENABLED" } } });
      else if ((oldLinks.get(k).status || "ENABLED") !== (row.status || "ENABLED")) operations.push({ assetGroupAssetOperation: { update: { resourceName: oldLinks.get(k).resourceName, status: row.status || "ENABLED" }, updateMask: "status" } });
    }
    if (operations.some(o => o.assetGroupAssetOperation)) add("Images and creative assets", summary(c.assetLinks), summary(t.assetLinks), "Relink the exact saved assets to the existing asset groups. Google assembles the delivered ads and reviews serving eligibility.");
    for (const group of t.assetGroups) {
      const oldTree = listingTree(c.listingGroups.filter(row => row.assetGroup === group.resourceName)), wantedTree = listingTree(t.listingGroups.filter(row => row.assetGroup === group.resourceName));
      if (equal(oldTree.map(x => x.path).sort(), wantedTree.map(x => x.path).sort())) continue;
      if (!wantedTree.length && oldTree.length) throw new Error("A retail listing tree cannot be restored to an empty selection.");
      if (wantedTree.filter(x => !x.row.parentListingGroupFilter).length !== 1) throw new Error("Saved product selection must have exactly one root.");
      const id = group.resourceName.split("/").pop(), aliases = new Map();
      wantedTree.sort((a, b) => a.depth - b.depth || a.path.localeCompare(b.path)).forEach((x, index) => aliases.set(x.row.resourceName, `customers/${customer}/assetGroupListingGroupFilters/${id}~-${800001 + index}`));
      oldTree.sort((a, b) => b.depth - a.depth).forEach(x => operations.push({ assetGroupListingGroupFilterOperation: { remove: x.row.resourceName } }));
      wantedTree.forEach(({ row }) => operations.push({ assetGroupListingGroupFilterOperation: { create: {
        resourceName: aliases.get(row.resourceName), assetGroup: group.resourceName, type: row.type, listingSource: row.listingSource,
        ...(row.parentListingGroupFilter ? { parentListingGroupFilter: aliases.get(row.parentListingGroupFilter), caseValue: row.caseValue || {} } : {})
      } } }));
      add("Product choices", oldTree.map(x => x.row), wantedTree.map(x => x.row), "Restore the recorded include/exclude tree on the same asset group. Google requires replacement filter-node IDs when immutable partitions change.");
      notes.push("Product filter node IDs may change; the campaign and asset group IDs stay the same.");
    }
  }
  for (const component of ["keywords", "campaignNegatives"]) {
    const existing = new Map(c[component].map(row => [semanticKeyword(row), row])), wanted = new Map(t[component].map(row => [semanticKeyword(row), row]));
    const operation = component === "keywords" ? "adGroupCriterionOperation" : "campaignCriterionOperation";
    let changed = false;
    for (const [key, row] of existing) if (!wanted.has(key)) {
      // Positive keywords are paused instead of irreversibly removed so a later
      // approved restore can resume their original criterion IDs.
      if (!row.negative && component === "keywords") { if (row.status === "PAUSED") continue; operations.push({ [operation]: { update: { resourceName: row.resourceName, status: "PAUSED" }, updateMask: "status" } }); }
      else operations.push({ [operation]: { remove: row.resourceName } });
      changed = true;
    }
    for (const [key, row] of wanted) {
      const prior = existing.get(key);
      if (!prior) { operations.push({ [operation]: { create: { ...(component === "keywords" ? { adGroup: row.adGroup, status: row.status || "ENABLED" } : { campaign: row.campaign }), negative: !!row.negative, keyword: row.keyword } } }); changed = true; }
      else if (component === "keywords" && prior.status !== row.status) { operations.push({ [operation]: { update: { resourceName: prior.resourceName, status: row.status }, updateMask: "status" } }); changed = true; }
    }
    if (changed) add(component === "keywords" ? "Keywords" : "Campaign negatives", summary(c[component]), summary(t[component]), "Restore recorded keyword text, match type and enabled/paused selection. Later positive keywords are paused; removed immutable criteria receive new criterion IDs when re-added.");
  }
  if (operations.length > 500) throw new Error("This restoration needs more than 500 operations and cannot be sent as one complete reviewed update.");
  return { operations, changes, notes: [...new Set(notes)], identityPreserved: true };
}
module.exports = { SCHEMA, COMPONENTS, snapshotHash, restorationEligibility, buildRestoreOperations };
