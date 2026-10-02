'use strict';

// Read-only catalogue audit. Merchant data and review drafts are written outside
// this repository. Findings are hypotheses for review, never mutation commands.
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');

const SCHEMA = 1;
const PRODUCT_ID = /^gid:\/\/shopify\/Product\/\d+$/;
const VARIANT_ID = /^gid:\/\/shopify\/ProductVariant\/\d+$/;
const FAMILY_WORDS = /\b(necklaces?|bracelets?|earrings?|studs?|huggies?|hoops?|rings?|charms?|keychains?)\b/gi;
const TYPE_VALUES = { necklace: 'Necklace', bracelet: 'Bracelets', earrings: 'Earrings', ring: 'Rings', charm: 'Charm', keychain: 'Keychain' };
const INITIAL_OPTION = /\b(initial|letter|monogram|alphabet)s?\b/i;
const LENGTH_OPTION = /\b(length|lengths)\b/i;
const CUSTOM_OPTION = /\b(upload|artwork|image|photo|drawing|design|handwriting|personalization|personalisation|custom text|name|message)\b/i;
const MOJIBAKE = /\uFFFD|Ã[\u0080-\u00BF]|Â[\u0080-\u00BF]|â(?:€|\u0080)|ðŸ|‚Ä[îôúù]|ï¿½/g;
const clean = value => String(value == null ? '' : value).replace(/\u0000/g, '').trim();
const hash = value => crypto.createHash('sha256').update(typeof value === 'string' ? value : JSON.stringify(value)).digest('hex');

function textOf(value) {
  return clean(value).replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, ' ').replace(/<style\b[^>]*>[\s\S]*?<\/style>/gi, ' ')
    .replace(/<[^>]+>/g, ' ').replace(/&nbsp;|&#160;/g, ' ').replace(/&amp;/g, '&').replace(/&quot;/g, '"')
    .replace(/&#39;|&apos;/g, "'").replace(/\s+/g, ' ').trim();
}
function canonicalUrl(product) {
  try {
    const url = new URL(product.url || product.onlineStoreUrl);
    if (url.protocol !== 'https:' || !['britesjewelry.com', 'www.britesjewelry.com'].includes(url.hostname) || url.username || url.password || url.port) return null;
    if (url.pathname !== '/products/' + product.handle) return null;
    return 'https://britesjewelry.com' + url.pathname;
  } catch { return null; }
}
function familyOfWord(word) {
  const w = word.toLowerCase();
  if (/^(earring|stud|huggie|hoop)/.test(w)) return 'earrings';
  return w.replace(/s$/, '');
}
function explicitFamilies(text) {
  const matches = [...clean(text).matchAll(FAMILY_WORDS)];
  return matches.map(m => ({ family: familyOfWord(m[1]), word: m[1], index: m.index }));
}
function primaryFamily(text) {
  const words = explicitFamilies(text);
  return words.length ? words[words.length - 1].family : null;
}
function variantsOf(product) {
  return Array.isArray(product.variants) ? product.variants : product.variants?.nodes || [];
}
function optionsOf(product) {
  const map = new Map();
  const add = (name, value) => {
    name = clean(name); value = clean(value);
    if (!name || !value) return;
    if (!map.has(name.toLowerCase())) map.set(name.toLowerCase(), { name, values: new Set() });
    map.get(name.toLowerCase()).values.add(value);
  };
  for (const option of product.options || []) for (const value of option.values || []) add(option.name, value);
  for (const variant of variantsOf(product)) for (const option of variant.options || variant.selectedOptions || []) add(option.name, option.value);
  return [...map.values()].map(o => ({ name: o.name, values: [...o.values] }));
}
function optionSummary(options) {
  return options.map(o => o.name + ': ' + o.values.join(' | ')).join('; ');
}
function evidence(product, field, quote, variantIds = []) {
  return { url: canonicalUrl(product), field, quote: clean(quote).slice(0, 750), checkedAt: Number.isFinite(product.checkedAt) ? product.checkedAt : null, variantIds: [...new Set(variantIds)] };
}
function snippet(text, match, radius = 85) {
  const index = typeof match.index === 'number' ? match.index : text.indexOf(match[0]);
  return text.slice(Math.max(0, index - radius), Math.min(text.length, index + match[0].length + radius));
}
function isStudioService(product) {
  return /custom charm studio/i.test(product.type || product.productType || '') && /(?:credits?|membership|design fee|engraving)/i.test(product.title || '');
}
function detectProductFindings(product) {
  const findings = [], description = textOf(product.descriptionHtml || product.description), title = clean(product.title), type = clean(product.type || product.productType);
  const titleFamily = primaryFamily(title), typeFamily = primaryFamily(type), options = optionsOf(product), variants = variantsOf(product);
  const variantIds = variants.map(v => v.id).filter(id => VARIANT_ID.test(id || ''));
  const add = (code, priority, detail, proofs, suggestion = null, limits = []) => {
    findings.push({ id: 'catalogue-' + hash(product.id + ':' + code).slice(0, 20), code, productId: product.id, handle: product.handle,
      title, canonicalUrl: canonicalUrl(product), priority, basis: 'heuristic', reviewState: 'requires_inspection',
      detail, evidence: proofs, proposal: suggestion, limitations: limits,
      shopperHolds: { recommendation: false, cart: false, meaning: false } });
  };
  if (!PRODUCT_ID.test(product.id || '') || !canonicalUrl(product)) {
    add('invalid_product_identity', 'P1', 'Product ID or canonical product URL cannot be bound to this catalogue record.', [evidence(product, 'identity', 'Product ID: ' + clean(product.id) + '; handle: ' + clean(product.handle) + '; URL: ' + clean(product.url))]);
  }
  if (!isStudioService(product) && titleFamily && typeFamily && titleFamily !== typeFamily) {
    const multiFormat = /\b(necklace|bracelet|earring|ring|charm)s?\s*[\/&]\s*(necklace|bracelet|earring|ring|charm)s?\b/i.test(title);
    const attachment = options.find(o => /\bcharm type\b/i.test(o.name));
    const optionFamilies = new Set();
    if (attachment && attachment.values.length && attachment.values.every(v => /\bcharms?\b/i.test(v))) optionFamilies.add('charm');
    if (options.some(o => /\bbracelet\s+length\b/i.test(o.name))) optionFamilies.add('bracelet');
    if (options.some(o => /\b(?:necklace|chain)\s+length\b/i.test(o.name))) optionFamilies.add('necklace');
    if (options.some(o => /\bhoop\s+size\b/i.test(o.name))) optionFamilies.add('earrings');
    if (options.some(o => /\bring\s+size\b/i.test(o.name))) optionFamilies.add('ring');
    const optionsSupportCurrentType = optionFamilies.size === 1 && optionFamilies.has(typeFamily);
    const customCategory = /^custom charm$/i.test(type);
    const proposal = multiFormat || customCategory ? null : optionsSupportCurrentType
      ? { field: 'title', instruction: 'Review included carrier and physical format: selectable option names support the current product type, so do not replace the type based on the title alone.', applyAutomatically: false }
      : { field: 'product_type', current: type, candidate: TYPE_VALUES[titleFamily], applyAutomatically: false };
    add('title_type_conflict', customCategory ? 'P2' : 'P1', 'The title ends in explicit ' + titleFamily + ' wording, while product type is ' + type + '.',
      [evidence(product, 'title', title), evidence(product, 'product_type', type), evidence(product, 'options', optionSummary(options), variantIds)],
      proposal,
      ['Explicit words are compared; physical format and existing smart-collection effects require review.',
        ...(multiFormat ? ['The title advertises multiple formats; a single replacement type is not proposed.'] : []),
        ...(optionsSupportCurrentType ? ['Selectable option wording supports the current type; title wording needs inspection before any type change.'] : []),
        ...(customCategory ? ['Custom Charm may be an intentional workflow/business category spanning physical formats; no replacement type is proposed.'] : [])]);
  }
  const subjectMatches = [...description.slice(0, 850).matchAll(/\b(?:this|these|our|each|the)\s+(?:(?:stunning|beautiful|dainty|delicate|elegant|unique|charming|exquisite|handmade|lovely|mini|small|gold|silver)\s+){0,4}(necklaces?|bracelets?|earrings?|rings?|keychains?)\b/gi)];
  const mismatched = titleFamily && subjectMatches.find(m => familyOfWord(m[1]) !== titleFamily && titleFamily !== 'charm' && !/\b(?:pair|layer|style|wear|combine|match)\w*\s+(?:it|them)?\s*with\b/i.test(snippet(description, m, 35)));
  if (mismatched && !isStudioService(product)) add('title_description_type_conflict', 'P1', 'The opening description refers to a different explicit product format than the title.',
    [evidence(product, 'title', title), evidence(product, 'description', snippet(description, mismatched)), evidence(product, 'options', optionSummary(options), variantIds)],
    { field: 'descriptionHtml', currentExcerpt: mismatched[0], instruction: 'Confirm the physical format, then correct only the contradictory format phrase.', applyAutomatically: false }, ['A copied sentence does not prove the physical product is wrong; inspect actual options and storefront.']);
  if (/\bstuds?\b/i.test(title)) {
    const moving = [...description.matchAll(/\b(?:suspended from (?:elegant )?ear wires|ear wires|earwires|french (?:ear )?hooks?|ear hooks?|hook fastenings?|hook closures?|suspended (?:from|on) (?:elegant )?hooks?|dangle earrings|drop earrings|dangling earrings)\b/gi)]
      .find(m => !/\b(?:pair|style|layer|combine|wear|match)\w*\b.{0,35}\b(?:with|alongside)\b/i.test(snippet(description, m, 70)));
    if (moving) add('stud_fastening_copy_conflict', 'P1', 'Stud wording conflicts with explicit ear-wire or dangle wording in the description.',
      [evidence(product, 'title', title), evidence(product, 'description', snippet(description, moving)), evidence(product, 'options', optionSummary(options), variantIds)],
      { field: 'descriptionHtml', instruction: 'Inspect fastening and actual shipped style before correcting stud versus hook/drop wording.', applyAutomatically: false }, ['No fastening or pictured motif is inferred from an uninspected image.']);
  }
  const attachment = options.find(o => /\bcharm type\b/i.test(o.name));
  if (attachment && /\bnecklace\b/i.test(description) && !/\bnecklace\b/i.test(title)) {
    add('charm_necklace_format_review', 'P2', 'Description markets a necklace while selectable attachment values identify charm formats.',
      [evidence(product, 'title', title), evidence(product, 'description', description.slice(0, 250)), evidence(product, 'options', optionSummary([attachment]), variantIds)],
      { field: 'descriptionHtml', instruction: 'Explain charm-only, charm-pair and included-carrier choices explicitly; verify chain/hoop inclusion before promising it.', applyAutomatically: false }, ['A charm may be intended for a necklace; these words alone do not prove a product identity defect.']);
  }
  const fixedInitial = /(?:^|\s)[A-Z]\s+(?:initial|letter)\b/i.test(title);
  const initialPrompt = /\b(?:choose|select|pick|specify|enter|tell us)\s+(?:your\s+|the\s+|a\s+|an\s+)?(?:initials?|letters?|monograms?)\b/i.exec(description);
  const initialProduct = /\b(initials?|monogram|alphabet)\b/i.test(title) && !fixedInitial;
  const initialOption = options.some(o => INITIAL_OPTION.test(o.name) || (o.values.length > 1 && o.values.every(v => /^(?:letter |initial )?[a-z]$/i.test(v))));
  if (!isStudioService(product) && (initialProduct || initialPrompt) && !initialOption) add('initial_selection_not_in_catalogue', 'P1', 'An initial/monogram choice is advertised but no initial/letter choice appears in the exported selectable options.',
    [evidence(product, 'title', title), ...(initialPrompt ? [evidence(product, 'description', snippet(description, initialPrompt))] : []), evidence(product, 'options', optionSummary(options) || 'No exported selectable options.', variantIds)],
    { field: 'selectionWorkflow', requiredInput: 'initial_or_letter', instruction: 'Inspect storefront line-item properties and existing customization app; expose the required selection in the concierge flow if supported.', applyAutomatically: false }, ['Catalogue options do not include theme controls or line-item properties; absence from this export is not proof that the website lacks a customization field.']);
  const lengthPrompt = /\b(?:choose|select|specify|pick)\s+(?:your\s+|the\s+)?(?:(?:necklace|chain|bracelet)\s+)?length\b|\b(?:custom|customized|customised|longer)\s+(?:chain\s+)?length\b/i.exec(title + '. ' + description);
  const hasLength = options.some(o => LENGTH_OPTION.test(o.name));
  if (!isStudioService(product) && lengthPrompt && !hasLength) add('length_selection_not_in_catalogue', 'P1', 'Length selection/custom length is advertised but no exported Length option is present.',
    [evidence(product, 'description', snippet(title + '. ' + description, lengthPrompt)), evidence(product, 'options', optionSummary(options), variantIds)],
    { field: 'selectionWorkflow', requiredInput: 'length', instruction: 'Check whether a fixed length, separate length add-on, or storefront property supplies this choice before changing options.', applyAutomatically: false }, ['Upsell links and external customization controls may legitimately supply the missing choice.']);
  const uploadPrompt = /\b(?:upload|attach|send|submit)\s+(?:(?:us|your|a|an|the|original|own)\s+){0,3}(?:image|photo|picture|drawing|artwork|handwriting|design|file)\b/i.exec(description);
  const customProduct = /\b(?:custom charm|custom design|handwriting|fingerprint|(?:custom|personalized|personalised)\s+(?:photo|portrait))\b/i.test(title) && !isStudioService(product);
  const customOption = options.some(o => CUSTOM_OPTION.test(o.name));
  if ((uploadPrompt || customProduct) && !customOption) add('custom_input_not_in_catalogue', 'P1', 'A custom/uploaded design workflow is advertised but design/file input is not represented in selectable catalogue options.',
    [evidence(product, 'title', title), ...(uploadPrompt ? [evidence(product, 'description', snippet(description, uploadPrompt))] : []), evidence(product, 'options', optionSummary(options) || 'No exported selectable options.', variantIds)],
    { field: 'selectionWorkflow', requiredInput: uploadPrompt ? 'upload_or_design_reference' : 'custom_design_reference', instruction: 'Inspect the existing upload/custom studio and property schema; require a valid design/file reference before concierge cart confirmation.', applyAutomatically: false }, ['Uploads and personalization usually live outside Shopify variants; this is an integration review, not proof of an absent storefront form.']);
  const range = /\b(?:available\s+)?(?:necklace|chain)\s+lengths?\s*(?:are|is|:)?\s*(\d+(?:\.\d+)?)\s*[-–]\s*(\d+(?:\.\d+)?)\s*(?:inches|inch|in\b|")/i.exec(description);
  if (range) {
    const low = Number(range[1]), high = Number(range[2]);
    const values = options.filter(o => LENGTH_OPTION.test(o.name) && !/bracelet|post/i.test(o.name)).flatMap(o => o.values);
    const outside = values.filter(v => /(?:inch|\bin\b|")/i.test(v)).filter(v => { const n = Number.parseFloat(v); return Number.isFinite(n) && (n < low || n > high); });
    if (outside.length) add('claimed_length_range_conflict', 'P2', 'Explicit inch-length range in the description excludes a currently selectable inch-length value.',
      [evidence(product, 'description', range[0]), evidence(product, 'options', 'Selectable lengths outside stated range: ' + outside.join(' | '), variantIds)],
      { field: 'descriptionHtml', currentExcerpt: range[0], instruction: 'Reconcile the stated range against current selectable lengths and actual chain fulfillment.', applyAutomatically: false });
  }
  const metal = options.find(o => /\bmetal\b/i.test(o.name));
  const roseClaim = /\b(?:available|choice|crafted)[^.]{0,160}\brose gold(?: filled)?\b/i.exec(description.slice(0, 1000));
  if (roseClaim && metal && !metal.values.some(v => /rose gold/i.test(v))) add('described_metal_not_selectable', 'P2', 'Opening copy offers rose gold, but no Rose Gold value exists in the exported Metal options.',
    [evidence(product, 'description', roseClaim[0]), evidence(product, 'options', optionSummary([metal]), variantIds)],
    { field: 'descriptionHtml', instruction: 'Verify whether the variant was intentionally removed; align availability wording with live offered metals.', applyAutomatically: false }, ['This does not invalidate selectable metals or establish physical composition.']);
  for (const [field, value] of [['title', title], ['descriptionHtml', description]]) {
    const bad = [...value.matchAll(MOJIBAKE)];
    if (bad.length) add('broken_encoding_' + field, 'P2', 'Recognizable replacement/mojibake sequences occur in ' + field + '.',
      bad.slice(0, 5).map(m => evidence(product, field, snippet(value, m))),
      { field, instruction: 'Restore intended punctuation/characters from the original text and review the rendered listing; preserve legitimate accents and multilingual text.', applyAutomatically: false }, ['A readable replacement is not invented for unknown corrupted bytes.']);
  }
  return findings;
}

function skuCollisions(products) {
  const exact = new Map(), caseFolded = new Map();
  let totalVariants = 0, missingSkuVariants = 0;
  for (const product of products) for (const variant of variantsOf(product)) {
    totalVariants++;
    const sku = clean(variant.sku);
    if (!sku) { missingSkuVariants++; continue; }
    const occurrence = { productId: product.id, variantId: variant.id, title: clean(product.title), handle: product.handle,
      canonicalUrl: canonicalUrl(product), variantTitle: clean(variant.title), sku, optionEvidence: (variant.options || variant.selectedOptions || []).map(o => ({ name: clean(o.name), value: clean(o.value) })),
      checkedAt: Number.isFinite(product.checkedAt) ? product.checkedAt : null };
    if (!exact.has(sku)) exact.set(sku, []);
    exact.get(sku).push(occurrence);
    const key = sku.toLocaleLowerCase('en-US');
    if (!caseFolded.has(key)) caseFolded.set(key, []);
    caseFolded.get(key).push(occurrence);
  }
  const group = (sku, occurrences, mode) => ({ sku, mode, occurrences: occurrences.length, productIds: [...new Set(occurrences.map(o => o.productId))], variantIds: [...new Set(occurrences.map(o => o.variantId))],
    risk: 'internal_matching_only', requiresReview: true, autoHold: false, evidence: occurrences });
  const reused = [...exact].filter(([, os]) => os.length > 1).map(([sku, os]) => group(sku, os, 'exact_trimmed'));
  const crossProduct = reused.filter(g => g.productIds.length > 1);
  const caseOnly = [...caseFolded].filter(([, os]) => new Set(os.map(o => o.sku)).size > 1 && new Set(os.map(o => o.productId)).size > 1).map(([sku, os]) => group(sku, os, 'case_folded_alias'));
  const compare = (a, b) => b.productIds.length - a.productIds.length || b.occurrences - a.occurrences || a.sku.localeCompare(b.sku);
  return { schema: SCHEMA, caveat: 'SKU reuse is an internal matching ambiguity. It never alone proves the motif, style, stock or selectable offer is wrong, and does not create shopper holds.',
    counts: { totalVariants, missingSkuVariants, uniqueNonblankExactSkus: exact.size, reusedExactSkuGroups: reused.length, crossProductExactSkuGroups: crossProduct.length,
      crossProductCaseAliasGroups: caseOnly.length, crossProductAffectedProducts: new Set(crossProduct.flatMap(g => g.productIds)).size, crossProductAffectedVariants: new Set(crossProduct.flatMap(g => g.variantIds)).size },
    crossProductGroups: crossProduct.sort(compare), withinProductGroups: reused.filter(g => g.productIds.length === 1).sort(compare), caseAliasGroups: caseOnly.sort(compare) };
}

function listingDrafts(findings) {
  const groups = new Map();
  for (const finding of findings) {
    if (!groups.has(finding.productId)) groups.set(finding.productId, []);
    groups.get(finding.productId).push(finding);
  }
  return [...groups].map(([productId, issues]) => {
    const priority = issues.some(i => i.priority === 'P1') ? 'P1' : 'P2';
    const typeChange = issues.find(i => i.code === 'title_type_conflict' && i.proposal?.candidate);
    return { id: 'listing-review-' + hash(productId).slice(0, 20), productId, handle: issues[0].handle, title: issues[0].title, canonicalUrl: issues[0].canonicalUrl,
      priority, basis: 'hypothesis', reviewState: 'requires_inspection', issueIds: issues.map(i => i.id),
      proposals: issues.map(i => ({ issueId: i.id, code: i.code, ...i.proposal })).filter(p => p.field),
      editorActionDrafts: typeChange ? [{ action: 'updateProductFields', body: { product_id: productId, product_type: typeChange.proposal.candidate }, expectedCurrent: { product_type: typeChange.proposal.current },
        supportedBy: 'shopifyEditor updateProductFields', executable: false, applyAutomatically: false, requiredBeforeApply: ['Verify the exact current product/variant identity and physical format.', 'Review smart collections affected by product_type.', 'Read live state again and reject a changed expected value.'] }] : [],
      evidence: issues.flatMap(i => i.evidence), measurement: ['Correct variant, format, included carrier and customization selection in a sandbox browser.', 'Verify relevant organic/paid queries and purchase attribution before measuring conversion change.'],
      productionMutations: false };
  }).sort((a, b) => a.priority.localeCompare(b.priority) || b.issueIds.length - a.issueIds.length || a.productId.localeCompare(b.productId));
}
function auditCatalogue(catalogue, generatedAt = Date.now()) {
  const products = Array.isArray(catalogue) ? catalogue : catalogue?.products;
  if (!Array.isArray(products)) throw new Error('Catalogue products array is required.');
  const findings = products.flatMap(detectProductFindings), collisions = skuCollisions(products), drafts = listingDrafts(findings);
  const byCode = {}, byPriority = {};
  for (const finding of findings) { byCode[finding.code] = (byCode[finding.code] || 0) + 1; byPriority[finding.priority] = (byPriority[finding.priority] || 0) + 1; }
  return { schema: SCHEMA, private: true, mode: 'read_only_review_drafts', generatedAt, sourceComplete: Array.isArray(catalogue) ? null : catalogue.complete === true,
    sourceFinishedAt: Number.isFinite(catalogue?.finishedAt) ? catalogue.finishedAt : null, sourceHash: hash(catalogue),
    summary: { products: products.length, studioServiceProducts: products.filter(isStudioService).length, findings: findings.length,
      affectedProducts: new Set(findings.map(f => f.productId)).size, findingsByCode: byCode, findingsByPriority: byPriority, listingDrafts: drafts.length, ...collisions.counts, automaticShopperHolds: 0, productionMutations: 0 },
    limitations: ['Heuristic findings require review; no image motif is inferred.', 'Variant option exports omit theme controls, uploads and line-item properties.', 'SKU collisions are internal matching risks and never alone block recommendations.', 'No search demand, sales uplift or competitor spending is inferred from catalogue defects.'], findings, collisions, drafts };
}

function privateOutputPath(out, repoRoot = path.resolve(__dirname, '..')) {
  const target = path.resolve(out), relative = path.relative(path.resolve(repoRoot), target);
  if (!relative || (!relative.startsWith('..' + path.sep) && relative !== '..' && !path.isAbsolute(relative))) throw new Error('Private audit output must be outside the Git repository.');
  return target;
}
function writeAudit(audit, out) {
  out = privateOutputPath(out);
  fs.mkdirSync(out, { recursive: true, mode: 0o700 });
  const put = (name, data) => fs.writeFileSync(path.join(out, name), JSON.stringify(data, null, 2) + '\n', { mode: 0o600 });
  put('summary.json', { schema: audit.schema, private: true, generatedAt: audit.generatedAt, sourceHash: audit.sourceHash, sourceComplete: audit.sourceComplete, summary: audit.summary, limitations: audit.limitations });
  put('findings.json', { schema: audit.schema, private: true, generatedAt: audit.generatedAt, findings: audit.findings });
  put('sku-collisions.json', audit.collisions);
  put('listing-app-drafts.json', { schema: audit.schema, private: true, generatedAt: audit.generatedAt, mode: 'review_only', productionMutations: false, drafts: audit.drafts });
  return out;
}
function main(args = process.argv.slice(2)) {
  const flags = {};
  for (let i = 0; i < args.length; i += 2) {
    if (!['--input', '--out'].includes(args[i]) || !args[i + 1] || flags[args[i]]) throw new Error('Usage: node scripts/audit-growth-catalogue.cjs [--input catalogue.json] [--out private-directory]');
    flags[args[i]] = args[i + 1];
  }
  const input = path.resolve(flags['--input'] || path.join(__dirname, '../../private-growth/live-catalogue.json'));
  const out = privateOutputPath(flags['--out'] || path.join(__dirname, '../../private-growth/catalogue-audit'));
  const catalogue = JSON.parse(fs.readFileSync(input, 'utf8'));
  if (catalogue.complete !== true) throw new Error('Refusing a systemic catalogue audit without a completed catalogue export.');
  const audit = auditCatalogue(catalogue);
  writeAudit(audit, out);
  process.stdout.write(JSON.stringify({ output: out, ...audit.summary }) + '\n');
}
if (require.main === module) { try { main(); } catch (error) { process.stderr.write(error.message + '\n'); process.exitCode = 1; } }
module.exports = { SCHEMA, textOf, canonicalUrl, primaryFamily, optionsOf, isStudioService, detectProductFindings, skuCollisions, listingDrafts, auditCatalogue, privateOutputPath, writeAudit, main };
