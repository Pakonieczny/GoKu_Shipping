'use strict';

// This shopper projection contains merchant guidance and fixed, public shop
// reads only. It never reads an account, a private dossier or a discount API.
const policy = require('./_britesConcierge');
const HOME = 'https://britesjewelry.com/';
const MAX_HTML_BYTES = 1024 * 1024;
const tidy = (value, max = 3000) => String(value ?? '').replace(/\u0000/g, '').trim().slice(0, max);
const unsafeText = /(?:system|developer|assistant|author|internal|hidden)\s+(?:prompt|message|instructions?)|\bignore (?:all |any )?(?:prior|previous|system|developer) instructions?\b|\b(?:assistant|concierge|model)\s+(?:must|should|shall|needs? to)\b/i;

function isStudioCreditProduct(product) {
  const title = tidy(product?.title, 300).toLowerCase(), type = tidy(product?.type || product?.productType || product?.product_type, 100).toLowerCase();
  // "Custom Charm Studio" is also used for physical custom pieces and
  // engraving. Neither that category nor the hidden-collection description
  // alone establishes a credit product, and neither is a blanket exclusion.
  if (/^(?:custom\s+)?(?:necklaces?|earrings?|bracelets?|rings?|charms?|pendants?|chain extenders?)$/.test(type)) return false;
  const serviceTitle = /^(?:custom\s+charm\s+)?studio\s+(?:membership|(?:design\s+)?credit\s+packs?|design\s+packs?)\b/i.exec(title);
  if (!serviceTitle) return false;
  const remainder = title.slice(serviceTitle[0].length);
  if (/\b(?:necklaces?|earrings?|bracelets?|rings?|charms?|pendants?|chain extenders?|engraving)\b/.test(remainder)) return false;
  const statedCredits = /\b\d[\d,]*\s+(?:design\s+)?credits?\b|\b(?:monthly|subscription|per month)\b/.test(remainder);
  const studioCategory = /^(?:custom\s+charm\s+)?studio(?:\s+(?:membership|design\s+packs?|credits?))?$/.test(type);
  const studioPlanHandle = /^studio-(?:plan-[a-z0-9-]+|pack-\d+)$/.test(tidy(product?.handle, 180));
  return statedCredits || studioCategory && studioPlanHandle;
}

function merchantGuidance() {
  return {
    source: {kind: 'merchant_statement', title: 'Brites studio guidance', statedAt: '2026-10-07'},
    production: {min: 2, max: 3, unit: 'days', stage: 'production', shippingIncluded: false, needsConfirmation: true,
      summary: 'The Brites studio says production takes 2–3 days. Shipping transit is separate; a custom design and a requested deadline need confirmation with the studio.'},
    sourcing: {status: 'merchant_provided', independentlyVerified: false, needsConfirmation: true,
      summary: 'The Brites studio says its materials are ethically sourced from the United States. The exact materials and finishes for a piece come from that piece’s published options and description.'},
    shipping: {optionsAvailable: true, ratesAtCheckout: true,
      summary: 'The studio offers a variety of shipping options. Available services, prices and transit estimates depend on the destination and are confirmed at checkout.'},
    gifts: {packages: true, notes: true, wrapping: true,
      summary: 'The studio offers gift packages, gift notes and gift wrapping. Confirm the package, availability and any charge for the selected order.'},
    customization: {designs: true, engraving: true, customerDesigns: true, requiresStudioReview: true,
      summary: 'The studio can help with customization, engraving and new pieces based on a customer’s design. Compatibility, design requirements, cost and timing need studio review for the selected piece.'}
  };
}

function visibleBlocks(html) {
  if (typeof html !== 'string' || Buffer.byteLength(html) > MAX_HTML_BYTES) return [];
  const entities = {amp: '&', nbsp: ' ', quot: '"', apos: "'", ndash: '–', mdash: '—', rsquo: "'", lsquo: "'"};
  return html.replace(/<!--[^]*?-->/g, ' ')
    .replace(/<(script|style|template|noscript)\b[^>]*>[^]*?<\/\1\s*>/gi, ' ')
    .replace(/<br\b[^>]*>|<\/(?:p|li|div|h[1-6]|ul|ol|section|header|footer)>/gi, '\n')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&(#x[0-9a-f]+|#\d+|[a-z]+);/gi, (whole, code) => {
      if (code[0] !== '#') return entities[code.toLowerCase()] ?? whole;
      const n = code[1].toLowerCase() === 'x' ? parseInt(code.slice(2), 16) : parseInt(code.slice(1), 10);
      return Number.isInteger(n) && n > 31 && n <= 0x10ffff && !(n >= 0xd800 && n <= 0xdfff) ? String.fromCodePoint(n) : ' ';
    }).split(/\n+/).map(value => tidy(value.replace(/\s+/g, ' '), 12000))
    .filter(value => value && !unsafeText.test(value) && !/https?:\/\//i.test(value)).slice(0, 500);
}

function source(url, title, checkedAt) {return {url, title, checkedAt};}
async function boundedHtml(response) {
  const declared = Number(response.headers?.get?.('content-length'));
  if (Number.isFinite(declared) && declared > MAX_HTML_BYTES) throw Error('The public offers response is too large.');
  if (response.body?.getReader) {
    const reader = response.body.getReader(), chunks = [];let size = 0;
    try {
      for (;;) {
        const chunk = await reader.read();if (chunk.done) break;
        size += chunk.value.byteLength;if (size > MAX_HTML_BYTES) {await reader.cancel();throw Error('The public offers response is too large.');}
        chunks.push(Buffer.from(chunk.value));
      }
    } finally {reader.releaseLock();}
    return Buffer.concat(chunks).toString('utf8');
  }
  const html = await response.text();if (Buffer.byteLength(html) > MAX_HTML_BYTES) throw Error('The public offers response is too large.');return html;
}
function offerBase(kind, boundSource) {
  return {kind, source: boundSource, expiresAt: null, stacking: null, minimumSpend: null,
    currency: null, eligibility: 'confirm_at_checkout', checkoutValidated: false};
}
function parsePublishedOffers(html, checkedAt) {
  const blocks = visibleBlocks(html), items = [], found = new Set(), homeSource = source(HOME, 'Brites storefront', checkedAt);
  const addCode = (code, percent, claim, firstOrderRequired = null, newsletterSignupRequired = null) => {
    const value = tidy(code, 32).toUpperCase(), amount = Number(percent);
    if (!/^[A-Z0-9][A-Z0-9_-]{2,31}$/.test(value) || !Number.isInteger(amount) || amount < 1 || amount > 100 ||
      /no longer|expired|not valid|unavailable|suspended|example|sample code/i.test(claim) || found.has('code:' + value)) return;
    found.add('code:' + value);
    items.push({...offerBase('code', homeSource), code: value, percent: amount, firstOrderRequired, newsletterSignupRequired,
      summary: newsletterSignupRequired ? 'The storefront publishes a newsletter signup offer of ' + amount + '% off a first order, with code ' + value + ' shown after signup. Checkout confirms the applicable terms.' :
        firstOrderRequired ? 'The storefront publishes ' + amount + '% off a first order with code ' + value + '. Checkout confirms the applicable terms.' :
        'The storefront publishes code ' + value + ' for ' + amount + '% off. Checkout confirms the applicable terms.'});
  };
  // Only associate a code with an explicit percentage in a short, observed
  // offer block. Adjacent navigation or another offer cannot supply the value.
  for (let blockIndex = 0; blockIndex < blocks.length; blockIndex++) {
    const block = blocks[blockIndex], guard = blocks.slice(blockIndex, blockIndex + 3).join(' ').slice(0, 14000);
    for (const hit of block.matchAll(/\bcode\s*[:–—-]?\s*([a-z0-9][a-z0-9_-]{2,31})\s+(?:for|to get|gives? you|and (?:get|save))\s+(\d{1,3})\s*%\s*off\b/gi)) addCode(hit[1], hit[2], guard);
    for (const hit of block.matchAll(/\b(\d{1,3})\s*%\s*off\b[^.!?]{0,100}?\b(?:with|using|use)\s+(?:the\s+)?code\s*[:–—-]?\s*([a-z0-9][a-z0-9_-]{2,31})\b/gi)) addCode(hit[2], hit[1], guard, /\bfirst\s+(?:order|purchase)\b/i.test(hit[0]) ? true : null);
    const free = /\bfree\s+(?:standard\s+)?shipping\s+(?:on\s+orders\s+)?(over|above|at least)\s*([$€£]\s*\d+(?:\.\d{1,2})?)/i.exec(block);
    if (free && !/no longer|expired|unavailable|suspended/i.test(block) && !found.has('shipping')) {
      found.add('shipping');
      const thresholdDisplay = free[2].replace(/\s+/g, '');
      items.push({...offerBase('shipping', homeSource), comparison: free[1].toLowerCase(), thresholdDisplay,
        summary: 'The storefront publishes free shipping on orders ' + free[1].toLowerCase() + ' ' + thresholdDisplay + '. Checkout confirms currency, service and destination eligibility.'});
    }
  }
  // Newsletter forms may put the first-order statement and the disclosed
  // code in separate neighboring blocks. Require both in a bounded group.
  for (let i = 0; i < blocks.length; i++) {
    const first = /\b(\d{1,3})\s*%\s*off\s+(?:your\s+)?first\s+(?:order|purchase)\b/i.exec(blocks[i]);
    if (!first || /no longer|expired|unavailable|suspended/i.test(blocks[i])) continue;
    const group = blocks.slice(i, i + 6).join(' ').slice(0, 900), code = /\byour\s+code\s*[:–—-]\s*([a-z0-9][a-z0-9_-]{2,31})\b/i.exec(group);
    if (code) addCode(code[1], first[1], group, true, /\bbjPopForm\b|data-bj-code|data-bj-step/i.test(html) ? true : /\b(?:newsletter|subscribe|sign\s*up|join (?:our|the) (?:list|newsletter))\b/i.test(group) ? true : null);
  }
  const sourcing = blocks.find(block => /\b(?:United States|U\.?S\.?)\b/i.test(block) && /\bItaly\b/i.test(block) && /\b(?:material|sourc|metal)/i.test(block) && !/\b(?:not|never|no longer|previously|formerly)\b/i.test(block));
  return {items: items.slice(0, 8), source: homeSource, parsed: blocks.length > 0,
    sourcing: sourcing ? {source: homeSource, summary: 'The published storefront describes materials sourced from the United States and Italy.', needsConfirmation: true} : null};
}

function createStorefrontServices({fetch = globalThis.fetch, now = Date.now, ttlMs = 60000, timeoutMs = 10000} = {}) {
  ttlMs = Math.min(60000, Math.max(0, ttlMs));
  let cached = null, pending = null;
  const shippingGuide = policy.createPolicyGuide({fetch, now, ttlMs, timeoutMs});
  async function homeOffers() {
    const response = await fetch(HOME, {method: 'GET', headers: {Accept: 'text/html', 'Cache-Control': 'no-cache'},
      redirect: 'error', credentials: 'omit', signal: AbortSignal.timeout(timeoutMs)});
    if (!response.ok || response.status !== 200 || (response.url && response.url.replace(/\/$/, '') !== HOME.replace(/\/$/, '')) ||
      !/^text\/html\b/i.test(response.headers?.get?.('content-type') || '')) throw Error('The public shop offers could not be checked.');
    const html = await boundedHtml(response);
    const checkedAt = now(), parsed = parsePublishedOffers(html, checkedAt);
    if (!parsed.parsed) throw Error('The published offers could not be read.');
    return {...parsed, checkedAt};
  }
  async function read() {
    if (cached && now() - cached.checkedAt >= 0 && now() - cached.checkedAt < ttlMs) return cached;
    if (pending) return pending;
    pending = (async () => {
      const [home, shipping] = await Promise.allSettled([homeOffers(), shippingGuide.read('shipping')]);
      const at = now(), items = home.status === 'fulfilled' ? [...home.value.items] : [], sources = home.status === 'fulfilled' ? [home.value.source] : [];
      let publishedShipping = null, publishedProduction = null;
      const guidance = merchantGuidance(), conflicts = [];
      if (shipping.status === 'fulfilled') {
        const record = shipping.value, boundSource = source(record.url, record.title, record.checkedAt);
        sources.push(boundSource);
        publishedShipping = {source: boundSource, ratesAtCheckout: record.facts.checkoutRates === true};
        if (record.facts.production || record.facts.rush) {
          publishedProduction = {source: boundSource, standard: record.facts.production, expedited: record.facts.rush, needsConfirmation: true};
          if (record.facts.production && (record.facts.production.min !== 2 || record.facts.production.max !== 3 || record.facts.production.unit !== 'unspecified')) {
            const range = record.facts.production;
            conflicts.push({topic: 'production', merchantSummary: 'The studio’s current guidance is 2–3 days for production.',
              publishedSummary: 'The published shipping policy lists standard production of ' + range.min + '–' + range.max + ' ' + (range.unit === 'unspecified' ? '' : range.unit + ' ') + 'days. Confirm the current timing and any expedited option with the studio before ordering.',
              source: boundSource, needsConfirmation: true});
          }
        }
        if (record.facts.freeShipping && !items.some(item => item.kind === 'shipping')) {
          const offer = record.facts.freeShipping;
          items.push({...offerBase('shipping', boundSource), comparison: offer.comparison, thresholdDisplay: offer.display,
            summary: 'The shipping policy lists free standard shipping for orders ' + offer.comparison + ' ' + offer.display + '. Checkout confirms currency, destination and applicable terms.'});
        }
      }
      const status = items.length ? 'published_not_checkout_validated' : home.status === 'fulfilled' && shipping.status === 'fulfilled' ? 'none_observed' : 'unavailable';
      const publishedSourcing = home.status === 'fulfilled' ? home.value.sourcing : null;
      if (publishedSourcing) conflicts.push({topic: 'sourcing', merchantSummary: 'The studio’s current guidance says its materials are ethically sourced from the United States.',
        publishedSummary: publishedSourcing.summary + ' Confirm the current source for the selected piece with the studio.', source: publishedSourcing.source, needsConfirmation: true});
      const result = {schema: 1, checkedAt: at, guidance, publishedShipping, publishedProduction, publishedSourcing, conflicts,
        offers: {status, partial: home.status !== 'fulfilled' || shipping.status !== 'fulfilled', checkedAt: sources.length ? Math.min(...sources.map(item => item.checkedAt)) : null, items, sources},
        policyLinks: Object.values(policy.POLICIES).map(item => ({label: item.title, title: item.title, url: item.url}))};
      cached = result;return result;
    })();
    try {return await pending;} finally {pending = null;}
  }
  return {read};
}

const publicServices = createStorefrontServices();

const PRIVATE_SERVICE_REQUEST = /\b(?:api keys?|credentials?|passwords?|system prompt|private (?:records|data)|owner data|repository|source code|sales history|customer (?:records|data))\b/i;
const SERVICE_PHRASES = '(?:gift\\s+(?:wrapp?ing|wrap|notes?|messages?|packages?|packaging|boxes?)|(?:custom(?:ization|isation|izing|ising)?|personalization|personalisation)\\s*(?:options?|help|services?)?|engraving(?:\\s+(?:options?|help|services?))?|(?:published|current|available)\\s+(?:offers?|discounts?|codes?)|discount\\s+codes?|coupons?|promo(?:tional)?\\s+codes?|(?:material\\s+)?sourcing(?:\\s+(?:help|guidance|information))?)';

function classifyStorefrontServices(message, history = []) {
  const text = tidy(message, 2000).replace(/[‘’]/g, "'");
  const base = policy.classify(text, history);
  if (PRIVATE_SERVICE_REQUEST.test(text)) return {...base, serviceTopics: []};
  const scoped = text.replace(/\b(?:skip|forget|don't discuss|do not discuss|not asking about|no need (?:to discuss|for))\s+(?:the\s+)?(?:gift\s+(?:wrapping|wrap|notes?|packages?|packaging)|sourcing|customization|customisation|engraving|discounts?|offers?|coupons?)/gi, '');
  const serviceTopics = [];
  if (base.topics.includes('shipping')) serviceTopics.push('shipping');
  if (/\b(?:ethic(?:al|ally)?(?:ly)?\s+sourc\w*|sourc(?:ed|ing)|material\s+origins?|where\s+(?:your|the|these)\s+materials?\s+(?:come|are)\s+from)\b/i.test(scoped)) serviceTopics.push('sourcing');
  if (/\bgift\s+(?:wrapping|wrap|notes?|messages?|packages?|packaging|boxes?)\b|\b(?:include|add|write)\s+(?:a\s+)?(?:personal\s+)?(?:gift\s+)?(?:note|message)\b/i.test(scoped)) serviceTopics.push('gifts');
  if (/\b(?:customization|customisation|customiz\w*|customis\w*|engrave|engraving|personaliz\w*|personalis\w*|custom\s+(?:options?|designs?|pieces?)|(?:my|own|customer'?s?)\s+design|(?:brand\s+new|new)\s+piece\s+(?:from|based on))\b/i.test(scoped)) serviceTopics.push('customization');
  if (/\b(?:discounts?|coupons?|promo(?:tional)?\s+codes?|promotion(?:al)?\s+(?:codes?|offers?)|(?:published|current|available)\s+offers?|(?:what|any|which)\s+offers?)\b/i.test(scoped)) serviceTopics.push('offers');
  const topics = [...new Set([...base.topics, ...serviceTopics])];
  // Asking to see service help is not a request for catalogue discovery. A
  // separate piece, preference or navigation request still takes the mixed path.
  const selection = text.replace(new RegExp('\\b(?:show|find|view|open|recommend|suggest|choose|browse|looking for|want|need|buy)\\s+(?:me\\s+)?(?:(?:a|an|the|your|my)\\s+)?' + SERVICE_PHRASES, 'gi'), '');
  const adjusted = policy.classify(selection, history);
  const discovery = /\b(?:find|show|recommend|suggest|choose|browse|looking for|want|buy|shopping for|need\s+(?:a|an|some))\b/i.test(selection) && /\b(?:necklaces?|earrings?|bracelets?|rings?|charms?|pendants?|jewelry|jewellery|pieces?|gifts?)\b/i.test(selection);
  const action = /\b(?:open|take me to|go to|view|add|put)\b[^.!?]{0,100}\b(?:first|second|third|fourth|fifth|sixth|piece|one|bag|cart|page)\b/i.test(selection);
  const checkoutAction = /\b(?:open|help with|complete|submit|place|pay|buy|purchase|use|charge|retrieve)\b[^.!?]{0,80}\b(?:checkout|check out|order|payment|saved (?:credit )?cards?|card details)\b/i.test(selection);
  const meaning = /\b(?:meanings?|symbolism|symbolic|stories|story|history)\b/i.test(selection) && !/\b(?:production|shipping|sourcing)\s+history\b/i.test(selection);
  const itemPreference = /\b(?:under|below|up to|at most|within|max(?:imum)?|budget|instead|rather than)\b/i.test(selection) && /\b(?:silver|gold|each|per (?:piece|item|necklace|pair))\b/i.test(selection);
  const mixed = discovery || action || checkoutAction || meaning || itemPreference || adjusted.topics.length > 0 && !adjusted.policyOnly;
  return {...base, topics, serviceTopics: [...new Set(serviceTopics)], policyOnly: topics.length > 0 && !mixed};
}

function checkedServiceSource(value, now) {
  const known = new Map([[HOME, 'Brites storefront'], ...Object.values(policy.POLICIES).map(item => [item.url, item.title])]);
  if (!value || !known.has(value.url) || !Number.isFinite(value.checkedAt) || value.checkedAt > now + 60000 || now - value.checkedAt > 5 * 60000) return null;
  return {url: value.url, title: known.get(value.url), checkedAt: value.checkedAt};
}
function safeServiceText(value, max = 1500) {
  const text = tidy(value, max);
  return text && !unsafeText.test(text) && !/<[^>]*>|https?:\/\//i.test(text) ? text : '';
}
function serviceAnswer({message, history = [], services = null, published = null, now = Date.now(), failed = false} = {}) {
  const classification = classifyStorefrontServices(message, history), wanted = new Set(classification.serviceTopics);
  if (!wanted.size) return published;
  const guidance = merchantGuidance(), parts = [], sources = [], conflicts = [];
  const fresh = services?.schema === 1 && Number.isFinite(services.checkedAt) && services.checkedAt <= now + 60000 && now - services.checkedAt <= 5 * 60000;
  if (wanted.has('shipping')) parts.push(guidance.production.summary + ' ' + guidance.shipping.summary);
  if (wanted.has('sourcing')) parts.push(guidance.sourcing.summary + ' This is merchant-provided guidance, not independent sourcing certification.');
  if (wanted.has('gifts')) parts.push(guidance.gifts.summary);
  if (wanted.has('customization')) parts.push(guidance.customization.summary);
  if (wanted.has('offers')) parts.push('Checkout confirms eligibility, currency, any minimum spend, expiry and whether offers can be combined. A published code is not a promise that it applies to your order.');
  if (published?.reply) parts.push('The checked published policy states: ' + published.reply);
  for (const item of fresh && Array.isArray(services.conflicts) ? services.conflicts.slice(0, 4) : []) {
    if (!(item.topic === 'production' && wanted.has('shipping') || item.topic === 'sourcing' && wanted.has('sourcing'))) continue;
    const bound = checkedServiceSource(item.source, now), merchantSummary = safeServiceText(item.merchantSummary), publishedSummary = safeServiceText(item.publishedSummary);
    if (!bound || !merchantSummary || !publishedSummary) continue;
    conflicts.push({topic: item.topic, merchantSummary, publishedSummary, source: bound, needsConfirmation: true});
    parts.push('Please confirm this difference with the studio: ' + publishedSummary);sources.push(bound);
  }
  const offers = fresh ? services.offers : null;
  const offerSources = (Array.isArray(offers?.sources) ? offers.sources : []).map(item => checkedServiceSource(item, now)).filter(Boolean);
  const safeOffers = [];
  if (wanted.has('offers')) {
    for (const item of Array.isArray(offers?.items) ? offers.items.slice(0, 8) : []) {
      const bound = checkedServiceSource(item?.source, now);
      if (!bound || item.checkoutValidated !== false || item.eligibility !== 'confirm_at_checkout') continue;
      if (item.kind === 'code' && /^[A-Z0-9][A-Z0-9_-]{2,31}$/.test(item.code || '') && Number.isInteger(item.percent) && item.percent > 0 && item.percent <= 100) {
        const requirements = item.newsletterSignupRequired === true ? ' through the published newsletter signup offer for a first order' : item.firstOrderRequired === true ? ' for a first order' : '';
        const summary = 'The storefront publishes code ' + item.code + ' for ' + item.percent + '% off' + requirements + '.';
        safeOffers.push({kind: 'code', code: item.code, percent: item.percent, firstOrderRequired: item.firstOrderRequired === true ? true : null, newsletterSignupRequired: item.newsletterSignupRequired === true ? true : null, source: bound, checkoutValidated: false});parts.push(summary);sources.push(bound);
      } else if (item.kind === 'shipping' && ['over', 'above', 'at least'].includes(item.comparison) && /^[$€£]\d+(?:\.\d{1,2})?$/.test(item.thresholdDisplay || '')) {
        safeOffers.push({kind: 'shipping', comparison: item.comparison, thresholdDisplay: item.thresholdDisplay, source: bound, checkoutValidated: false});parts.push('The published shipping offer is free standard shipping on orders ' + item.comparison + ' ' + item.thresholdDisplay + '.');sources.push(bound);
      }
    }
    if (!safeOffers.length) parts.push(offers?.status === 'none_observed' && offers.partial !== true && offerSources.length ? 'No published offer was observed in the checked public sources.' : 'Current published offers could not be verified just now; ask the shop before using a code.');
    if (offers?.partial === true && safeOffers.length) parts.push('Some offer sources could not be checked.');
  }
  const publishedSources = (Array.isArray(published?.policyKnowledge?.sources) ? published.policyKnowledge.sources : []).map(item => checkedServiceSource(item, now)).filter(Boolean);
  sources.push(...publishedSources);
  const missingPublished = failed || !fresh || wanted.has('shipping') && published?.policyKnowledge?.status !== 'verified' || wanted.has('sourcing') && !offerSources.some(item => item.url === HOME) || wanted.has('offers') && (!offers || offers.status === 'unavailable' || offers.partial === true);
  if (missingPublished && !wanted.has('offers')) parts.push('Current public policy details could not be fully checked. Confirm the selected piece, charges and timing with the studio before ordering.');
  const uniqueSources = [...new Map(sources.map(item => [item.url, item])).values()];
  const links = (Array.isArray(published?.policyLinks) ? published.policyLinks : []).filter(item => Object.values(policy.POLICIES).some(known => item.url === known.url));
  if (wanted.has('shipping')) links.push({label: policy.POLICIES.shipping.title, url: policy.POLICIES.shipping.url});
  if (wanted.has('sourcing') || wanted.has('offers')) links.push({label: 'Brites storefront', url: HOME});
  const onlyMerchant = !wanted.has('shipping') && !wanted.has('offers') && !published;
  const productionOnly = /\b(?:production|turnaround)\b/i.test(message || '') && !/\b(?:shipping|delivery|deliver|arrive|arrival|transit|postage)\b/i.test(message || '') && !classification.country;
  return {reply: tidy(parts.join('\n\n'), 5000), question: productionOnly ? null : published?.question || null, policyOnly: classification.policyOnly,
    policyLinks: [...new Map(links.map(item => [item.url, {label: item.label, url: item.url}])).values()],
    policyKnowledge: {status: missingPublished ? 'partial' : onlyMerchant ? 'merchant_guidance' : 'verified', sources: uniqueSources, checkedAt: uniqueSources.length ? Math.min(...uniqueSources.map(item => item.checkedAt)) : null},
    policyUnavailable: missingPublished,
    serviceKnowledge: {schema: 1, readCompleted: !failed && fresh, checkedAt: fresh ? services.checkedAt : now, status: 'merchant_provided', independentlyVerified: false, source: guidance.source,
      topics: classification.serviceTopics, conflicts, offers: safeOffers, publishedStatus: missingPublished ? 'partial' : 'checked'}};
}
function createStorefrontGuide({readServices = () => publicServices.read(), policyGuide = policy.createPolicyGuide(), now = Date.now} = {}) {
  async function answer({message, history = []} = {}) {
    const classification = classifyStorefrontServices(message, history);
    if (!classification.topics.length) return null;
    if (!classification.serviceTopics.length) return policyGuide.answer({message, history});
    const existing = policy.classify(message, history);
    const checks = await Promise.allSettled([readServices(), existing.topics.length ? policyGuide.answer({message, history}) : Promise.resolve(null)]);
    const failed = checks.find(item => item.status === 'rejected');if (failed) throw failed.reason;
    return serviceAnswer({message, history, services: checks[0].value, published: checks[1].value, now: now()});
  }
  return {classify: classifyStorefrontServices, answer, unavailableAnswer: args => {
    const classification = classifyStorefrontServices(args?.message, args?.history);
    return classification.serviceTopics.length ? serviceAnswer({...args, published: policy.unavailableAnswer(args), failed: true, now: now()}) : policy.unavailableAnswer(args);
  }};
}
module.exports = {HOME, MAX_HTML_BYTES, merchantGuidance, visibleBlocks, parsePublishedOffers, createStorefrontServices, isStudioCreditProduct,
  classifyStorefrontServices, serviceAnswer, createStorefrontGuide, readServices: () => publicServices.read()};
