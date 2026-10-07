'use strict';

// This shopper projection contains merchant guidance and fixed, public shop
// reads only. It never reads an account, a private dossier or a discount API.
const policy = require('./_britesConcierge');
const HOME = 'https://britesjewelry.com/';
const MAX_HTML_BYTES = 1024 * 1024;
const tidy = (value, max = 3000) => String(value ?? '').replace(/\u0000/g, '').trim().slice(0, max);
const unsafeText = /(?:system|developer|assistant|author|internal|hidden)\s+(?:prompt|message|instructions?)|\bignore (?:all |any )?(?:prior|previous|system|developer) instructions?\b|\b(?:assistant|concierge|model)\s+(?:must|should|shall|needs? to)\b/i;

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
module.exports = {HOME, MAX_HTML_BYTES, merchantGuidance, visibleBlocks, parsePublishedOffers, createStorefrontServices,
  readServices: () => publicServices.read()};
