'use strict';
// Product ads (Performance Max) opportunities from the WHOLE eligible Merchant Center catalogue, not only from products that sold.
// Pure functions: no network, no model, no Google or Etsy request. The engine hands in the feed offers it already read, the sales it
// already loaded, the occasion calendar and (when read) Keyword Planner volumes, and gets back ranked opportunity units.
//
// Every opportunity, proven or untested, must be grounded in measured facts (see grounded()) to be shown at all, and carries one `opportunity` object (version 1):
//   { version, kind: "proven"|"new", label: "Proven seller"|"New idea", rankScore 0-100, confidence {score,label},
//     factors: [{ key, label, points, max, available, detail }], reasons: [plain sentences with numbers/dates],
//     cautions: [..], season: { label, date, daysAway, role } | null, unit: { theme, themeLabel, type, offers },
//     testPlan: null | { dailyBudget, days, success[], stop[], budgetNote } }
// rankScore = 100 x points earned / points possible over the factors that could be measured, so a missing source (say, search
// volumes) neither helps nor hurts an idea, and the reasons name what was not measured.

const VERSION = 1;
const TARGET = 5;          // ideas shown: the strongest five ...
const LIMIT = 6;           // ... or six when the sixth is nearly as strong as the fifth. Weak candidates are dropped, never used to fill the list.
const NEW_FLOOR = 2;       // when untested ideas pass the evidence bar, at least this many of the TARGET slots go to them
const MIN_SCORE = 30;      // rank score (0-100) below which nothing is shown, whatever its kind
const MIN_UNIT = 2;        // an untested idea needs at least this many listings to be a theme
const MAX_UNIT = 30;       // listings per idea (the campaign scope limit)

const FACTORS = [
  { key: 'proven', label: 'Proven demand', max: 25 },
  { key: 'season', label: 'Season fit', max: 20 },
  { key: 'demand', label: 'Search demand', max: 15 },
  { key: 'margin', label: 'Price and margin', max: 15 },
  { key: 'quality', label: 'Listing quality', max: 10 },
  { key: 'fresh', label: 'Newness', max: 10 },
  { key: 'gap', label: 'Catalogue gap', max: 10 },
  { key: 'perf', label: 'Known performance', max: 10 }
];
const POSSIBLE = FACTORS.reduce((n, f) => n + f.max, 0);

const num = (v, d = 0) => (Number.isFinite(Number(v)) ? Number(v) : d);
const clamp = (v, a, b) => Math.max(a, Math.min(b, v));
const r1 = v => Math.round(v * 10) / 10;
const r2 = v => Math.round(v * 100) / 100;
const commas = n => Math.round(num(n)).toLocaleString('en-US');
const norm = s => String(s == null ? '' : s).toLowerCase();
const plural = (n, one, other) => commas(n) + ' ' + (Math.round(n) === 1 ? one : (other || one + 's'));
const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
const fmtDate = (ymd, today) => {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(ymd || '')); if (!m) return '';
  return MONTHS[Number(m[2]) - 1] + ' ' + Number(m[3]) + (today && String(today).slice(0, 4) !== m[1] ? ', ' + m[1] : '');
};
const median = a => { const s = a.filter(x => Number.isFinite(x)).sort((x, y) => x - y); if (!s.length) return null; const i = s.length >> 1; return s.length % 2 ? s[i] : (s[i - 1] + s[i]) / 2; };
const ofN = (k, n, one, many) => (n === 1 ? 'Its only listing ' + (k ? one : 'has not') : k + ' of its ' + n + ' listings ' + (k === 1 ? one : many));
const cap1 = s => (s ? s.charAt(0).toUpperCase() + s.slice(1) : s);
const money = (v, cur) => (cur ? cur + ' ' : '') + (Math.abs(v) >= 100 ? commas(v) : String(r2(v)).replace(/(\.\d)$/, '$10'));

// ---------------------------------------------------------------------------------------------------------------
// themes: product type + a motif or occasion, read from the listing title and the feed's own product type
// ---------------------------------------------------------------------------------------------------------------
const TYPES = [
  ['charm', /\bcharms?\b/g], ['necklace', /\b(?:necklaces?|chokers?)\b/g], ['pendant', /\bpendants?\b/g], ['bracelet', /\b(?:bracelets?|bangles?|cuffs?)\b/g],
  ['earring', /\b(?:earrings?|hoops?|studs?|huggies|huggie|danglers?)\b/g], ['ring', /\brings?\b/g], ['anklet', /\banklets?\b/g], ['locket', /\blockets?\b/g], ['keychain', /\b(?:keychains?|key rings?)\b/g]
];
const TYPE_LABEL = { charm: 'charms', necklace: 'necklaces', pendant: 'pendants', bracelet: 'bracelets', earring: 'earrings', ring: 'rings', anklet: 'anklets', locket: 'lockets', keychain: 'keychains', other: 'jewelry' };
function typeIn(text) {
  let best = null, at = -1;
  TYPES.forEach(([key, re]) => { re.lastIndex = 0; let m; while ((m = re.exec(text))) { if (m.index > at) { at = m.index; best = key; } } });
  return best;
}
function typeOf(offer) { return typeIn(norm(offer && offer.title)) || typeIn(norm([offer && offer.type2, offer && offer.type1].join(' '))) || 'other'; }

// [key, label, pattern, occasions the theme is bought for (names from the engine's occasion calendar)]
const THEMES = [
  ['birth-flower', 'birth flower', /\bbirth\s*flowers?\b/, []],
  ['birthstone', 'birthstone', /\bbirth\s*stones?\b|\bbirthstones?\b/, []],
  ['graduation', 'graduation', /\bgraduat\w*|\bgrad\b|\bclass of\b/, ['Graduation']],
  ['wedding', 'wedding and bridal', /\b(?:bride|bridal|bridesmaids?|wedding|maid of honou?r|flower girl)\b/, []],
  ['family', 'mother and family', /\b(?:mom|mum|mama|mother|mommy|grandma|grandmother|nana|gigi|daughter|sister|aunt|auntie|godmother|family|baby)\b/, ["Mother's Day"]],
  ['friends', 'best friends', /\b(?:best friends?|bff|friendship|bestie|besties)\b/, ["Galentine's Day"]],
  ['love', 'love and hearts', /\b(?:hearts?|love|infinity|valentine|couples?|soulmate|sweetheart)\b/, ["Valentine's Day", "Galentine's Day"]],
  ['halloween', 'Halloween', /\b(?:halloween|pumpkin|ghost|witch|spooky|bat|skull|spider|cauldron)\b/, ['Halloween']],
  ['thanksgiving', 'autumn and Thanksgiving', /\b(?:thanksgiving|turkey|harvest|autumn|acorn)\b/, ['Thanksgiving', 'Canadian Thanksgiving']],
  ['christmas', 'Christmas and winter', /\b(?:christmas|xmas|santa|reindeer|snowflake|snowman|holiday|winter|ornament|candy cane|nutcracker|mistletoe)\b/, ['Christmas']],
  ['faith', 'faith and spirit', /\b(?:cross|angel|faith|blessed|prayer|praying|hamsa|evil eye|lotus|saint|guardian|rosary)\b/, []],
  ['remembrance', 'remembrance keepsakes', /\b(?:memorial|remembrance|in loving memory|keepsake|ashes|urn)\b/, []],
  ['pets', 'pets', /\b(?:dogs?|cats?|paws?|corgi|puppy|kitten|pets?|labrador|poodle|dachshund|horse|bunny|rabbit)\b/, []],
  ['wildlife', 'birds and wildlife', /\b(?:birds?|owl|eagle|butterfly|butterflies|bees?|turtle|whale|dolphin|fox|bear|elephant|deer|frog|hummingbird|dragonfly|ladybug|penguin|flamingo|sloth|wolf|lion)\b/, []],
  ['celestial', 'moon and stars', /\b(?:moon|stars?|celestial|zodiac|constellation|galaxy|sun|planet|cosmic|astro|horoscope|aries|taurus|gemini|cancer|leo|virgo|libra|scorpio|sagittarius|capricorn|aquarius|pisces)\b/, []],
  ['floral', 'flowers and botanicals', /\b(?:flowers?|floral|rose|roses|daisy|sunflower|lily|leaf|leaves|clover|tree|botanical|lavender|tulip|peony|fern|blossom)\b/, []],
  ['ocean', 'ocean and travel', /\b(?:ocean|sea|waves?|beach|shell|anchor|nautical|compass|globe|travel|mountain|adventure|palm|starfish|seahorse|mermaid)\b/, []],
  ['initial', 'initials and names', /\b(?:initials?|letters?|monogram|name|custom|personali[sz]ed|engraved)\b/, []]
];
const MATERIALS = [['14k solid gold', /\b14\s*k\b|\bsolid gold\b/], ['rose gold filled', /\brose gold\b/], ['gold filled', /\bgold[\s-]*filled\b/], ['sterling silver', /\bsterling\b|\b925\b|\bsilver\b/]];
function themeOf(offer) {
  const t = norm(offer && offer.title).replace(/['’]/g, '');
  for (const [key, label, re, occasions] of THEMES) if (re.test(t)) return { key, label, occasions };
  const m = MATERIALS.find(x => x[1].test(t));
  return m ? { key: 'material:' + m[0], label: m[0], occasions: [] } : { key: 'general', label: 'everyday', occasions: [] };
}
function unitLabel(theme, type) {
  const t = TYPE_LABEL[type] || 'jewelry';
  return theme.key === 'general' ? cap1(t === 'jewelry' ? 'everyday jewelry' : 'everyday ' + t) : cap1(theme.label + ' ' + t);
}
const idsOf = offer => { const m = String((offer && offer.itemId) || '').match(/^shopify_[A-Za-z]{2}_(\d+)_(\d+)$/i); return m ? { product: Number(m[1]), variant: Number(m[2]) } : null; };
const offerKey = o => String(o.feedLabel || '').toUpperCase() + '|' + String(o.itemId || '').toLowerCase();
const marketOf = (o, defaults) => { let f = String((o && o.feedLabel) || '').trim().toUpperCase(); if (f === 'UK') f = 'GB'; return /^[A-Z]{2}$/.test(f) ? f : ((defaults && defaults[0]) || 'CA'); };

// ---------------------------------------------------------------------------------------------------------------
// one listing's quality: image, full title, approval, no feed warnings, price
// ---------------------------------------------------------------------------------------------------------------
function offerQuality(o, titleProblem) {
  let pts = 0;
  const title = String((o && o.title) || '').trim();
  if (o && o.imageUrl) pts += 3;
  if (title.length >= 25 && title.length <= 150 && !(titleProblem && titleProblem(title))) pts += 3;
  const st = norm(o && o.status);
  pts += st === 'eligible' ? 2 : st === 'eligible_limited' ? 1 : 1.5;
  if (!(o && Array.isArray(o.issues) && o.issues.filter(i => !/^no campaigns advertising this product/i.test(String(i))).length)) pts += 1;
  if (o && Number(o.price) > 0) pts += 1;
  if (o && (o.gtin || o.hasGtin)) pts += 0; // the Google Ads product view does not return GTINs, so they are not scored
  return pts / 10;
}

// ---------------------------------------------------------------------------------------------------------------
// grouping the catalogue into themes, and mapping a theme to a collection page
// ---------------------------------------------------------------------------------------------------------------
function prepare(input) {
  const x = input || {}, h = x.helpers || {}, offers = Array.isArray(x.offers) ? x.offers : [], today = x.today || null;
  const exclude = x.excludeIds instanceof Set ? x.excludeIds : new Set(), covered = x.coveredIds instanceof Set ? x.coveredIds : new Set();
  const stats = { read: offers.length, eligible: 0, unsafe: 0, offMarket: 0, sold: 0, covered: 0, pool: 0, units: 0, thin: 0, noCollection: 0, offersInThin: 0 };
  const pool = [];
  offers.forEach(o => {
    if (!o || !o.itemId || !o.title) return;
    if (h.isEligible && !h.isEligible(o)) return;
    stats.eligible++;
    if ((h.brandSafe && !h.brandSafe(o.title)) || (h.titleProblem && h.titleProblem(o.title))) { stats.unsafe++; return; }
    const fl = String(o.feedLabel || '').toUpperCase();
    if (/^[A-Z]{2}$/.test(fl) && Array.isArray(o.targetCountries) && o.targetCountries.length && !o.targetCountries.includes(fl === 'UK' ? 'GB' : fl)) { stats.offMarket++; return; }
    pool.push(o);
  });
  // "Newest" needs only the order of Shopify product numbers, which grow over time: the top slice of the eligible catalogue.
  const pids = pool.map(o => (idsOf(o) || {}).product).filter(Number.isFinite).sort((a, b) => a - b);
  const freshCut = pids.length >= 20 && pids.length >= pool.length * 0.6 ? pids[Math.floor(pids.length * 0.85)] : null;
  const groups = new Map();
  pool.forEach(o => {
    const k = offerKey(o);
    if (exclude.has(k)) { stats.sold++; return; }
    if (covered.has(k)) { stats.covered++; return; }
    stats.pool++;
    const type = typeOf(o), theme = themeOf(o), key = String(o.feedLabel || '') + '|' + type + '|' + theme.key;
    const u = groups.get(key) || { key, feedLabel: o.feedLabel || null, market: marketOf(o, x.defaults), type, theme, label: unitLabel(theme, type), offers: [] };
    u.offers.push(o); groups.set(key, u);
  });
  const units = [];
  groups.forEach(u => {
    if (u.offers.length < MIN_UNIT) { stats.thin++; stats.offersInThin += u.offers.length; return; }
    u.total = u.offers.length;
    u.offers = u.offers.map(o => ({ o, q: offerQuality(o, h.titleProblem), p: (idsOf(o) || {}).product || 0 })).sort((a, b) => b.q - a.q || b.p - a.p).slice(0, MAX_UNIT).map(e => e.o);
    u.collections = collectionOptions(u, x.profiles, x.collections);
    if (!u.collections.length) { stats.noCollection++; u.noCollection = true; }
    units.push(u);
  });
  stats.units = units.length + stats.thin;
  return { units, stats, freshCut };
}

const STOP = new Set(['and', 'the', 'for', 'jewelry', 'jewellery', 'collection', 'collections', 'gifts', 'gift', 'shop', 'all', 'products', 'new']);
const words = s => norm(s).replace(/[^a-z0-9]+/g, ' ').split(' ').filter(w => w.length > 2 && !STOP.has(w)).map(w => w.replace(/(?:ies)$/, 'y').replace(/(?:es|s)$/, ''));
function collectionOptions(unit, profiles, collections) {
  const cols = new Map(); (Array.isArray(collections) ? collections : []).forEach(c => { if (c && c.handle) cols.set(c.handle, { handle: c.handle, title: c.title || c.handle }); });
  const prof = new Map(); (Array.isArray(profiles) ? profiles : []).forEach(p => { if (p && p.handle) { prof.set(p.handle, p); if (!cols.has(p.handle)) cols.set(p.handle, { handle: p.handle, title: p.title || p.handle }); } });
  const themeWords = new Set(words(unit.theme.key === 'general' || unit.theme.key.startsWith('material:') ? '' : unit.theme.label)), typeWord = unit.type === 'other' ? null : words(TYPE_LABEL[unit.type])[0];
  const matWords = unit.theme.key.startsWith('material:') ? new Set(words(unit.theme.label)) : new Set();
  const productIds = new Set(unit.offers.map(o => (idsOf(o) || {}).product).filter(Number.isFinite).map(String));
  const out = [];
  cols.forEach(c => {
    if (/\b(?:all products|catalog|shop all|all jewelry)\b/i.test(String(c.title))) return; // a catch-all page is only a last resort, below
    const p = prof.get(c.handle) || {}, bag = new Set(words([c.title, c.handle].join(' ')));
    const extra = new Set(words([].concat((p.listingTags || []).map(t => t && (t.t || t)), (p.motifs || []).map(t => t && (t.t || t)), (p.types || []).map(t => t && (t.t || t))).join(' ')));
    let score = 0;
    themeWords.forEach(w => { if (bag.has(w)) score += 3; else if (extra.has(w)) score += 1; });
    matWords.forEach(w => { if (bag.has(w)) score += 1; });
    if (typeWord && bag.has(typeWord)) score += 1;
    const votes = (p.topProducts || []).filter(t => t && t.productId && productIds.has((String(t.productId).match(/(\d{5,})\s*$/) || [])[1])).length;
    score += Math.min(6, votes * 2);
    if (score > 0) out.push({ handle: c.handle, title: c.title, score, votes });
  });
  out.sort((a, b) => b.score - a.score || a.title.localeCompare(b.title));
  if (!out.length) {
    const all = [...cols.values()].find(c => /\b(?:all products|catalog|shop all|all jewelry)\b/i.test(String(c.title)));
    if (all) out.push({ handle: all.handle, title: all.title, score: 0, votes: 0, catchAll: true });
  }
  return out.slice(0, 4);
}

// ---------------------------------------------------------------------------------------------------------------
// scoring: the same eight factors for proven and untested ideas
// ---------------------------------------------------------------------------------------------------------------
// input: { kind, offers[], market, theme, typeLabel, sales:{orders,orders30,free30,paidConv}, paid:{available,conversions,cost,value,monetaryComplete,currency}, free30conv, keywords[] | null }
// ctx:   { today, currency, marginFor(text)->{rate}, occ:{ [market]: { [label]: timingEntry } } | null, gifting[], freshCut, paidById, accountRoas, breakEven }
function score(input, ctx) {
  const x = input, c = ctx || {}, kind = x.kind === 'proven' ? 'proven' : 'new', offers = x.offers || [], n = Math.max(1, offers.length), cur = c.currency || '';
  const F = {}; FACTORS.forEach(f => { F[f.key] = { key: f.key, label: f.label, points: 0, max: f.max, available: false, detail: '' }; });
  const set = (key, points, detail) => { F[key].available = true; F[key].points = r1(clamp(points, 0, F[key].max)); F[key].detail = detail; };

  // 1. proven demand (one factor among eight, never a gate)
  const s = x.sales || {};
  if (kind === 'proven') {
    const w = num(s.orders) + 2 * num(s.orders30) + 4 * num(s.free30) + 6 * num(s.paidConv);
    const bits = [plural(num(s.orders), 'order') + ' in 90 days' + (num(s.orders30) ? ', ' + num(s.orders30) + ' in the last 30' : '')];
    if (num(s.free30) > 0) bits.push(plural(num(s.free30), 'free-listing purchase') + ' in 30 days');
    if (num(s.paidConv) > 0) bits.push(plural(num(s.paidConv), 'paid purchase'));
    set('proven', 25 * clamp(Math.log(1 + w) / Math.log(31), 0, 1), cap1(bits.join('; ')) + '.');
  } else set('proven', 0, 'No store sale has matched these listings yet, so this is an untested idea.');

  // 2. season fit, dated
  const theme = x.theme || { occasions: [] }, market = x.market || 'CA', occ = c.occ && c.occ[market] ? c.occ[market] : null;
  let seasonEvent = null;
  if (occ) {
    const hit = (theme.occasions || []).map(l => occ[l]).filter(Boolean).sort((a, b) => a.daysAway - b.daysAway)[0];
    const away = e => (e.daysAway === 0 ? 'today' : plural(e.daysAway, 'day') + ' away');
    if (hit && hit.role === 'main') {
      set('season', hit.daysAway <= 200 ? 20 : 14, hit.label + ' on ' + fmtDate(hit.date, c.today) + ' (' + away(hit) + ') suits these ' + theme.label + ' pieces' + (hit.startBy ? '; the ideal start is ' + fmtDate(hit.startBy, c.today) : '') + '.');
      seasonEvent = hit;
    } else if (hit && hit.role === 'also') {
      set('season', 12, hit.label + ' on ' + fmtDate(hit.date, c.today) + ' (' + away(hit) + ') suits these ' + theme.label + ' pieces; a run for it would start ' + (hit.startBy ? fmtDate(hit.startBy, c.today) : 'later') + '.');
      seasonEvent = hit;
    } else if (hit) {
      set('season', 2, hit.label + ' (' + fmtDate(hit.date, c.today) + ') is too close for Google to finish learning and still sell, so this is an ongoing test.');
    } else {
      const g = (c.gifting || []).map(l => occ[l]).filter(e => e && e.role === 'main').sort((a, b) => a.daysAway - b.daysAway)[0];
      if (g) set('season', g.daysAway <= 120 ? 9 : 5, 'A giftable piece, and ' + g.label + ' on ' + fmtDate(g.date, c.today) + ' (' + away(g) + ') still leaves time to learn and sell.');
      else set('season', 4, 'No dated gift occasion leaves time to learn and sell, so this is judged as an ongoing test.');
    }
  }

  // 3. search demand (Keyword Planner volumes the scan reads once)
  const kws = Array.isArray(x.keywords) ? x.keywords.filter(k => k && Number.isFinite(Number(k.monthlySearches))) : [];
  if (kws.length) {
    const top = kws.slice().sort((a, b) => b.monthlySearches - a.monthlySearches).slice(0, 3), sum = top.reduce((t, k) => t + Number(k.monthlySearches), 0);
    set('demand', 15 * clamp(Math.log10(1 + sum) / 4, 0, 1), '"' + top[0].text + '" gets about ' + commas(top[0].monthlySearches) + ' searches a month' + (top.length > 1 ? ', ' + commas(sum) + ' across its top ' + top.length + ' phrases' : '') + '.');
  }

  // 4. price and margin against break-even ROAS
  const prices = offers.map(o => Number(o.price)).filter(v => v > 0), price = median(prices);
  if (price != null) {
    const rates = offers.map(o => (c.marginFor ? num((c.marginFor([o.title, o.type1, o.type2].join(' ')) || {}).rate, .65) : .65)), margin = rates.reduce((t, v) => t + v, 0) / rates.length, be = 1 / margin;
    const ease = clamp((2.2 - be) / (2.2 - 1.4), 0, 1), lift = clamp(Math.log(price / 15) / Math.log(150 / 15), 0, 1);
    const acct = c.accountRoas != null && c.accountRoas > 0 ? ' The account currently returns ' + r2(c.accountRoas) + ' on ad spend, so start small.' : '';
    set('margin', 15 * (0.4 * ease + 0.6 * lift), 'Median price ' + money(price, x.priceCurrency || cur) + ' at an estimated ' + Math.round(margin * 100) + '% margin means ads must return ' + r2(be) + ' to break even.' + (c.accountRoas != null && c.accountRoas < be ? acct : ''));
    x._econ = { price, margin, breakEven: r2(be) };
  }

  // 5. listing quality
  const qs = offers.map(o => offerQuality(o, c.titleProblem)), q = qs.reduce((t, v) => t + v, 0) / n, withImage = offers.filter(o => o.imageUrl).length;
  set('quality', 10 * q, ofN(withImage, offers.length, 'has', 'have') + ' a photo, and its listings average ' + Math.round(q * 100) + '% on title, approval, feed warnings and price.');

  // 6. newness (Shopify product numbers grow with time)
  if (c.freshCut != null) {
    const fresh = offers.filter(o => { const p = (idsOf(o) || {}).product; return Number.isFinite(p) && p >= c.freshCut; }).length;
    set('fresh', 10 * clamp((fresh / n) / 0.5, 0, 1), fresh ? ofN(fresh, offers.length, 'is', 'are') + ' among the newest 15% of the catalogue.' : (offers.length === 1 ? 'Its only listing is not' : 'None of its listings is') + ' among the newest 15% of the catalogue.');
  }

  // 7. catalogue gap: no paid product-ad history in the last 90 days
  if (c.paidById) {
    const ran = offers.filter(o => { const r = c.paidById[String(o.itemId).toLowerCase()]; return r && (num(r.impressions) > 0 || num(r.clicks) > 0); }).length, free = 1 - ran / n;
    set('gap', 10 * free, ran ? ofN(ran, offers.length, 'has', 'have') + ' shown in product ads in the last 90 days.' : (offers.length === 1 ? 'Its only listing has' : 'None of its ' + offers.length + ' listings has') + ' shown in a product campaign in the last 90 days.');
  }

  // 8. performance already known
  const p = x.paid || {}, f30 = num(x.free30conv);
  if (p.available && (num(p.conversions) > 0 || num(p.cost) > 0 || f30 > 0)) {
    const be = x._econ ? x._econ.breakEven : c.breakEven || null;
    let pts = 0, detail;
    if (num(p.conversions) > 0 && p.cost > 0 && p.value != null) { const roas = p.value / p.cost; pts = 10 * clamp((roas / (be || 1.54)) / 1.5, 0, 1); detail = 'Paid ads returned ' + r2(roas) + ' on ad spend' + (be ? ' against a ' + r2(be) + ' break-even' : '') + ' (' + plural(p.conversions, 'purchase') + ').'; }
    else if (num(p.cost) > 0 && !num(p.conversions)) detail = 'Paid ads spent ' + money(p.cost, p.currency || cur) + ' with no purchase yet.';
    else if (num(p.conversions) > 0) { pts = 4; detail = plural(p.conversions, 'paid purchase') + ' so far.'; }
    if (f30 > 0) { pts = Math.min(10, pts + 4); detail = (detail ? detail + ' ' : '') + plural(f30, 'free-listing purchase') + ' in 30 days.'; }
    set('perf', pts, detail || 'Paid history is mixed.');
  }

  const list = FACTORS.map(f => F[f.key]), avail = list.filter(f => f.available), possible = avail.reduce((t, f) => t + f.max, 0), earned = avail.reduce((t, f) => t + f.points, 0);
  const rankScore = possible ? Math.round((earned / possible) * 1000) / 10 : 0;
  const share = possible / POSSIBLE;
  let conf;
  if (kind === 'proven') conf = clamp(Math.round(num(x.confidence, 40)), 15, 99);
  else conf = clamp(Math.round(15 + 25 * share + (F.demand.available && F.demand.points >= 9 ? 8 : 0) + (F.season.available && F.season.points >= 12 ? 5 : 0) + (F.quality.points >= 8 ? 3 : 0)), 15, 56);
  const cautions = [];
  if (F.season.available && F.season.points <= 2) cautions.push(F.season.detail);
  if (x._econ && c.accountRoas != null && c.accountRoas < x._econ.breakEven) cautions.push('The account returns ' + r2(c.accountRoas) + ' on ad spend against a ' + x._econ.breakEven + ' break-even for these pieces, so keep the budget at the minimum.');
  if (F.perf.available && F.perf.points === 0 && /no purchase/.test(F.perf.detail)) cautions.push(F.perf.detail);
  ['demand', 'fresh', 'gap', 'margin'].forEach(k => { if (!F[k].available) cautions.push(F[k].label + ' was not measured this time, so it is left out of the score.'); });
  const ORDER = { proven: 0, season: 1, demand: 2, margin: 3, perf: 4, gap: 5, fresh: 6, quality: 7 };
  const reasons = avail.filter(f => f.points > 0 && f.detail).sort((a, b) => (ORDER[a.key] ?? 9) - (ORDER[b.key] ?? 9)).slice(0, 3).map(f => f.detail);
  const out = { version: VERSION, kind, label: kind === 'proven' ? 'Proven seller' : 'New idea', rankScore, confidence: { score: conf, label: conf >= 70 ? 'high' : conf >= 50 ? 'medium' : 'low' },
    factors: list.map(f => ({ key: f.key, label: f.label, points: f.points, max: f.max, available: f.available, detail: f.detail })), reasons, cautions: cautions.slice(0, 4),
    season: seasonEvent ? { label: seasonEvent.label, date: seasonEvent.date, daysAway: seasonEvent.daysAway, role: seasonEvent.role, market } : null,
    econ: x._econ || null,
    unit: { theme: theme.key || null, themeLabel: theme.label || null, type: x.typeLabel || null, offers: offers.length }, testPlan: null, listings: [] };
  out.grounded = grounded(out);
  return out;
}

// A conservative test for an untested idea: the minimum daily budget, the days the schedule gave, and plain success and stop rules.
function testPlan(opp, o) {
  if (!opp || opp.kind !== 'new') return null;
  const cur = o.currency || '', days = Math.max(1, Math.round(num(o.days, 28))), daily = num(o.dailyBudget, 6), planned = daily * days;
  const econ = opp.econ, beCpa = econ ? econ.price * econ.margin : null, stopAt = beCpa ? Math.min(planned, Math.max(daily * 7, Math.round(beCpa * 3))) : Math.min(planned, daily * 14);
  const judge = o.judgeAfter ? fmtDate(o.judgeAfter, o.today) : null, be = econ ? econ.breakEven : null;
  return {
    dailyBudget: daily, days,
    success: ['At least 2 orders' + (judge ? ' by ' + judge : ' by the end of the run') + ', with ads returning ' + (be ? r2(be) : 'more than they cost') + ' or better on ad spend.'],
    stop: ['Stop early if ' + money(stopAt, cur) + ' is spent with no order.', 'Do not raise the budget until it beats break-even' + (be ? ' (' + r2(be) + ')' : '') + '.'],
    budgetNote: (o.accountRoas != null && be && o.accountRoas < be ? 'The account returns ' + r2(o.accountRoas) + ' on ad spend against a ' + r2(be) + ' break-even, so this stays at the minimum budget of ' + money(daily, cur) + ' a day.' : 'A small test at the minimum budget of ' + money(daily, cur) + ' a day (' + money(planned, cur) + ' over the run).')
  };
}


// An idea is shown only when facts support it. A proven seller needs real sales, free-listing or paid purchases. An untested idea needs measured
// search volume or a dated occasion that suits its theme, a usable listing set, and enough other measured factors. Otherwise it is dropped, with the reason.
function grounded(opp) {
  const f = {}; (opp.factors || []).forEach(x => { f[x.key] = x; });
  const measured = (opp.factors || []).filter(x => x.available).length;
  if (opp.kind === 'proven') {
    const ok = f.proven && f.proven.points > 0 && opp.rankScore >= MIN_SCORE;
    return { ok: !!ok, why: ok ? null : 'its sales evidence is too thin to rank' };
  }
  const demand = !!(f.demand && f.demand.available && f.demand.points >= 7), dated = !!(f.season && f.season.available && f.season.points >= 12);
  if (!demand && !dated) return { ok: false, why: 'no measured search volume and no dated occasion supports it' };
  if (!(f.quality && f.quality.points >= 6)) return { ok: false, why: 'its listings are missing photos, full titles or approvals' };
  if (measured < 4) return { ok: false, why: 'too few facts could be measured' };
  if (opp.rankScore < MIN_SCORE + 10) return { ok: false, why: 'its measured facts add up to a weak score (' + Math.round(opp.rankScore) + ' of 100)' };
  return { ok: true, why: null };
}

// The facts about one chosen listing, from its own feed row and history only. Nothing is written for a fact that is not there.
// sold: { orders, orders30 } for this listing when it has sales; paid/free30: its own product-ad and free-listing rows.
function listingFacts(o, ctx, sold) {
  const c = ctx || {}, cur = o.currency || c.currency || '', out = [];
  if (sold && num(sold.orders) > 0) out.push('This product: ' + plural(num(sold.orders), 'order') + ' in 90 days' + (num(sold.orders30) ? ', ' + num(sold.orders30) + ' in the last 30' : ''));
  if (Number(o.price) > 0) {
    const m = c.marginFor ? num((c.marginFor([o.title, o.type1, o.type2].join(' ')) || {}).rate, 0) : 0;
    out.push('Price ' + money(Number(o.price), cur) + (m > 0 ? ', about ' + Math.round(m * 100) + '% margin (break-even ' + r2(1 / m) + ')' : ''));
  }
  const st = norm(o.status);
  out.push(st === 'eligible' ? 'In stock and approved in the feed' : st === 'eligible_limited' ? 'In stock, approved with limits' : 'In stock; approval is pending its first campaign');
  out.push(o.imageUrl ? 'Has a listing photo' : 'No listing photo in the feed');
  const p = (idsOf(o) || {}).product;
  if (c.freshCut != null && Number.isFinite(p) && p >= c.freshCut) out.push('Among the newest 15% of the catalogue');
  const r = c.paidById && c.paidById[String(o.itemId).toLowerCase()];
  if (c.paidById) out.push(r && (num(r.impressions) > 0 || num(r.clicks) > 0) ? 'Shown in product ads: ' + plural(num(r.clicks), 'click') + ' in 90 days' + (num(r.conversions) > 0 ? ', ' + plural(num(r.conversions), 'purchase') : '') : 'No product-ad impressions in 90 days');
  const f = c.free30ById && c.free30ById[String(o.itemId).toLowerCase()];
  if (f && (num(f.clicks) > 0 || num(f.conversions) > 0)) out.push('Free listings: ' + plural(num(f.clicks), 'click') + (num(f.conversions) > 0 ? ', ' + plural(num(f.conversions), 'purchase') : '') + ' in 30 days');
  return out.slice(0, 6);
}

// The theme and product type most of a set of titles share (for scoring a proven idea with the same factors as a new one).
function dominant(titles) {
  const tc = new Map(), yc = new Map();
  (titles || []).forEach(t => {
    const th = themeOf({ title: t }); if (th.key !== 'general' && th.key !== 'initial') tc.set(th.key, { th, n: (tc.get(th.key) ? tc.get(th.key).n : 0) + 1 });
    const ty = typeOf({ title: t }); yc.set(ty, (yc.get(ty) || 0) + 1);
  });
  const th = [...tc.values()].sort((a, b) => b.n - a.n)[0], ty = [...yc.entries()].sort((a, b) => b[1] - a[1])[0];
  return { theme: th ? th.th : { key: 'general', label: 'everyday', occasions: [] }, type: ty ? ty[0] : 'other' };
}

// ---------------------------------------------------------------------------------------------------------------
// choosing: one theme per collection page and market, the LIMIT best, proven and untested mixed
// ---------------------------------------------------------------------------------------------------------------
function assign(units, o) {
  const x = o || {}, used = new Set(x.usedKeys || []), tagOf = x.tagOf || ((h, f) => h + '|' + (f || '')), picked = [], dropped = [];
  units.forEach(u => {
    if (u.noCollection) { dropped.push({ unit: u, kind: 'no_collection' }); return; }
    let sawTaken = false, sawUsed = false, hit = null;
    for (const c of u.collections) {
      if (x.isTaken && x.isTaken(c.handle, u.feedLabel)) { sawTaken = true; continue; }
      const k = tagOf(c.handle, u.feedLabel);
      if (used.has(k)) { sawUsed = true; continue; }
      hit = c; used.add(k); break;
    }
    if (hit) picked.push({ unit: u, collection: hit }); else dropped.push({ unit: u, kind: sawTaken && !sawUsed ? 'taken' : sawUsed ? 'same_collection' : 'taken' });
  });
  return { picked, dropped };
}

// all: [{ kind, opportunity:{rankScore} }]. Only grounded ones are eligible. The strongest TARGET are shown, mixing proven sellers and untested ideas
// (at least NEW_FLOOR untested when that many pass the bar); a sixth joins only when it is nearly as strong as the fifth.
function select(all) {
  const ok = all.filter(a => a.opportunity && a.opportunity.grounded && a.opportunity.grounded.ok).sort((a, b) => b.opportunity.rankScore - a.opportunity.rankScore);
  const proven = ok.filter(a => a.kind === 'proven'), fresh = ok.filter(a => a.kind !== 'proven');
  const reserve = Math.min(fresh.length, NEW_FLOOR), provenTake = Math.min(proven.length, TARGET - reserve), freshTake = Math.min(fresh.length, TARGET - provenTake);
  let shown = proven.slice(0, provenTake).concat(fresh.slice(0, freshTake)).sort((a, b) => b.opportunity.rankScore - a.opportunity.rankScore);
  const rest = ok.filter(a => !shown.includes(a));
  if (shown.length >= TARGET && rest[0] && rest[0].opportunity.rankScore >= 0.9 * shown[TARGET - 1].opportunity.rankScore && shown.length < LIMIT) shown = shown.concat([rest[0]]).sort((a, b) => b.opportunity.rankScore - a.opportunity.rankScore);
  const keep = new Set(shown);
  return { shown, held: ok.filter(a => !keep.has(a)), weak: all.filter(a => !(a.opportunity && a.opportunity.grounded && a.opportunity.grounded.ok)) };
}

// The one sentence that names what limited the list, for the "why so few" answer.
function limiter(s, o) {
  const x = s || {}, u = o || {};
  if (u.readError) return 'the full Merchant Center catalogue could not be read (' + u.readError + '), so only products that already sold were considered';
  if (!x.read) return 'the catalogue read returned no products';
  if (x.eligible < 12) return 'only ' + plural(x.eligible, 'product') + ' in the feed ' + (x.eligible === 1 ? 'is' : 'are') + ' in stock and approved';
  const parts = [
    ['covered', x.covered + x.sold, plural(x.covered + x.sold, 'eligible product') + ' already sold or run in a campaign or draft'],
    ['unsafe', x.unsafe + x.offMarket, plural(x.unsafe + x.offMarket, 'product') + ' failed the brand-safety or market check'],
    ['thin', x.offersInThin, plural(x.thin, 'theme') + ' had a single listing, too thin to test'],
    ['no_collection', u.noCollectionOffers || 0, plural(x.noCollection, 'theme') + ' had no matching collection page to link to'],
    ['taken', u.takenUnits || 0, plural(u.takenUnits || 0, 'theme') + ' already ' + ((u.takenUnits || 0) === 1 ? 'has' : 'have') + ' a campaign or review draft on its collection page'],
    ['same', u.sameUnits || 0, plural(u.sameUnits || 0, 'theme') + ' shared a collection page with a stronger idea'],
    ['weak', u.weakUnits || 0, plural(u.weakUnits || 0, 'theme') + ' lacked measured facts to support an ad (' + (u.weakWhy || 'no search volume or dated occasion') + ')']
  ].filter(p => p[1] > 0).sort((a, b) => b[1] - a[1]);
  if (!x.pool) return 'every eligible product ' + (x.sold ? 'has already sold or ' : '') + 'is already in a campaign or draft';
  return parts.length ? parts[0][2] : 'only ' + plural(x.units, 'theme') + ' could be built from the products left';
}

module.exports = { VERSION, TARGET, LIMIT, NEW_FLOOR, MIN_SCORE, MIN_UNIT, MAX_UNIT, FACTORS, typeOf, themeOf, unitLabel, dominant, prepare, collectionOptions, score, grounded, listingFacts, testPlan, assign, select, limiter, offerKey, offerQuality, fmtDate };
