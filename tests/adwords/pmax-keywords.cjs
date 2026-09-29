// Performance Max search themes: candidates come only from what the listings and the timing really contain, the
// ranking uses Keyword Planner volumes when there are any and invents none when there are not, and every theme
// handed to Google is valid. Offline: pure functions, no Google request, no AI call.
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path');
const file = path.resolve(__dirname, '../../netlify/functions/_googleAdsPmaxKeywords.js');
const K = require(file);
let n = 0; const check = (v, m) => { assert.ok(v, m); n++; };
const same = (a, b, m) => { assert.deepEqual(a, b, m); n++; };
const freeze = v => { if (v && typeof v === 'object' && !Object.isFrozen(v)) { Object.freeze(v); Object.values(v).forEach(freeze); } return v; };
const clone = v => JSON.parse(JSON.stringify(v));
const words = t => t.split(' ');
const valid = t => t === t.toLowerCase() && /^[a-z0-9 ]+$/.test(t) && t.length <= 80 && words(t).length <= 10 && t === t.trim();

// A group whose listings hold necklaces, a charm bracelet and nothing else. The group profile also lists earrings and
// rings, which none of these listings or offers contain.
const candidate = freeze({
  handle: 'birthstone-charms', collectionTitle: 'Birthstone Charms',
  productTitles: ['Birthstone Charm Bracelet', 'Personalized Initial Necklace, Sterling Silver, Gift for Mom', 'Gold Filled Dainty Heart Charm Necklace', "Mother's Day Corgi Dog Mom Necklace"],
  types: ['Bracelet', 'Necklace', 'Earrings', 'Ring'],
  offerDetails: [
    { itemId: 'shopify_US_1_1', title: 'Birthstone Charm Bracelet - 14k Gold Filled', productTitle: 'Birthstone Charm Bracelet', type1: 'Bracelets', type2: null, customLabels: ['bestseller'] },
    { itemId: 'shopify_US_2_1', title: 'Personalized Initial Necklace', productTitle: 'Personalized Initial Necklace, Sterling Silver, Gift for Mom', type1: 'Necklaces', type2: null, customLabels: [] },
    { itemId: 'shopify_US_3_1', title: 'Gold Filled Dainty Heart Charm Necklace', productTitle: 'Gold Filled Dainty Heart Charm Necklace', type1: 'Necklaces', type2: null, customLabels: [] }],
  demandEvidence: [
    { title: 'Birthstone Charm Bracelet', orders: 12, units: 14, revenue: 600 },
    { title: 'Personalized Initial Necklace, Sterling Silver, Gift for Mom', orders: 5, units: 5, revenue: 300 },
    { title: 'Gold Filled Dainty Heart Charm Necklace', orders: 0, units: 0, revenue: 0 }]
});
const timing = freeze([
  { label: 'Christmas', date: '2026-12-25', daysAway: 87, market: 'US', role: 'main', startBy: '2026-11-01', note: '' },
  { label: "Valentine's Day", date: '2027-02-14', daysAway: 138, market: 'US', role: 'also', startBy: null, note: '' },
  { label: "Mother's Day", date: '2026-05-10', daysAway: -142, market: 'US', role: 'too-late', startBy: null, note: '' },
  { label: "Father's Day", date: '2027-06-20', daysAway: 264, market: 'US', role: 'too-late', startBy: null, note: '' }]);
const today = '2026-09-29';
// Occasion words that must never appear unless the timing lists them as main or also.
const OCCASIONS = /\b(christmas|xmas|holidays?|hanukkah|easter|halloween|thanksgiving|valentines?|galentines?|birthday|anniversary|graduation|wedding|bridal|mothers day|fathers day|black friday|cyber monday|teachers?|nurses?)\b/;
const TYPE_WORDS = /\b(earrings?|hoops?|studs?|rings?|anklets?|lockets?|keychains?|pendants?|bangles?|cuffs?)\b/;

(async () => {
  // ---- no network, no AI, no storage: the file requires nothing ----
  const source = fs.readFileSync(file, 'utf8');
  check(!/require\(/.test(source) && !/\bfetch\b|process\.env|Date\.now|new Date/.test(source), 'the module reads no network, environment or clock');
  same(K.keywordCandidates(null), [], 'nothing in, nothing out'); same(K.keywordCandidates({}), [], 'an empty candidate gives no candidates');
  same(K.keywordCandidates({ collectionTitle: 'Corgi', productTitles: ['Corgi Dog'] }, {}), [], 'a listing with no jewelry type invents none');
  same(K.rankKeywords([], {}), [], 'no candidates, no keywords'); same(K.themesFromKeywords(null), [], 'no keywords, no themes');

  // ---- candidates: only types the candidate contains ----
  const cands = K.keywordCandidates(candidate, { today, timing, tags: [{ t: 'dainty', n: 4 }, 'sterling silver'] });
  check(cands.length > 0 && cands.length <= 40, 'up to 40 candidates: ' + cands.length);
  check(new Set(cands.map(c => c.text)).size === cands.length, 'candidate texts are unique');
  check(cands.every(c => valid(c.text)), 'every candidate is a valid search theme');
  check(cands.every(c => K.KINDS.includes(c.kind) && c.reason && c.reason.length <= 100), 'every candidate has a kind and a reason of at most 100 characters');
  check(cands.every(c => !TYPE_WORDS.test(c.text)), 'no earrings, ring, anklet, locket or other type the listings lack: ' + cands.filter(c => TYPE_WORDS.test(c.text)).map(c => c.text));
  check(cands.some(c => /\bcharm bracelet\b/.test(c.text)) && cands.some(c => /\bnecklace\b/.test(c.text)), 'the types the listings hold are used');
  check(cands.every(c => /\b(necklace|bracelet|charm)\b/.test(c.text)), 'each candidate names a type the group holds');
  check(cands.some(c => c.text === 'birthstone charm bracelet'), 'the listing itself becomes a search');
  check(cands.some(c => c.text === 'personalized initial necklace'), 'the listing title is cleaned of copy after the comma');
  check(cands.some(c => /^gold filled\b/.test(c.text)) && cands.some(c => /sterling silver/.test(c.text)), 'materials come from the listings and tags');
  check(cands.some(c => /\bheart\b/.test(c.text)) && cands.some(c => /\bdainty\b/.test(c.text)), 'motifs and looks come from the titles and tags');
  check(cands.some(c => /\bbirthstone\b/.test(c.text)) && cands.some(c => /\bpersonalized\b/.test(c.text)), 'personalization words the listings use');
  check(!cands.some(c => /\b(engraved|photo|monogram|coordinates|fingerprint)\b/.test(c.text)), 'personalization the listings never mention is not offered');
  check(!cands.some(c => /brites/.test(c.text)), 'the shop name is never a search theme');
  const onlyTypes = K.keywordCandidates({ collectionTitle: 'Studs', productTitles: ['Coral Beach Dream'], types: ['Earrings'], offerDetails: [{ title: 'Coral Beach Dream', productTitle: 'Coral Beach Dream', type1: 'Anklets' }] }, {});
  check(onlyTypes.length > 0 && onlyTypes.every(c => /anklet/.test(c.text) && !/earring/.test(c.text)), 'the offer type outranks the group profile: ' + onlyTypes.map(c => c.text));
  const profileOnly = K.keywordCandidates({ collectionTitle: 'Studs', productTitles: ['Coral Beach Dream'], types: ['Hoop Earrings'], offerDetails: [{ title: 'Coral Beach Dream', productTitle: 'Coral Beach Dream' }] }, {});
  check(profileOnly.length > 0 && profileOnly.every(c => /hoop earrings/.test(c.text)), 'the group profile stands in only when no listing shows a type');
  const charmOnly = K.keywordCandidates({ productTitles: ['Corgi Dog Charm Only'], types: ['Necklace Charm Only'] }, {});
  check(charmOnly.length > 0 && charmOnly.every(c => /charm/.test(c.text) && !/necklace/.test(c.text)), 'a charm-only listing is a charm, not a necklace');

  const cuff = K.keywordCandidates({ productTitles: ['Engraved Cuff', 'Slim Choker'] }, {});
  check(cuff.length > 0 && cuff.some(c => /cuff bracelet/.test(c.text)) && cuff.some(c => /choker necklace/.test(c.text)) && cuff.every(c => !/\b(ring|earrings?|anklet)\b/.test(c.text)), 'a cuff is a cuff bracelet and a choker a choker necklace: ' + cuff.map(c => c.text));

  // ---- candidates: occasions only from the timing ----
  const occText = cands.filter(c => OCCASIONS.test(c.text));
  const seen = new Set(occText.flatMap(c => c.text.match(new RegExp(OCCASIONS.source, 'g'))));
  same([...seen].sort(), ['christmas', 'valentines'], 'only Christmas (main) and Valentine\'s Day (also) appear');
  check(!cands.some(c => /mothers day|fathers day|birthday|graduation|easter|thanksgiving/.test(c.text)), 'too-late and unlisted occasions never appear, even when a title says Mother\'s Day');
  check(cands.some(c => c.text === 'birthstone charm bracelet christmas gift' && c.kind === 'occasion' && c.occasion === 'Christmas'), 'occasion plus product, the way a shopper phrases it');
  check(cands.some(c => c.reason === 'Christmas is 87 days away; matches the "Birthstone Charm Bracelet" listing'), 'an occasion reason names the days and the listing');
  check(cands.some(c => c.text === 'birthstone charm bracelet' && c.reason === 'matches the "Birthstone Charm Bracelet" listing, one of your best sellers'), 'a listing reason names the listing, and a best seller says so');
  check(cands.filter(c => c.occasion).every(c => /^(Christmas|Valentine's Day) is \d+ days away/.test(c.reason)), 'every occasion candidate says which occasion and how far away');
  const stuffed = K.keywordCandidates({ productTitles: ['Black Holiday Friday Wedding Bridal Necklace', 'Baby Birthday Shower Anniversary Necklace'] }, { today, timing: [timing[0]] });
  check(stuffed.length > 0 && stuffed.every(c => !/black friday|wedding|bridal|baby shower|birthday|anniversary/.test(c.text)), 'occasion words a title happens to hold never become searches: ' + stuffed.map(c => c.text));
  const noTiming = K.keywordCandidates(candidate, { today });
  check(noTiming.length > 0 && noTiming.every(c => !OCCASIONS.test(c.text) && !c.occasion), 'without timing no occasion is invented');
  const lateOnly = K.keywordCandidates(candidate, { today, timing: timing.filter(t => t.role === 'too-late') });
  check(lateOnly.every(c => !OCCASIONS.test(c.text)), 'occasions marked too late are not used');
  const derived = K.keywordCandidates(candidate, { today, timing: [{ label: 'Christmas', date: '2026-12-25', role: 'main' }] });
  check(derived.some(c => c.reason.startsWith('Christmas is 87 days away')), 'days away come from the date and today when the timing has no count');
  const tomorrow = K.keywordCandidates(candidate, { today: '2026-12-24', timing: [{ label: 'Christmas', date: '2026-12-25', role: 'main' }] });
  check(tomorrow.some(c => c.reason.startsWith('Christmas is tomorrow')), 'tomorrow is said as tomorrow');
  const split = K.keywordCandidates(candidate, { today, timing: [{ label: 'Memorial / Sympathy', date: '2026-10-20', daysAway: 21, role: 'main' }] });
  check(split.some(c => /^memorial |memorial gift/.test(c.text) || /\bmemorial\b/.test(c.text)) && split.some(c => /\bsympathy\b/.test(c.text)) && split.every(c => valid(c.text)), 'a label with a slash becomes valid searches: ' + split.filter(c => c.occasion).map(c => c.text));
  const canada = K.keywordCandidates(candidate, { today, markets: ['CA'], timing: [{ label: 'Thanksgiving', date: '2026-10-12', daysAway: 13, market: 'CA', role: 'main' }, { label: 'Thanksgiving', date: '2026-11-26', daysAway: 58, market: 'US', role: 'main' }] });
  check(canada.filter(c => c.occasion).every(c => c.reason.startsWith('Thanksgiving is 13 days away')), 'only the targeted markets\' dates count');
  const british = K.keywordCandidates(candidate, { today, markets: ['GB', 'AU'], timing: [{ label: 'Christmas', date: '2026-12-25', daysAway: 87, market: 'GB', role: 'main' }] });
  check(british.some(c => c.text === 'christmas jewellery gift') && !british.some(c => /jewelry/.test(c.text)) && K.keywordCandidates(candidate, { today, markets: ['US', 'CA'], timing: [timing[0]] }).some(c => c.text === 'christmas jewelry gift'), 'British spelling for British markets, American otherwise');

  // ---- candidates: recipients only when the occasion implies them and the listing fits ----
  check(cands.some(c => c.text === 'personalized charm bracelet gift for mom' && c.kind === 'recipient'), 'personalized charm bracelet gift for mom');
  check(cands.some(c => /gift for (grandma|daughter|best friend|her)$/.test(c.text)), 'Christmas implies more than one recipient');
  check(cands.some(c => c.text === 'personalized necklace gift for mom' && c.reason === 'the "Personalized Initial Necklace" listing is written for mom'), 'a recipient the listing names belongs to that listing');
  check(!cands.some(c => c.text === 'personalized charm bracelet gift for mom' && /written for/.test(c.reason)), 'a recipient one listing names is not credited to another');
  check(!cands.some(c => /\b(dad|husband|grandpa|boyfriend)\b/.test(c.text)), 'no father-side recipient without a Father\'s Day in the timing');
  const fathers = [{ label: "Father's Day", date: '2027-06-20', daysAway: 264, role: 'main' }];
  const feminine = K.keywordCandidates(candidate, { today, timing: fathers });
  check(!feminine.some(c => /\b(dad|husband|grandpa)\b/.test(c.text)), 'a dainty, heart and birthstone group does not become a gift for dad');
  const compass = K.keywordCandidates({ collectionTitle: 'Compass', productTitles: ['Engraved Compass Bracelet'], types: ['Bracelet'] }, { today, timing: fathers });
  check(compass.some(c => c.text === 'personalized bracelet gift for dad') && compass.some(c => c.text === 'fathers day bracelet'), 'a plain bracelet can be a gift for dad on Father\'s Day: ' + compass.map(c => c.text));
  const named = K.keywordCandidates({ productTitles: ['Grandma Birthstone Charm Bracelet'] }, {});
  check(named.some(c => c.text === 'personalized charm bracelet gift for grandma' && c.reason === 'the "Grandma Birthstone Charm Bracelet" listing is written for grandma'), 'a listing that names the recipient is enough, with no occasion');

  // ---- candidates: brand safety and validity ----
  const safeOnly = K.keywordCandidates(candidate, { today, timing, tags: ['sterling silver'], brandSafe: t => !/silver|christmas/.test(t) });
  check(safeOnly.length > 0 && safeOnly.every(c => !/silver|christmas/.test(c.text)), 'excluded terms are never candidates');
  check(safeOnly.some(c => /valentines day/.test(c.text)) && safeOnly.some(c => /gold filled/.test(c.text)), 'everything else is kept');
  check(K.keywordCandidates(candidate, { today, timing, tags: ['sterling silver'] }).some(c => /silver/.test(c.text)), 'without a safety check nothing is excluded');
  check(K.keywordCandidates(candidate, { today, timing, brandSafe: () => { throw new Error('boom'); } }).length === 0, 'a safety check that fails excludes rather than lets through');
  const odd = K.keywordCandidates({ productTitles: ['Caf\u00e9 \u00c9toile Star Charm Bracelet \u2728 (Handmade) 7" & 8"', 'Extra Long Personalized Birthstone Engraved Initial Name Photo Monogram Charm Bracelet For Your Best Friend'], types: ['Bracelet'] },
    { today, timing: [{ label: 'Christmas', date: '2026-12-25', daysAway: 87, role: 'main' }] });
  check(odd.length > 0 && odd.every(c => valid(c.text)), 'symbols, accents and over-long titles never produce an invalid theme: ' + odd.filter(c => !valid(c.text)).map(c => c.text));
  check(odd.some(c => c.text === 'cafe etoile star charm bracelet'), 'accents fold to plain letters');
  const before = JSON.stringify(candidate), again = K.keywordCandidates(candidate, { today, timing, tags: [{ t: 'dainty', n: 4 }, 'sterling silver'] });
  same(again, cands, 'the same input gives the same candidates'); check(JSON.stringify(candidate) === before, 'the candidate is not changed');
  const kinds = new Set(cands.map(c => c.kind));
  check(kinds.size === 5, 'candidates cover every kind: ' + [...kinds]);
  same(cands.slice(0, 5).map(c => c.kind), ['product', 'occasion', 'recipient', 'material', 'style'], 'kinds take turns, so the first entries already cover the idea');

  // ---- planner seeds ----
  const seeds = K.plannerSeeds(cands);
  same(seeds, cands.map(c => c.text), 'seeds are the candidate texts in order');
  same(K.plannerSeeds(cands, 5), cands.slice(0, 5).map(c => c.text), 'seeds respect the limit');
  same(K.plannerSeeds([{ text: 'a b c d e f g h i j k' }, { text: 'ok one' }, 'ok one', 'bad!', null], 40), ['ok one'], 'invalid and repeated seeds are not sent');
  check(seeds.length <= 40 && seeds.every(valid), 'at most 40 valid seeds');

  // ---- ranking with Keyword Planner volumes ----
  const C = (text, kind, reason, extra) => ({ text, kind, reason, ...extra });
  const pool = [
    C('birthstone charm bracelet', 'product', 'matches the "Birthstone Charm Bracelet" listing', { top: true }),
    C('birthstone charm bracelet christmas gift', 'occasion', 'Christmas is 87 days away; matches the "Birthstone Charm Bracelet" listing', { occasion: 'Christmas', top: true }),
    C('personalized charm bracelet gift for mom', 'recipient', 'Christmas is 87 days away; a gift for mom fits the "Birthstone Charm Bracelet" listing', { occasion: 'Christmas' }),
    C('sterling silver necklace', 'material', 'the "Initial Necklace" listing is sterling silver'),
    C('initial necklace', 'product', 'matches the "Personalized Initial Necklace" listing'),
    C('personalized initial necklace', 'product', 'matches the "Personalized Initial Necklace" listing'),
    C('initial necklace christmas gift', 'occasion', 'Christmas is 87 days away; matches the "Personalized Initial Necklace" listing', { occasion: 'Christmas' }),
    C('christmas gift', 'occasion', 'Christmas is 87 days away', { occasion: 'Christmas' }),
    C('christmas jewelry gift', 'occasion', 'Christmas is 87 days away', { occasion: 'Christmas' }),
    C('christmas gift ideas', 'occasion', 'Christmas is 87 days away', { occasion: 'Christmas' }),
    C('heart charm necklace', 'style', 'matches the heart design in the "Gold Filled Dainty Heart Charm Necklace" listing', { top: true }),
    C('dainty charm necklace', 'style', 'matches the dainty look in the "Gold Filled Dainty Heart Charm Necklace" listing'),
    C('gold filled charm bracelet', 'material', 'the "Birthstone Charm Bracelet" listing is gold filled'),
    C('gold filled birthstone charm bracelet', 'material', 'the "Birthstone Charm Bracelet" listing is gold filled'),
    C('birthstone bracelet for grandma', 'recipient', 'Christmas is 87 days away; a gift for grandma fits the "Birthstone Charm Bracelet" listing', { occasion: 'Christmas' }),
    C('engraved name necklace', 'product', 'matches the "Name Necklace" listing')];
  const ideas = {
    'birthstone charm bracelet': { text: 'birthstone charm bracelet', searches: 5400, competition: 'HIGH', competitionIndex: 80 },
    'birthstone charm bracelet christmas gift': { text: 'birthstone charm bracelet christmas gift', searches: 1900, competition: 'MEDIUM', competitionIndex: 50 },
    'personalized charm bracelet gift for mom': { text: 'personalized charm bracelet gift for mom', searches: 2400, competition: 'LOW', competitionIndex: 20 },
    'sterling silver necklace': { text: 'sterling silver necklace', searches: 14800, competition: 'HIGH' },
    'initial necklace': { text: 'initial necklace', searches: 22200, competition: 'UNKNOWN', competitionIndex: 40 },
    'personalized initial necklace': { text: 'personalized initial necklace', searches: '8100', competition: 'UNKNOWN', competitionIndex: 10 },
    'initial necklace christmas gift': { text: 'initial necklace christmas gift', searches: 400, competition: 'LOW' },
    'christmas gift': { text: 'christmas gift', searches: 823000, competition: 'HIGH' },
    'christmas jewelry gift': { text: 'christmas jewelry gift', searches: 260, competition: 'MEDIUM' },
    'christmas gift ideas': { text: 'christmas gift ideas', searches: 550000, competition: 'HIGH' },
    'heart charm necklace': { text: 'heart charm necklace', searches: 1000, competition: 'LOW' },
    'dainty charm necklace': { text: 'dainty charm necklace', searches: 590, competition: 'UNSPECIFIED' },
    'gold filled charm bracelet': { text: 'gold filled charm bracelet', searches: 1300, competition: 'MEDIUM' },
    'gold filled birthstone charm bracelet': { text: 'gold filled birthstone charm bracelet', searches: 90, competition: 'LOW' },
    'engraved name necklace': { text: 'engraved name necklace', searches: 0, competition: 'LOW' },
    'unrelated planner idea necklace': { text: 'unrelated planner idea necklace', searches: 99999, competition: 'HIGH' }
  };
  const ranked = K.rankKeywords(pool, ideas);
  const at = t => ranked.findIndex(r => r.text === t);
  check(ranked.length > 0 && ranked.length <= 12, 'at most 12 keywords: ' + ranked.length);
  same(Object.keys(ranked[0]).sort(), ['competition', 'kind', 'monthlySearches', 'reason', 'text'], 'each keyword has exactly the contract fields');
  check(ranked.every(r => valid(r.text) && r.reason.length <= 100 && K.KINDS.includes(r.kind)), 'contract limits hold');
  check(ranked.every(r => Number.isInteger(r.monthlySearches) && r.monthlySearches > 0), 'every keyword has a measured volume');
  check(ranked.every(r => r.monthlySearches === Number(ideas[r.text].searches)), 'the volume is the planner\'s number');
  check(at('engraved name necklace') < 0, 'a phrase measured at zero is dropped');
  check(!ranked.some(r => /unrelated/.test(r.text)), 'planner ideas that were not candidates are not added');
  check(ranked[0].text === 'initial necklace', 'the largest relevant volume leads: ' + ranked[0].text);
  check(at('birthstone charm bracelet christmas gift') >= 0 && at('birthstone charm bracelet christmas gift') < at('personalized charm bracelet gift for mom'), 'occasion plus product beats a phrase with more searches (1,900 vs 2,400)');
  check(at('heart charm necklace') < at('gold filled charm bracelet'), 'a phrase matching a best seller beats one with more searches (1,000 vs 1,300)');
  check(at('initial necklace christmas gift') >= 0 && at('initial necklace christmas gift') < at('dainty charm necklace'), 'the occasion bonus works alone (400 vs 590)');
  const typed = ranked.filter(r => /necklace|bracelet/.test(r.text)), bare = ranked.filter(r => !/necklace|bracelet/.test(r.text));
  check(bare.length <= 2, 'at most two phrases without a product: ' + bare.map(r => r.text));
  check(ranked.slice(0, typed.length).every(r => /necklace|bracelet/.test(r.text)), 'phrases without a product come after every phrase with one, even at 823,000 searches');
  check(ranked.findIndex(r => r.text === 'christmas gift') !== 0, 'a broad occasion phrase never leads');
  const perKind = k => ranked.filter(r => r.kind === k).length;
  check(K.KINDS.every(k => perKind(k) <= 6) && new Set(ranked.map(r => r.kind)).size >= 4, 'kinds are spread: ' + K.KINDS.map(k => k + ' ' + perKind(k)));
  check(ranked.find(r => r.text === 'initial necklace').reason === 'about 22,000 searches a month; matches the "Personalized Initial Necklace" listing', 'the volume is rounded, and its wording shortens before a listing name does');
  const longName = K.rankKeywords([C('family necklace', 'style', 'matches the family design in the "Family Tree of Life Pendant Necklace 14k Gold Filled Set" listing')], { 'family necklace': { searches: 12100 } })[0].reason;
  check(longName.length <= 100 && /^about 12,000 searches a month; matches the family design in the "Family Tree of Life/.test(longName) && longName.endsWith('listing'), 'a very long listing name is shortened inside its quotes: ' + longName);
  check(ranked.find(r => r.text === 'birthstone charm bracelet').reason === 'about 5,400 searches a month in the target markets; matches the "Birthstone Charm Bracelet" listing', 'a reason that fits is kept whole');
  check(ranked.find(r => r.text === 'personalized charm bracelet gift for mom').reason.startsWith('about 2,400 searches a month in the target markets; Christmas is 87 days away'), 'about 2,400 searches a month in the target markets');
  check(ranked.every(r => /^about [\d,]+ searches? a month( in the target markets)?; \S/.test(r.reason)), 'every measured reason leads with the volume and keeps a specific reason');
  check(ranked.every(r => r.competition === null || ['low', 'medium', 'high'].includes(r.competition)), 'competition is low, medium, high or unknown');
  const comp = t => ranked.find(r => r.text === t).competition;
  same([comp('birthstone charm bracelet'), comp('birthstone charm bracelet christmas gift'), comp('personalized charm bracelet gift for mom')], ['high', 'medium', 'low'], 'the planner\'s competition is mapped');
  same([comp('initial necklace'), comp('personalized initial necklace'), comp('dainty charm necklace')], ['medium', 'low', null], 'an unknown label falls back to the index, and to null without one');
  same(K.rankKeywords(pool, ideas), ranked, 'ranking is repeatable');
  same(K.rankKeywords(pool, new Map(Object.entries(ideas))), ranked, 'a Map of ideas works the same');
  same(K.rankKeywords(pool, Object.values(ideas)), ranked, 'a list of ideas works the same');
  same(K.rankKeywords(pool, ideas, { limit: 4 }).length, 4, 'the limit is respected');
  const frozenPool = freeze(clone(pool)), frozenIdeas = freeze(clone(ideas));
  same(K.rankKeywords(frozenPool, frozenIdeas), ranked, 'frozen inputs are only read, never changed');

  // Spread: ten product phrases with the biggest volumes cannot fill the list
  const wide = [], wideIdeas = {};
  ['gold', 'silver', 'rose gold', 'tiny', 'delicate', 'custom', 'letter', 'mini', 'classic'].forEach((w, i) => { const t = w + ' initial necklace'; wide.push(C(t, 'product', 'matches the "Initial Necklace" listing')); wideIdeas[t] = { text: t, searches: 9000 - i * 500 }; });
  [['christmas necklace', 'occasion'], ['birthstone necklace for mom', 'recipient'], ['sterling silver necklace', 'material'], ['heart necklace', 'style'], ['valentines day necklace', 'occasion'], ['dainty necklace', 'style']]
    .forEach(([t, k], i) => { wide.push(C(t, k, 'matches the "Initial Necklace" listing')); wideIdeas[t] = { text: t, searches: 400 - i * 50 }; });
  const spread = K.rankKeywords(wide, wideIdeas);
  check(spread.length === 12 && spread.filter(r => r.kind === 'product').length <= 6 && new Set(spread.map(r => r.kind)).size === 5, 'no kind takes over: ' + spread.map(r => r.kind + ':' + r.text).join(', '));
  const dupPool = [C('initial necklace', 'product', 'r one'), C('initial necklaces', 'product', 'r two'), C('initial necklace gift', 'product', 'r three'), C('personalized initial necklace', 'product', 'r four'), C('name bracelet', 'product', 'r five'),
    C('christmas name bracelet', 'occasion', 'r six', { occasion: 'Christmas' }), C('heart bracelet', 'style', 'r seven'), C('gold necklace', 'material', 'r eight')];
  const dupIdeas = Object.fromEntries(dupPool.map((c, i) => [c.text, { text: c.text, searches: 5000 - i * 100 }]));
  const noTwins = K.rankKeywords(dupPool, dupIdeas, { limit: 5 });
  check(noTwins.filter(r => /initial necklace/.test(r.text)).length === 1, 'plural, gift and personalized twins of one search are not all returned: ' + noTwins.map(r => r.text));

  // ---- ranking without volumes ----
  const bigPool = cands;
  for (const missing of [undefined, null, {}, new Map(), [], { 'unrelated phrase': { searches: 500 } }, { [bigPool[0].text]: { searches: 0 } }]) {
    const out = K.rankKeywords(bigPool, missing);
    check(out.length === 8, 'eight grounded keywords when nothing is measured: ' + out.length);
    check(out.every(r => r.monthlySearches === null && r.competition === null), 'no volume and no competition are invented');
    check(out.every(r => !/search(es)? a month|\d{3}/.test(r.reason.replace(/87|138/g, ''))), 'no volume figure appears in a reason');
    check(new Set(out.map(r => r.text)).size === 8 && out.every(r => bigPool.some(c => c.text === r.text)), 'they are distinct candidates');
    check(new Set(out.map(r => r.kind)).size >= 4 && K.KINDS.every(k => out.filter(r => r.kind === k).length <= 4), 'kinds are spread: ' + out.map(r => r.kind));
    check(out.every(r => /"[^"]+" listing|days away|tagged|written for/.test(r.reason)) && new Set(out.map(r => r.reason)).size === 8, 'every reason names its listing or occasion, and differs');
    check(out.some(r => r.kind === 'occasion') && out.some(r => r.kind === 'recipient'), 'the occasion and the recipient are both represented');
    same(Object.keys(out[0]).sort(), ['competition', 'kind', 'monthlySearches', 'reason', 'text'], 'same contract fields');
  }
  same(K.rankKeywords(bigPool, {}, { limit: 5 }).length, 5, 'a smaller limit also limits the unmeasured list');
  same(K.rankKeywords(bigPool, {}, { limit: 12 }).length, 8, 'unmeasured never returns more than the best eight');
  // A few measured phrases: they lead, grounded ones without a number fill up to eight
  const few = K.rankKeywords(bigPool, { [bigPool[1].text]: { text: bigPool[1].text, searches: 700, competition: 'LOW' }, [bigPool[4].text]: { text: bigPool[4].text, searches: 90 }, [bigPool[2].text]: { text: bigPool[2].text, searches: 0 } });
  check(few.length === 8 && few[0].monthlySearches === 700 && few[1].monthlySearches === 90 && few.slice(2).every(r => r.monthlySearches === null), 'measured phrases lead and unmeasured ones fill up');
  check(!few.some(r => r.text === bigPool[2].text), 'a phrase measured at zero is not used to fill either');
  check(few.slice(2).every(r => !/search(es)? a month/.test(r.reason)), 'filled entries claim no volume');

  // ---- themes ----
  const kw = [
    { text: "Mother's Day gift", kind: 'occasion' }, { text: 'gift for mom!!' }, { text: 'a b c d e f g h i j k' }, { text: 'x'.repeat(81) }, { text: 'a'.repeat(80) },
    { text: 'caf\u00e9 bracelet' }, { text: 'charm bracelet' }, { text: 'Charm Bracelets' }, { text: '  charm   bracelet  ' }, 'birthstone necklace', { text: '' }, null, { text: 'one two three four five six seven eight nine ten' },
    { text: 'silver-plated ring' }, { text: 'initial necklace' }, { text: 'heart charm' }, { text: 'star charm' }, { text: 'moon charm' }, { text: 'sun charm' }, { text: 'dainty necklace' }, { text: 'name necklace' }, { text: 'evil eye charm' }];
  const themes = K.themesFromKeywords(kw);
  same(themes.slice(0, 5), ['mothers day gift', 'a'.repeat(80), 'charm bracelet', 'birthstone necklace', 'one two three four five six seven eight nine ten'], 'best first, apostrophes removed, invalid and repeated themes dropped');
  check(themes.length === 10 && themes.every(valid) && new Set(themes).size === 10, 'at most ten valid themes: ' + themes.length);
  same(K.themesFromKeywords(K.rankKeywords(pool, ideas)), K.rankKeywords(pool, ideas).map(r => r.text).slice(0, 10), 'ranked keywords become themes in the same order');
  same(K.themesFromKeywords([{ text: 'CHRISTMAS gift' }]), ['christmas gift'], 'themes are lower case');
  same(K.validTheme("Mother's Day"), 'mothers day', 'validTheme removes apostrophes'); same(K.validTheme('gift for mom!'), null, 'validTheme rejects symbols'); same(K.validTheme('a b c d e f g h i j k'), null, 'validTheme rejects eleven words');

  console.log('PASS ' + n + ' pmax search-theme checks (grounded candidates, occasions from timing, volume ranking, no invented volumes, valid themes)');
  require('./suite-guard.cjs').done();
})().catch(e => { console.error(e); process.exitCode = 1; });
