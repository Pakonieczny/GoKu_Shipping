// Performance Max search themes: the searches a real shopper types for the listings in a Product ads idea.
// Performance Max has no keywords; it takes search themes (audience signals). Pure functions used by
// googleAdsAutopilot.js and its offline tests: no network, no AI, no storage, no clock (today is passed in).
//
//   keywordCandidates(candidate, { today, timing, tags, markets, brandSafe }) -> up to 40 { text, kind, reason, occasion, top }
//   plannerSeeds(cands, limit = 40)                                            -> texts to send to Keyword Planner
//   rankKeywords(cands, ideasByText, { limit = 12 })                           -> contract keyword entries
//   themesFromKeywords(keywords)                                               -> up to 10 valid Google search themes
//
// Grounding rules: a jewelry type appears only if the candidate's own listings or offers contain it, materials,
// motifs, personalization and recipients only if a listing (or listing tag) says so, and an occasion only if the
// timing has it as "main" or "also". Nothing is guessed and no volume is ever invented.
'use strict';

const MAX_CANDIDATES = 40, MAX_THEMES = 10, MAX_WORDS = 10, MAX_CHARS = 80, MAX_REASON = 100;
const KINDS = ['product', 'occasion', 'recipient', 'material', 'style'];

// ---------- text helpers ----------
const plain = s => String(s == null ? '' : s).normalize('NFD').replace(/[\u0300-\u036f]/g, '');
// Lower case, apostrophes removed, everything else that is not a letter or digit becomes one space.
const norm = s => plain(s).toLowerCase().replace(/['\u2018\u2019`]/g, '').replace(/[^a-z0-9]+/g, ' ').trim();
const stemWord = w => (w.length > 3 && /[^s]s$/.test(w) ? w.slice(0, -1) : w);
const keyOf = t => t.split(' ').map(stemWord).join(' ');
const uniq = a => [...new Set(a)];

// Google rejects a whole request for one search theme over 10 words, over 80 characters or with anything but
// letters, digits and spaces (after lower-casing and removing apostrophes). Returns the clean text or null.
function validTheme(text) {
  const t = String(text == null ? '' : text).toLowerCase().replace(/['\u2019]/g, '').replace(/\s+/g, ' ').trim();
  if (!t || t.length > MAX_CHARS || t.split(' ').length > MAX_WORDS || !/^[a-z0-9 ]+$/.test(t)) return null;
  return t;
}

const FUNCTION_WORDS = new Set(['for', 'the', 'and', 'with', 'gift', 'gifts', 'a', 'an', 'of', 'to', 'in']);
// Joins parts into one phrase; returns null when a content word would repeat ("charm charm bracelet").
function compose(...parts) {
  const words = [];
  parts.forEach(p => { if (p) String(p).split(' ').forEach(w => { if (w) words.push(w); }); });
  const out = [];
  words.forEach(w => { if (out[out.length - 1] !== w) out.push(w); });
  const seen = new Set();
  for (const w of out) {
    if (w.length > 3 && !FUNCTION_WORDS.has(w)) { const k = stemWord(w); if (seen.has(k)) return null; seen.add(k); }
  }
  return out.join(' ') || null;
}

// Text safe to show in a reason: letters, digits and a few marks only, so no symbols or emoji reach a card.
const shown = s => String(s == null ? '' : s).replace(/[^\p{L}\p{N} '&.\-\/]/gu, '').replace(/\s+/g, ' ').trim();
const cleanTitle = s => String(s == null ? '' : s).replace(/\s+/g, ' ').trim();
// The first part of a keyword-stuffed listing title ("Initial Necklace, Sterling Silver, Gift for Mom").
const coreOf = title => cleanTitle(title).split(/\s+[-\u2013\u2014|]\s+|[|,;:\u2013\u2014]/)[0].trim();
function refTitle(title) {
  let s = shown(coreOf(title));
  if (s.length > 48) s = s.slice(0, 45).replace(/\s+\S*$/, '') + '...';
  return s;
}
// Keeps a reason within the limit by shortening quoted listing names first, never by dropping its start.
function fitReason(text, max = MAX_REASON) {
  let s = String(text == null ? '' : text).replace(/\s+/g, ' ').trim();
  if (s.length <= max) return s;
  for (const cut of [30, 22, 16, 12]) {
    s = s.replace(/"([^"]+)"/g, (m, q) => (q.length > cut ? '"' + q.slice(0, cut - 3).trimEnd() + '..."' : m));
    if (s.length <= max) return s;
  }
  return s.slice(0, max - 3).trimEnd() + '...';
}

// ---------- vocabulary ----------
// Canonical jewelry types found in a piece of text. "charm only" listings are charms; a charm on a necklace or
// bracelet makes the search "charm necklace" / "charm bracelet". A type is only ever reported if its word is there.
function typesIn(text) {
  let t = ' ' + norm(text) + ' ';
  const out = [];
  if (/ charm only /.test(t)) return ['charm'];
  if (/ (keychains?|key chains?|key rings?|keyrings?) /.test(t)) { out.push('keychain'); t = t.replace(/ (key chains?|key rings?|keyrings?) /g, ' '); }
  const charm = / charms? /.test(t), neck = / (necklaces?|chokers?|lariats?) /.test(t);
  // a type keeps the word the listing uses: a cuff is searched as "cuff bracelet", a choker as "choker necklace"
  if (/ necklaces? /.test(t)) out.push(charm ? 'charm necklace' : 'necklace');
  else if (/ chokers? /.test(t)) out.push('choker necklace');
  else if (/ lariats? /.test(t)) out.push('lariat necklace');
  if (/ bracelets? /.test(t)) out.push(charm ? 'charm bracelet' : 'bracelet');
  else if (/ bangles? /.test(t)) out.push('bangle bracelet');
  else if (/ cuffs? /.test(t)) out.push('cuff bracelet');
  if (/ (earrings?|hoops?|studs?) /.test(t)) out.push(/ hoops? /.test(t) ? 'hoop earrings' : / studs? /.test(t) ? 'stud earrings' : 'earrings');
  if (/ rings? /.test(t)) out.push('ring');
  if (/ anklets? /.test(t)) out.push('anklet');
  if (!neck && / lockets? /.test(t)) out.push('locket');
  if (!neck && / pendants? /.test(t)) out.push('pendant');
  if (!out.length && charm) out.push('charm');
  return uniq(out);
}
const BASE_TYPE = { 'charm necklace': 'necklace', 'choker necklace': 'necklace', 'lariat necklace': 'necklace', 'charm bracelet': 'bracelet', 'bangle bracelet': 'bracelet',
  'cuff bracelet': 'bracelet', 'hoop earrings': 'earrings', 'stud earrings': 'earrings' };
const baseType = t => BASE_TYPE[t] || t;

function materialsIn(text) {
  const t = ' ' + norm(text) + ' ', out = [];
  if (/ sterling( silver)? | 925 (sterling )?silver | 925 sterling /.test(t)) out.push('sterling silver');
  if (/ solid (\d+ ?k(t|arat)? |\d+ karat )?gold /.test(t)) out.push('solid gold');
  if (/ rose gold filled /.test(t)) out.push('rose gold filled'); else if (/ rose gold /.test(t)) out.push('rose gold');
  const k = /(?:^| )(10|14|18) ?k(?:t|arat)?(?: |$)/.exec(t);
  if (/ gold filled /.test(t) && !/ rose gold filled /.test(t)) out.push('gold filled');
  else if (k) out.push(k[1] + 'k gold');
  if (/ white gold /.test(t)) out.push('white gold');
  if (/ gold plated /.test(t)) out.push('gold plated');
  if (!out.some(m => /silver/.test(m)) && / silver /.test(t)) out.push('silver');
  if (!out.some(m => /gold/.test(m)) && / gold /.test(t)) out.push('gold');
  return uniq(out);
}
const MATERIAL_STRIP = /\b(sterling silver|925 sterling silver|925 silver|925|sterling|solid gold|rose gold filled|rose gold|white gold|yellow gold|gold filled|gold plated|gold vermeil|(?:10|14|18|24) ?k(?:t|arat)?|karat|silver|gold|filled|plated|solid)\b/g;

const PERS = [
  ['engraved', / (engraved|engraving|engrave|engravable) /], ['birthstone', / birthstones? /], ['initial', / initials? /],
  ['photo', / photos? /], ['name', / (names?|nameplate) /], ['monogram', / monogram(med|s)? /],
  ['coordinates', / coordinates? /], ['handwriting', / (handwriting|handwritten) /], ['fingerprint', / fingerprints? /]
];
const PERS_GENERIC = / (personali[sz]ed|customi[sz]ed|custom|made to order) /;
// Personalization the listing text offers: specific words first ("birthstone"), then the generic "personalized".
function persIn(text) {
  const t = ' ' + norm(text) + ' ', out = PERS.filter(([, re]) => re.test(t)).map(([w]) => w);
  if (out.length || PERS_GENERIC.test(t)) out.push('personalized');
  return out;
}
const STYLES = ['dainty', 'minimalist', 'delicate', 'tiny', 'vintage', 'boho', 'layered', 'layering', 'statement', 'simple'];
const stylesIn = text => { const t = ' ' + norm(text) + ' '; return STYLES.filter(w => t.includes(' ' + w + ' ')); };

const MOTIFS = ['tree of life', 'evil eye', 'birth flower', 'paw print', 'heart', 'butterfly', 'flower', 'star', 'moon', 'sun', 'cross',
  'angel', 'clover', 'infinity', 'feather', 'anchor', 'bee', 'dog', 'cat', 'horse', 'bird', 'turtle', 'elephant', 'owl', 'fox', 'rose',
  'daisy', 'sunflower', 'pearl', 'zodiac', 'wings', 'lotus', 'compass', 'shell', 'crown', 'snowflake'];
const motifsIn = text => { const t = ' ' + norm(text) + ' '; return MOTIFS.filter(m => t.includes(' ' + m + ' ') || t.includes(' ' + m + 's ')); };

// Who a listing names (its own words) and who an occasion is usually for.
const RECIPIENTS = [
  ['mom', / (mom|moms|mommy|mama|mother|mothers) /], ['grandma', / (grandma|grandmas|grandmother|nana|nanna|grammy|granny) /],
  ['daughter', / (daughter|daughters|granddaughter) /], ['sister', / (sister|sisters) /],
  ['best friend', / (best friends?|bestie|besties|bff) /], ['wife', / (wife|wives) /], ['girlfriend', / girlfriends? /],
  ['bridesmaid', / bridesmaids? /], ['teacher', / teachers? /], ['nurse', / nurses? /],
  ['dad', / (dad|daddy|father) /], ['husband', / husbands? /], ['grandpa', / (grandpa|grandfather|papa) /], ['boyfriend', / boyfriends? /]
];
const MEN = new Set(['dad', 'husband', 'grandpa', 'boyfriend', 'brother', 'him']);
function recipientsIn(text) {
  const t = ' ' + norm(text).replace(/ (mothers|fathers) day /g, ' ').replace(/ mother of pearl /g, ' ') + ' ';
  return RECIPIENTS.filter(([, re]) => re.test(t)).map(([w]) => w);
}
const OCC_RECIPIENTS = [
  [/mother/, ['mom', 'grandma', 'wife']], [/father/, ['dad', 'husband', 'grandpa']], [/galentine|friendship/, ['best friend', 'sister']],
  [/valentine/, ['her', 'girlfriend', 'wife']], [/anniversary/, ['wife', 'girlfriend', 'her']],
  [/christmas|xmas|holiday|hanukkah/, ['her', 'mom', 'daughter', 'best friend', 'grandma', 'sister', 'wife']],
  [/birthday/, ['her', 'mom', 'daughter', 'sister', 'best friend', 'grandma']], [/graduat/, ['her', 'daughter', 'best friend']],
  [/wedding|bridal|bride/, ['bridesmaid', 'bride', 'mother of the bride']], [/baby shower|new baby/, ['new mom', 'mom to be']],
  [/teacher/, ['teacher']], [/nurse/, ['nurse']]
];
const MEN_TYPES = new Set(['bracelet', 'necklace', 'keychain', 'ring', 'bangle bracelet', 'cuff bracelet', 'pendant']);
const FEMININE = /\b(dainty|delicate|women|womens|ladies|her|girl|girls|mom|mama|grandma|daughter|sister|bride|bridesmaid|butterfly|flower|floral|heart|earrings?|anklet|locket)\b/;
const MASCULINE = /\b(men|mens|him|dad|father|husband|boyfriend|groomsman|groomsmen)\b/;
function recipientFits(r, unit) {
  const text = norm(unit.text);
  if (MEN.has(r)) return unit.types.some(t => MEN_TYPES.has(t)) && !FEMININE.test(text);
  return !MASCULINE.test(text);
}

// Occasion words never come from a title: a listing that says "Mother's Day" does not make Mother's Day a season.
const OCC_PHRASES = /\b((?:mothers|fathers|valentines|galentines|new years|christmas|thanksgiving|memorial|labor|independence|teachers|nurses|friendship|grandparents) day|black friday|cyber monday|teacher appreciation(?: week)?|nurses week|baby shower|new year|christmas eve)\b/g;
const OCC_TOKENS = new Set(['christmas', 'xmas', 'holiday', 'holidays', 'hanukkah', 'easter', 'halloween', 'thanksgiving', 'valentine', 'valentines',
  'galentine', 'galentines', 'birthday', 'anniversary', 'graduation', 'wedding', 'bridal', 'prom', 'memorial', 'sympathy', 'mothers', 'fathers']);
const OCC_TEXT = /\b(christmas|xmas|holidays?|hanukkah|easter|halloween|thanksgiving|valentines?|galentines?|birthday|anniversary|graduation|wedding|bridal|prom|memorial|sympathy|mothers day|fathers day|black friday|cyber monday|new years?|teacher appreciation|nurses week|baby shower)\b/;

// Selling copy, sizes and filler that describe a listing without saying what it is: never part of a search or a motif.
const MARKETING = new Set(('unique beautiful gorgeous lovely perfect cute pretty amazing great special handmade handcrafted hand made brand new best seller ' +
  'bestseller sale free shipping gift gifts set pack pair jewelry jewellery jewel jewels fine quality premium luxury exclusive limited edition women womens ' +
  'woman ladies girls girl kids men mens her him for with and the a an of in on to by from your you my our is it this that one two three size inch inches ' +
  'mm cm pcs piece pieces only no not ready ship today collection series style design designer stainless steel waterproof tarnish resistant hypoallergenic ' +
  'adjustable lobster clasp extra long short big large small medium thick thin wide narrow').split(' '));
// Words that name a type or a kind of personalization: real search words, but never a motif of their own.
const NOT_MOTIF = new Set([...MARKETING, ...('charm charms pendant pendants necklace necklaces bracelet bracelets bangle bangles cuff cuffs earring earrings hoop hoops ' +
  'stud studs ring rings anklet anklets locket lockets keychain keychains choker chokers lariat lariats initial initials name names birthstone birthstones ' +
  'photo photos personalized personalised custom customized engraved engraving').split(' ')]);
const BRAND_RE = /\bbrites\b|britesjewelry/;

// The listing title as a search: selling copy, sizes, metals and occasion words removed, must still name a type.
// "of", "with" and the like stay only between two words that were side by side in the title.
const JOINERS = new Set(['of', 'with', 'and', 'in', 'on', 'the', 'a', 'an', 'to', 'by', 'from']);
function productPhrase(title) {
  let s = ' ' + norm(coreOf(title).split(/\s+for\s+/i)[0]) + ' ';
  s = s.replace(OCC_PHRASES, ' ').replace(MATERIAL_STRIP, ' ');
  const toks = s.split(' ').filter(Boolean);
  const drop = w => /^\d+$/.test(w) || OCC_TOKENS.has(w) || BRAND_RE.test(w) || MARKETING.has(w);
  const keep = toks.map(w => !drop(w));
  // the original neighbours decide whether a joiner survives, so removed words leave no dangling "of"
  const words = toks.filter((w, i) => (JOINERS.has(w) ? i > 0 && i < toks.length - 1 && keep[i - 1] && keep[i + 1] : keep[i]));
  const phrase = words.join(' ');
  return words.length >= 2 && words.length <= 6 && typesIn(phrase).length ? phrase : null;
}
// A motif is a design word the shop names in the title: a known one, or the one or two unexplained words left over.
function motifsOfTitle(title) {
  const core = coreOf(title).split(/\s+for\s+/i)[0];
  const known = motifsIn(core);
  let s = ' ' + norm(core) + ' ';
  s = s.replace(OCC_PHRASES, ' ').replace(MATERIAL_STRIP, ' ');
  const covered = new Set(known.flatMap(m => m.split(' ')).concat(known.map(m => m + 's')));
  const left = s.split(' ').filter(w => w.length >= 3 && /^[a-z]+$/.test(w) && !NOT_MOTIF.has(w) && !OCC_TOKENS.has(w) && !BRAND_RE.test(w) &&
    !covered.has(w) && !STYLES.includes(w) && !RECIPIENTS.some(([, re]) => re.test(' ' + w + ' ')) && !PERS.some(([, re]) => re.test(' ' + w + ' ')));
  const out = known.slice();
  if (left.length && left.length <= 2) out.push(left.join(' '));
  return uniq(out);
}

// ---------- the candidate's listings ----------
function topTitles(evidence) {
  const rows = (Array.isArray(evidence) ? evidence : []).filter(e => e && (e.title || e.soldTitle))
    .map(e => ({ e, s: [Number(e.orders) || 0, Number(e.units) || 0, Number(e.revenue) || 0] })).filter(x => x.s.some(n => n > 0));
  rows.sort((a, b) => b.s[0] - a.s[0] || b.s[1] - a.s[1] || b.s[2] - a.s[2]);
  const set = new Set();
  rows.slice(0, 2).forEach(r => { if (r.e.title) set.add(norm(r.e.title)); if (r.e.soldTitle) set.add(norm(r.e.soldTitle)); });
  return set;
}
const withRef = (words, ref) => words.map(w => ({ w, ref }));
const mergeAttrs = (own, extra) => { const seen = new Set(own.map(a => a.w)); return own.concat(extra.filter(a => !seen.has(a.w))); };

// Listing tags (strings, or { t, n } from the collection profile): the shop's own search words, shared by the group.
function tagAttributes(tags) {
  const out = { pers: [], materials: [], styles: [], motifs: [], recipients: [] };
  (Array.isArray(tags) ? tags : []).forEach(x => {
    const raw = typeof x === 'string' ? x : x && (x.t || x.tag || x.name);
    const tag = shown(raw);
    if (!tag || norm(tag).split(' ').length > 6) return;
    const ref = 'listings tagged "' + tag + '"', t = ' ' + norm(tag) + ' ';
    out.pers.push(...withRef(persIn(t), ref)); out.materials.push(...withRef(materialsIn(t), ref));
    out.styles.push(...withRef(stylesIn(t), ref)); out.motifs.push(...withRef(motifsIn(t), ref));
    out.recipients.push(...withRef(recipientsIn(t), ref));
  });
  for (const k of Object.keys(out)) out[k] = mergeAttrs([], out[k]);
  return out;
}

function buildUnits(cand, tagAttr) {
  const clean = a => uniq((a || []).map(cleanTitle).filter(Boolean));
  const offers = Array.isArray(cand.offerDetails) ? cand.offerDetails.filter(o => o && typeof o === 'object') : [];
  let titles = clean(cand.productTitles).slice(0, 8);
  if (!titles.length) titles = clean(offers.map(o => o.productTitle || o.title)).slice(0, 8);
  const top = topTitles(cand.demandEvidence);
  const fallbackTypes = uniq((Array.isArray(cand.types) ? cand.types : []).flatMap(typesIn));
  const units = titles.map(title => {
    const mine = offers.filter(o => norm(o.productTitle) === norm(title));
    const own = mine.length ? mine : titles.length === 1 ? offers : [];
    const offerTitles = own.map(o => o.title).filter(Boolean).join(' ');
    const labels = own.flatMap(o => (Array.isArray(o.customLabels) ? o.customLabels : [])).join(' ');
    const typeText = own.flatMap(o => [o.type1, o.type2]).filter(Boolean).join(' ');
    let types = typesIn(title);
    if (!types.length) types = typesIn(offerTitles);
    if (!types.length) types = typesIn(typeText);
    const text = [title, offerTitles, labels].join(' ');
    const ref = 'the "' + refTitle(title) + '" listing';
    return {
      title, ref, text, types, top: top.has(norm(title)), phrase: productPhrase(title),
      pers: mergeAttrs(withRef(persIn(text), ref), tagAttr.pers), materials: mergeAttrs(withRef(materialsIn(text), ref), tagAttr.materials),
      styles: mergeAttrs(withRef(stylesIn(text), ref), tagAttr.styles),
      motifs: mergeAttrs(withRef(motifsOfTitle(title), ref), tagAttr.motifs),
      recipients: mergeAttrs(withRef(recipientsIn(text), ref), tagAttr.recipients)
    };
  });
  // Only when no listing or offer shows a type does the collection's own type list stand in for it.
  const anyType = units.some(u => u.types.length);
  if (!anyType && fallbackTypes.length) units.forEach(u => { u.types = fallbackTypes.slice(); });
  if (!units.length && cand.collectionTitle) {
    const title = cleanTitle(cand.collectionTitle), ref = 'the "' + refTitle(title) + '" collection';
    units.push({ title, ref, text: title, types: fallbackTypes.slice(), top: false, phrase: null,
      pers: mergeAttrs(withRef(persIn(title), ref), tagAttr.pers), materials: mergeAttrs(withRef(materialsIn(title), ref), tagAttr.materials),
      styles: mergeAttrs(withRef(stylesIn(title), ref), tagAttr.styles), motifs: mergeAttrs([], tagAttr.motifs),
      recipients: mergeAttrs(withRef(recipientsIn(title), ref), tagAttr.recipients) });
  }
  return units.filter(u => u.types.length);
}

// ---------- occasions (only what the timing says) ----------
function daysBetween(today, date) {
  const a = /^(\d{4})-(\d{2})-(\d{2})/.exec(String(today || '')), b = /^(\d{4})-(\d{2})-(\d{2})/.exec(String(date || ''));
  if (!a || !b) return null;
  return Math.round((Date.UTC(+b[1], +b[2] - 1, +b[3]) - Date.UTC(+a[1], +a[2] - 1, +a[3])) / 86400000);
}
function occasionsFrom(timing, today, markets) {
  const mk = Array.isArray(markets) && markets.length ? new Set(markets.map(m => String(m).toUpperCase())) : null;
  const rows = (Array.isArray(timing) ? timing : []).filter(t => t && (t.role === 'main' || t.role === 'also') && t.label)
    .filter(t => !(mk && t.market && !mk.has(String(t.market).toUpperCase())))
    .map(t => ({ t, days: t.daysAway != null && t.daysAway !== '' && Number.isFinite(Number(t.daysAway)) ? Math.round(Number(t.daysAway)) : daysBetween(today, t.date) }))
    .filter(r => r.days == null || r.days >= 0);
  const dayKey = r => (r.days == null ? Infinity : r.days);
  rows.sort((a, b) => (a.t.role === b.t.role ? 0 : a.t.role === 'main' ? -1 : 1) || (dayKey(a) === dayKey(b) ? 0 : dayKey(a) < dayKey(b) ? -1 : 1));
  const out = [], seen = new Set();
  rows.forEach(({ t, days }) => {
    const label = shown(String(t.label).replace(/\([^)]*\)/g, ' '));
    if (!label || /evergreen|year round|everyday/i.test(label)) return;
    String(t.label).replace(/\([^)]*\)/g, ' ').split('/').forEach(part => {
      const phrase = norm(part).replace(/ (gifting|gifts?)$/, '');
      if (!phrase || phrase.split(' ').length > 3 || seen.has(phrase) || out.length >= 3) return;
      seen.add(phrase);
      out.push({ label, phrase, days, role: t.role, when: days == null ? label + ' is coming up' : days === 0 ? label + ' is today' : days === 1 ? label + ' is tomorrow' : label + ' is ' + days + ' days away' });
    });
  });
  return out;
}
const recipientsForOccasion = phrase => { const hit = OCC_RECIPIENTS.find(([re]) => re.test(phrase)); return hit ? hit[1] : []; };

// ---------- candidates ----------
function keywordCandidates(candidate, opts) {
  const o = opts && typeof opts === 'object' ? opts : {}, cand = candidate && typeof candidate === 'object' ? candidate : {};
  const safe = typeof o.brandSafe === 'function' ? t => { try { return !!o.brandSafe(t); } catch (e) { return false; } } : () => true;
  const uk = Array.isArray(o.markets) && o.markets.length && !o.markets.some(m => /^(US|CA)$/i.test(m)) && o.markets.some(m => /^(GB|UK|AU|NZ|IE|ZA|IN)$/i.test(m));
  const jewel = uk ? 'jewellery' : 'jewelry';
  const tagAttr = tagAttributes(o.tags), units = buildUnits(cand, tagAttr), occs = occasionsFrom(o.timing, o.today, o.markets);
  const lists = { product: [], occasion: [], recipient: [], material: [], style: [] }, seen = new Set();
  // Safety net: whatever a title held, an occasion word may appear only when the timing lists that occasion.
  const allowedOcc = new Set(occs.flatMap(oc => oc.phrase.split(' ')));
  const occasionOk = t => (t.match(new RegExp(OCC_TEXT.source, 'g')) || []).every(hit => hit.split(' ').every(w => allowedOcc.has(w)));
  const add = (kind, text, reason, extra) => {
    const t = validTheme(text); if (!t || BRAND_RE.test(t) || !occasionOk(t)) return;
    const k = keyOf(t); if (seen.has(k) || !safe(t)) return;
    seen.add(k);
    lists[kind].push({ text: t, kind, reason: fitReason(reason), occasion: (extra && extra.occasion) || null, top: !!(extra && extra.top) });
  };
  // Each pair is one listing and one of its own types, best seller first.
  const pairs = []; units.slice(0, 5).forEach(u => u.types.slice(0, 2).forEach(t => pairs.push({ u, t })));
  const specific = u => u.pers.filter(a => a.w !== 'personalized');
  const ownSpecific = u => specific(u).filter(a => a.ref === u.ref);
  const lead = p => { const a = ownSpecific(p.u)[0]; return compose(a && a.w, p.t); };         // "birthstone charm bracelet"
  const persAttr = u => u.pers.find(a => a.w === 'personalized') || u.pers[0];
  const personalized = p => (p.u.pers.length ? compose('personalized', p.t) : null);           // "personalized charm bracelet"
  const T = p => ({ top: p.u.top });
  const matches = ref => 'matches ' + ref;
  const madeOf = m => (/^the /.test(m.ref) ? m.ref + ' is ' + m.w : matches(m.ref));
  const lookOf = (a, what) => (/^the /.test(a.ref) ? 'matches the ' + a.w + ' ' + what + ' in ' + a.ref : matches(a.ref));

  // product: the listing itself, then what it offers
  units.slice(0, 5).forEach(u => u.phrase && add('product', u.phrase, matches(u.ref) + (u.top ? ', one of your best sellers' : ''), { top: u.top }));
  pairs.forEach(p => specific(p.u).slice(0, 2).forEach(a => add('product', compose(a.w, p.t), matches(a.ref), T(p))));
  pairs.slice(0, 4).forEach(p => p.u.pers.length && add('product', personalized(p), matches(persAttr(p.u).ref), T(p)));
  pairs.slice(0, 2).forEach(p => add('product', compose(lead(p), 'gift'), matches(p.u.ref), T(p)));

  // occasion: only what the timing lists as main or also
  const withOcc = (occ, text, why, p) => add('occasion', text, why ? occ.when + '; ' + why : occ.when, { occasion: occ.label, top: p ? p.u.top : false });
  occs.forEach(occ => pairs.slice(0, 3).forEach(p => withOcc(occ, compose(lead(p), occ.phrase, 'gift'), matches(p.u.ref), p)));
  occs.forEach(occ => pairs.slice(0, 3).forEach(p => withOcc(occ, compose(occ.phrase, p.t), matches(p.u.ref), p)));
  occs.forEach(occ => withOcc(occ, compose(occ.phrase, jewel, 'gift'), null, null));
  occs.forEach(occ => pairs.slice(0, 2).forEach(p => p.u.pers.length && withOcc(occ, compose(personalized(p), occ.phrase, 'gift'), matches(p.u.ref), p)));
  occs.forEach(occ => withOcc(occ, compose(occ.phrase, 'gift'), null, null));

  // recipient: the listing names them, or the occasion implies them and the listing plausibly fits
  const recips = [];
  const addRecip = (r, via, ref, unit) => {
    const key = r + '|' + (via ? via.phrase : '') + '|' + (unit ? unit.title : '');
    if (r && !recips.some(x => x.key === key)) recips.push({ key, r, via, ref, unit });
  };
  units.slice(0, 5).forEach(u => u.recipients.forEach(a => { if (a.ref === u.ref) addRecip(a.w, null, a.ref, u); }));
  tagAttr.recipients.forEach(a => addRecip(a.w, null, a.ref, null));
  occs.forEach(occ => recipientsForOccasion(occ.phrase).forEach(r => addRecip(r, occ, null, null)));
  const useRecips = recips.slice(0, 10);
  // A recipient a listing names belongs to that listing; an occasion's recipient needs a listing that plausibly fits.
  const fits = (rec, p) => (rec.unit ? p.u === rec.unit : rec.via ? recipientFits(rec.r, p.u) : true);
  const recWhy = (rec, p) => (rec.via ? rec.via.when + '; a gift for ' + rec.r + ' fits ' + p.u.ref : (/^the /.test(rec.ref) ? rec.ref + ' is written for ' + rec.r : matches(rec.ref)));
  const recOcc = rec => (rec.via ? rec.via.label : null);
  useRecips.forEach(rec => pairs.filter(p => fits(rec, p)).slice(0, 2).forEach(p =>
    add('recipient', compose(p.u.pers.length ? 'personalized' : '', p.t, 'gift for', rec.r), recWhy(rec, p), { top: p.u.top, occasion: recOcc(rec) })));
  useRecips.forEach(rec => rec.via && pairs.some(p => fits(rec, p)) && add('recipient', compose(rec.via.phrase, 'gift for', rec.r), rec.via.when + '; a gift for ' + rec.r, { occasion: rec.via.label }));
  useRecips.forEach(rec => pairs.filter(p => fits(rec, p)).slice(0, 1).forEach(p => add('recipient', compose(lead(p), 'for', rec.r), recWhy(rec, p), { top: p.u.top, occasion: recOcc(rec) })));

  // material: metals the listings state
  pairs.slice(0, 4).forEach(p => p.u.materials.slice(0, 2).forEach(m => add('material', compose(m.w, p.t), madeOf(m), T(p))));
  pairs.slice(0, 4).forEach(p => p.u.materials.slice(0, 1).forEach(m => ownSpecific(p.u).slice(0, 1).forEach(a => add('material', compose(m.w, a.w, p.t), madeOf(m), T(p)))));
  pairs.slice(0, 3).forEach(p => p.u.materials.slice(0, 1).forEach(m => add('material', compose(m.w, p.t, 'gift'), madeOf(m), T(p))));

  // style: design motifs and looks the listings name
  pairs.slice(0, 4).forEach(p => p.u.motifs.slice(0, 2).forEach(m => add('style', compose(m.w, p.t), lookOf(m, 'design'), T(p))));
  pairs.slice(0, 4).forEach(p => p.u.styles.slice(0, 2).forEach(a => add('style', compose(a.w, p.t), lookOf(a, 'look'), T(p))));
  pairs.slice(0, 3).forEach(p => p.u.styles.slice(0, 1).forEach(a => p.u.materials.slice(0, 1).forEach(m => add('style', compose(a.w, m.w, p.t), lookOf(a, 'look'), T(p)))));
  pairs.slice(0, 3).forEach(p => p.u.motifs.slice(0, 1).forEach(m => add('style', compose(m.w, p.t, 'gift'), lookOf(m, 'design'), T(p))));

  // Take turns between kinds so the first entries already cover the whole idea.
  const out = [], idx = {};
  KINDS.forEach(k => { idx[k] = 0; });
  while (out.length < MAX_CANDIDATES && KINDS.some(k => idx[k] < lists[k].length)) {
    for (const k of KINDS) {
      if (out.length >= MAX_CANDIDATES) break;
      if (idx[k] < lists[k].length) out.push(lists[k][idx[k]++]);
    }
  }
  return out;
}

function plannerSeeds(cands, limit = MAX_CANDIDATES) {
  const cap = Math.max(0, Math.min(MAX_CANDIDATES, parseInt(limit, 10) || 0)), out = [], seen = new Set();
  (Array.isArray(cands) ? cands : []).forEach(c => {
    const t = validTheme(typeof c === 'string' ? c : c && c.text);
    if (t && out.length < cap && !seen.has(t)) { seen.add(t); out.push(t); }
  });
  return out;
}

// ---------- ranking ----------
const competitionOf = v => {
  const s = String(v.competition == null ? '' : v.competition).toLowerCase();
  if (s === 'low' || s === 'medium' || s === 'high') return s;
  const i = v.competitionIndex == null || v.competitionIndex === '' ? NaN : Number(v.competitionIndex);
  return Number.isFinite(i) ? (i >= 66 ? 'high' : i >= 33 ? 'medium' : 'low') : null;
};
function ideaInfo(v) {
  if (typeof v === 'number') return { volume: Number.isFinite(v) ? Math.max(0, Math.round(v)) : null, competition: null };
  if (!v || typeof v !== 'object') return { volume: null, competition: null };
  const raw = v.searches != null ? v.searches : v.avgMonthlySearches != null ? v.avgMonthlySearches : v.monthlySearches;
  const n = raw == null || raw === '' ? NaN : Number(raw);
  return { volume: Number.isFinite(n) ? Math.max(0, Math.round(n)) : null, competition: competitionOf(v) };
}
// What keywordResearchPool returns: an object keyed by lower-case text (a Map or a list of ideas also works).
function ideaIndex(ideasByText) {
  const map = new Map(), put = (text, v) => { const k = norm(text); if (k && !map.has(k)) map.set(k, ideaInfo(v)); };
  if (ideasByText instanceof Map) ideasByText.forEach((v, k) => put((v && v.text) || k, v));
  else if (Array.isArray(ideasByText)) ideasByText.forEach(v => v && put(v.text, v));
  else if (ideasByText && typeof ideasByText === 'object') Object.keys(ideasByText).forEach(k => { const v = ideasByText[k]; put((v && typeof v === 'object' && v.text) || k, v); });
  return map;
}
function prepare(cands) {
  const out = [], seen = new Set();
  (Array.isArray(cands) ? cands : []).forEach(c => {
    const obj = typeof c === 'string' ? { text: c } : c;
    const text = validTheme(obj && obj.text); if (!text) return;
    const k = keyOf(text); if (seen.has(k)) return; seen.add(k);
    const occasion = obj.occasion || (OCC_TEXT.test(text) ? 'occasion' : null);
    const kind = KINDS.includes(obj.kind) ? obj.kind : occasion ? 'occasion' : 'product';
    out.push({ text, kind, occasion, top: !!obj.top, reason: fitReason(cleanTitle(obj.reason) || 'a search a shopper would type for these listings') });
  });
  return out;
}
const hasType = text => typesIn(text).length > 0;
// Occasion plus product is what a shopper buys on; a best seller is what the shop can deliver.
function weightOf(c) {
  let w = 1;
  if (hasType(c.text)) { if (c.occasion) w *= 1.6; if (c.kind === 'recipient') w *= 1.1; } else w *= 0.15;
  if (c.top) w *= 1.4;
  return w;
}
const LOW_INFO = new Set(['for', 'gift', 'gifts', 'the', 'a', 'an', 'and', 'with', 'of', 'personalized', 'personalised', 'custom', 'jewelry', 'jewellery']);
const contentTokens = text => new Set(norm(text).split(' ').filter(w => w && !LOW_INFO.has(w)).map(stemWord));
// Two phrases are the same search when they share nearly every word. Inside one kind that includes a phrase that
// only adds one word; across kinds ("birthstone necklace" and the same words plus an occasion) it takes more overlap.
function nearDup(x, y, strict) {
  const a = x.tokens, b = y.tokens, same = x.kind === y.kind;
  let inter = 0; a.forEach(w => { if (b.has(w)) inter++; });
  const union = a.size + b.size - inter;
  if (!union) return true;
  const j = inter / union;
  if (same) return j >= 0.75 || (strict && inter === Math.min(a.size, b.size) && Math.abs(a.size - b.size) <= 1);
  return strict ? j > 0.75 : j >= 0.9;
}
// Best first, but no kind or type takes over the list and no phrase repeats another.
function pickDiverse(rows, n, seed) {
  const chosen = [], all = () => (seed || []).concat(chosen), capKind = Math.max(2, Math.ceil(n / 2)), capType = new Set(rows.map(r => r.base).filter(Boolean)).size > 1 ? Math.max(2, Math.ceil(n * 0.6)) : Infinity;
  const count = (pred) => all().filter(pred).length;
  const passes = [
    r => count(x => x.kind === r.kind) < capKind && (!r.base || count(x => x.base === r.base) < capType) && (r.base || count(x => !x.base) < 2) && !all().some(x => nearDup(x, r, true)),
    r => (r.base || count(x => !x.base) < 2) && !all().some(x => nearDup(x, r, true)),
    r => !all().some(x => nearDup(x, r, false)),
    () => true
  ];
  for (const ok of passes) for (const r of rows) { if (chosen.length >= n) return chosen; if (!chosen.includes(r) && ok(r)) chosen.push(r); }
  return chosen;
}
const commas = n => String(n).replace(/\B(?=(\d{3})+(?!\d))/g, ',');
function approx(v) { return v >= 10000 ? Math.round(v / 1000) * 1000 : v >= 1000 ? Math.round(v / 100) * 100 : v >= 100 ? Math.round(v / 10) * 10 : v; }
const volumePhrase = v => 'about ' + commas(approx(v)) + (v === 1 ? ' search' : ' searches') + ' a month in the target markets';
// The volume first, then as much of the specific reason as fits: whole clauses are dropped from the end before any is cut.
function volumeReason(v, reason) {
  const parts = String(reason).split('; ').filter(Boolean), head = volumePhrase(v);
  for (let n = parts.length; n >= 1; n--) {
    const line = head + '; ' + parts.slice(0, n).join('; ');
    if (line.length <= MAX_REASON) return line;
  }
  // even the first clause is too long beside the full volume wording: shorten the wording before the listing name
  const short = head.replace(/ in the target markets$/, '');
  for (const line of [short + '; ' + parts.join('; '), short + '; ' + parts[0]]) if (line.length <= MAX_REASON) return line;
  return fitReason(short + '; ' + parts[0]);
}
function rankKeywords(cands, ideasByText, opts) {
  const { limit = 12, fill = 8 } = opts && typeof opts === 'object' ? opts : {};
  const cap = Math.max(1, Math.min(25, parseInt(limit, 10) || 12)), want = Math.min(cap, Math.max(1, parseInt(fill, 10) || 8));
  const ideas = ideaIndex(ideasByText);
  const rows = prepare(cands).map((c, i) => {
    const info = ideas.get(norm(c.text)) || null, ts = typesIn(c.text);
    return { c, i, kind: c.kind, info, tokens: contentTokens(c.text), base: ts.length ? baseType(ts[0]) : '', typed: ts.length > 0 };
  });
  const measured = rows.filter(r => r.info && r.info.volume > 0);            // measured-zero phrases (info.volume === 0) are dropped
  const open = rows.filter(r => !r.info || r.info.volume == null);
  measured.forEach(r => { r.score = r.info.volume * weightOf(r.c); });
  const byScore = (a, b) => (b.typed - a.typed) || (b.score - a.score) || (a.i - b.i);
  measured.sort(byScore);
  const picked = pickDiverse(measured, cap, []).sort(byScore);
  // Fewer than a handful of measured phrases: add grounded ones without a number, never a made-up volume.
  const extra = picked.length < want ? pickDiverse(open, want - picked.length, picked).sort((a, b) => a.i - b.i) : [];
  const entry = (r, measuredRow) => ({
    text: r.c.text,
    reason: measuredRow ? volumeReason(r.info.volume, r.c.reason) : r.c.reason,
    kind: r.kind,
    monthlySearches: measuredRow ? r.info.volume : null,
    competition: measuredRow ? r.info.competition : null
  });
  return picked.map(r => entry(r, true)).concat(extra.map(r => entry(r, false)));
}

function themesFromKeywords(keywords) {
  const out = [], seen = new Set();
  (Array.isArray(keywords) ? keywords : []).forEach(k => {
    const t = validTheme(typeof k === 'string' ? k : k && k.text);
    if (!t || out.length >= MAX_THEMES) return;
    const key = keyOf(t); if (seen.has(key)) return;
    seen.add(key); out.push(t);
  });
  return out;
}

module.exports = { keywordCandidates, rankKeywords, themesFromKeywords, plannerSeeds, validTheme, KINDS };
