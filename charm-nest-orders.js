/*  charm-nest-orders.js — reading an order line, naming a set, holding an order.
 *  ═══════════════════════════════════════════════════════════════════════
 *  Pure, deterministic, shared by the sorter page and the node tests. Nothing
 *  in here asks a model anything: material comes from the Design Station's
 *  classifier, form / size / chain from the listing's options through the
 *  option map, the SKU from the transaction (or a learned alias). Free text
 *  (personalisation, buyer message, staff note, messages) is only carried
 *  along for the engraving classifier (design §5.2, §7.1).
 *  ═══════════════════════════════════════════════════════════════════════ */
(function (root, factory) {
  if (typeof module === "object" && module.exports) module.exports = factory();
  else root.CharmNestOrders = factory();
})(typeof self !== "undefined" ? self : this, function () {
  "use strict";

  /* ═══ 1 · material: the station's metal key → the sorter's card ═══════ */
  const METAL_TO_CARD = { gold: "gold", silver: "silver", rose: "rose", "10k": "gold10k", "14k": "gold14k" };
  const CARD_TAG = { gold: "GF", silver: "SS", rose: "RG", gold10k: "10K", gold14k: "14K" };
  const CARD_LABEL = { gold: "GF 14/20", silver: "SS", rose: "RG 14/20", gold10k: "10K Gold", gold14k: "14K Gold" };

  /* ═══ 2 · option maps ══════════════════════════════════════════════════
     Charm_Option_Map/{listingId or "*"} = { optionName: { value: { field, value } } }. Names and values are matched
     after normalisation (lower-case, trimmed, single spaces). The built-in "*" map below covers the plain literal
     values the shop's listings use; anything else is a "Needs mapping" review item until a person maps it once. */
  const norm = s => String(s == null ? "" : s).replace(/&quot;/g, "\"").toLowerCase().replace(/\s+/g, " ").trim();
  // Display-only order details. Never alter fulfilment mapping or infer a
  // purchased variant from a design SKU (the same charm can be sold many ways).
  const optionText = value => String(value ?? "").replace(/&(?:quot|amp|apos|lt|gt|#39|#34);/g, entity => ({"&quot;":'"',"&amp;":"&","&apos;":"'","&#39;":"'","&#34;":'"',"&lt;":"<","&gt;":">"}[entity])).trim();
  function purchaseOptions(line = {}, spec = {}) {
    line=line || {};spec=spec || {};
    const raw = Array.isArray(line.variations) && line.variations.length ? line.variations : (spec.options || []);
    const seen = new Set();
    return raw.map(v => ({name:optionText(v.name ?? v.formatted_name),value:optionText(v.value ?? v.formatted_value)})).filter(v => {
      const key=JSON.stringify([v.name,v.value]);
      if(!v.value || /personali[sz]ation|engraving text|custom text/i.test(v.name) || seen.has(key))return false;
      seen.add(key);return true;
    });
  }
  function purchaseDetails(line = {}, spec = {}) {
    line=line || {};spec=spec || {};
    const options=purchaseOptions(line,spec);
    const families=text=>{
      const s=optionText(text).toLowerCase(),found=[];
      if(/\bnecklace\b|\bwith chain\b/.test(s))found.push('necklace');
      if(/\bhuggies?\b/.test(s))found.push('huggie');
      else if(/\bhoops?\b/.test(s))found.push('hoop');
      if(/\bstuds?\b/.test(s))found.push('stud');
      if(/\bbracelets?\b/.test(s))found.push('bracelet');
      if(/\banklets?\b/.test(s))found.push('anklet');
      if(/\bkey\s*(chains?|rings?)\b/.test(s))found.push('keychain');
      return found;
    };
    const titleFamilies=families(line.title), titleFamily=titleFamilies.length===1?titleFamilies[0]:null;
    const label={necklace:'Necklace',stud:'Stud earrings',hoop:'Hoop earrings',huggie:'Huggie hoop earrings',bracelet:'Bracelet',anklet:'Anklet',keychain:'Keychain',earrings:'Earrings','earring-single':'Single earring'};
    const charmLabel=(family,text='')=>family==='necklace'?'Charm only · Necklace':family==='huggie'||family==='hoop'?'Charm only · '+(family==='huggie'?'Huggie hoop':'Hoop')+(/\bset\b|\bpair\b|\bcharms\b/i.test(text)?' charm set':' charm'):'Charm only';
    const selected=options.filter(v=>/type|style|product|item|option|select|choose|choice|chain|length/i.test(v.name) && !/metal|colou?r|finish|plating|text/i.test(v.name));
    const types=[];
    for(const v of selected){
      const text=v.value.toLowerCase(),fs=families(text),family=fs.length===1?fs[0]:null;
      const only=/\b(?:charms?|pendants?)\s*only\b|\bonly\s+(?:charms?|pendants?)\b|\b(?:no|without)\s+(?:a\s+)?(?:chain|hoops?|earrings?)\b|\bloose charms?\b/.test(text) || /^(?:charm|pendant)s?(?:\s*[+&]\s*engraving)?$/.test(text);
      if(only)types.push(charmLabel(family || titleFamily,text));
      else if(family)types.push(label[family]);
      else if(/\bearrings?\b/.test(text))types.push(label[['stud','hoop','huggie'].includes(titleFamily)?titleFamily:'earrings']);
    }
    const unique=[...new Set(types)];
    // Explicit charm-only choices override a listing's generic necklace title.
    const only=unique.filter(t=>t.startsWith('Charm only'));
    let type=only.length===1?only[0]:unique.length===1?unique[0]:null;
    if(!type && !unique.length){
      if(spec.form==='charm')type=charmLabel(titleFamily,line.title);
      else if(spec.form==='earrings' && ['stud','hoop','huggie'].includes(titleFamily))type=label[titleFamily];
      else type=label[spec.form] || label[titleFamily] || (/\bearrings?\b/i.test(line.title || '') && !titleFamilies.length?'Earrings':null);
    }
    return {type:type || 'Type not specified',options};
  }
  const FORM_VALUES = {
    "necklace": "necklace", "pendant necklace": "necklace", "charm necklace": "necklace", "necklace (with chain)": "necklace", "with chain": "necklace", "with necklace": "necklace", "necklace & charm": "necklace", "charm + chain": "necklace", "charm and chain": "necklace", "charm with chain": "necklace",
    "earrings": "earrings", "earring": "earrings", "pair of earrings": "earrings", "earrings (pair)": "earrings", "single earring": "earring-single",
    "charm only": "charm", "charm": "charm", "pendant only": "charm", "pendant": "charm", "charm only (no chain)": "charm", "no chain": "charm", "charm without chain": "charm", "loose charm": "charm",
    "huggie": "huggie", "huggie charm": "huggie", "huggie hoop": "huggie", "huggie hoops": "huggie", "huggie earrings": "huggie", "huggies": "huggie",
    "bracelet": "bracelet", "charm bracelet": "bracelet", "anklet": "anklet", "keychain": "keychain", "key chain": "keychain", "keyring": "keychain"
  };
  const SIZE_VALUES = { "xs": "XS", "extra small": "XS", "s": "S", "small": "S", "m": "M", "medium": "M", "l": "L", "large": "L", "xl": "XL", "extra large": "XL", "mini": "XS", "regular": "M", "standard": "M" };
  const DEFAULT_OPTION_MAP = { "*": {} };
  for (const nm of ["style", "type", "product", "option", "options", "select", "item", "product type", "jewelry type", "jewellery type", "choose", "choice", "charm type", "charm style", "jewelry style", "jewellery style", "necklace or charm", "charm or necklace"]) { DEFAULT_OPTION_MAP["*"][nm] = {}; for (const [v, f] of Object.entries(FORM_VALUES)) DEFAULT_OPTION_MAP["*"][nm][v] = { field: "form", value: f }; }
  for (const nm of ["size", "charm size", "pendant size"]) { DEFAULT_OPTION_MAP["*"][nm] = {}; for (const [v, s] of Object.entries(SIZE_VALUES)) DEFAULT_OPTION_MAP["*"][nm][v] = { field: "size", value: s }; }
  /* A shop writes the same choice in its own word order — "Necklace CHARM" for what this vocabulary calls "charm
     necklace" — and adds a word that is not part of the choice: "CHARM + Engraving" is a charm, engraved. A value is read
     as a form when its words, with those fillers removed, are exactly the words of one entry in FORM_VALUES. Anything
     else stays unmapped for a person to decide once: nothing here guesses what a buyer ordered. */
  const FORM_FILLER = new Set(["engraving", "engraved", "engrave", "personalised", "personalized", "with", "and", "plus", "only", "the", "a", "an", "&", "+", "-"]);
  const wordSet = v => new Set(String(v || "").toLowerCase().replace(/[^\w\s+&-]+/g, " ").split(/[\s+&-]+/).filter(w => w && !FORM_FILLER.has(w)));
  const sameWords = (a, b) => a.size === b.size && [...a].every(w => b.has(w));
  const FORM_WORDS = Object.entries(FORM_VALUES).map(([k, f]) => ({ words: wordSet(k), form: f }));
  /* An ear word that the shop or the buyer says is NOT part of what is sold ("Charm only (no hoops)", "pendant without earrings"): it says nothing about earrings, and an
     option that is a charm or a pendant with such a note is the loose charm (EARWORDS, 10 Oct: "Charm only (no hoops)" on an earring-titled listing was read as a pair). */
  const EAR_NEG_SRC = "\\b(?:no|without|minus|excluding)\\s+(?:the\\s+|a\\s+|any\\s+)?(?:ear[\\s-]?rings?|earings?|studs?|huggies|hugg(?:ie|y)(?:\\s+hoops?)?|hoops?|posts?)\\b";
  const EAR_NEG = new RegExp(EAR_NEG_SRC, "gi"), EAR_NEG_TEST = new RegExp(EAR_NEG_SRC, "i");
  const notEar = s => String(s == null ? "" : s).replace(EAR_NEG, " ");
  const formByWords = value => { const w = wordSet(notEar(value)); if (!w.size) return null; const hit = FORM_WORDS.filter(x => sameWords(w, x.words)); const forms = [...new Set(hit.map(x => x.form))]; return forms.length === 1 ? forms[0] : null; };
  const isFormOption = name => /^\s*(charm |pendant |jewel+ery |jewelry )?(type|style|form)\s*$/i.test(String(name || ""));
  const isEarFormOption = name => /^\s*(?:earrings?|hoops?|studs?|huggies|huggie)\s+(?:type|style|form)\s*$/i.test(String(name || ""));   // ("Earring Style: Huggie": a form option too; never a length)
  const isMetalOption = name => /metal|colou?r|finish|plating/i.test(String(name || ""));
  // what a buyer's text carries that no one can see or cut: the placeholder Etsy leaves where a picture or sticker was
  // pasted (U+FFFC, which browsers draw as a boxed "OBJ"), the replacement mark (U+FFFD), zero-width spaces and control codes
  const visible = s => String(s == null ? "" : s).replace(/[\uFFFC\uFFFD\u200B\u2060\uFEFF\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F]/g, "");
  const isPersonalisation = name => /personali[sz]ation|engraving text|custom text/i.test(String(name || ""));
  const isSizeOption = name => /(^|\s)size\s*$/i.test(String(name || ""));
  const isChainOption = name => /chain|necklace length|length|extender/i.test(String(name || ""));
  // 16" is how a shop writes a necklace length. The old rule ended in \b, which cannot follow a quote mark, so every
  // length written that way fell through as an unmapped option and held the line.
  const looksLikeLength = v => /^\s*\d+(\.\d+)?\s*("|''|\u201d|\u2033|in\b|inch|cm\b|mm\b)/i.test(String(v || "")) || /^\s*\d{1,2}(\.\d)?\s*$/.test(String(v || ""));
  const bareValue = v => norm(String(v || "").replace(/[^\w\s.+&-]+/g, " ")).split(/[\s+&]+/).filter(w => w && !FORM_FILLER.has(w)).join(" ");
  const looksLikeSize = v => /^(xs|s|m|l|xl|xxl|\d{1,2}(\.\d)?\s*(mm|cm)?( us)?)$/i.test(bareValue(v));
  // a made-to-order listing prices each order with an option of its own ("Price: 28"): it says what was paid, never what
  // is made, so it is read as nothing to map (it held every custom line under Options as well as under Material)
  const isPriceOption = name => /^\s*(price|amount|total|cost|payment|deposit|balance)\s*$/i.test(String(name || ""));
  const looksLikePrice = v => /^\s*(?:[$€£]|usd|cad|eur|gbp)?\s*\d{1,6}(?:[.,]\d{1,2})?\s*(?:[$€£]|usd|cad|eur|gbp)?\s*$/i.test(String(v || ""));

  /* ═══ 2b · font options ═══════════════════════════════════════════════
     (Paul, 10 Oct 2026, on 4175370240 "Fonts: 16"/ Typewriter" and 4172791262 "Font: Stylish", both "not mapped": "I thought you were supposed
     to check all of the drop down menu SKU links".) A font option has no SKU and never picks a charm or changes a cut: it says how the BACK is engraved.
     Nothing read it, so every font option fell through to the generic "not mapped" hold, and no question card exists any more to answer it (Paul removed
     them, 29 Sep): the line could only be finished by hand. What the app has to engrave with is ENGRAVING_FONTS below: ONE family, Source Sans 3 (the
     back files and every back record say so; the Regular or Semibold weight is picked by size, never by the buyer). A font option is read like this (fontRead):
       · the value is split into its parts: a necklace length (16"), an alignment (Center) and the font ("16"/ Typewriter" is 16" and Typewriter);
       · a font that names an app font EXACTLY, once case, spacing and punctuation are set aside, maps by itself ({ field: "font", value: id }, source rule:font);
       · any other font is never mapped to a look-alike. It does not hold the line either (FONT_RULES.unknownHolds = false): the line is mapped as the font the
         buyer ASKED for ({ field: "font", value: null }, spec.font.asked), the cut and the sheet go on, and the engraving review says "Requested font: Typewriter"
         beside the words, where the shop decides (the same place a font asked for in a note is shown). unknownHolds = true holds such a line instead, with ONE
         plain line naming the font and the app's fonts (a needsMapping problem with `font`);
       · a person's saved answer (for the whole value, or for the font alone: "typewriter") is always read first and never replaced.
     Only an option NAMED for a font is read: a length option that carries a font in its value ("Necklace Length /Font /Alignment": 18"·STYLISH·Center) is
     still a chain length, exactly as before (no order that already resolves reads differently). */
  const ENGRAVING_FONTS = [{ id: "source-sans-3", name: "Source Sans 3", names: ["Source Sans 3"] }];
  const FONT_RULES = { unknownHolds: false };
  const fontKey = s => String(s == null ? "" : s).normalize("NFKD").replace(/[\u0300-\u036f]/g, "").toLowerCase().replace(/[^\p{L}\p{N}]+/gu, "");
  const fontById = id => ENGRAVING_FONTS.find(f => f.id === String(id || "").trim().toLowerCase()) || null;
  const FONT_NAME = /\b(?:fonts?|typefaces?|lettering)\b/i, NOT_FONT_NAME = /\b(?:size|colou?r|metal|finish|plating|weight|length)\b/i;
  const isFontOption = name => FONT_NAME.test(String(name || "")) && !NOT_FONT_NAME.test(String(name || ""));
  const ALIGN_WORD = /^(?:(?:left|right|cent(?:er|re)(?:ed)?|middle)(?:\s+(?:align(?:ed|ment)?|justified))?|align(?:ed)?\s+(?:left|right|cent(?:er|re)))$/i;
  /** A font option's value in parts: { chain (a necklace length), align, words[] (what is left: the font) }. Parts are split at / \ · • | , ; and " - ". */
  function fontParts(value) {
    const out = { chain: "", align: "", words: [] };
    for (const t of String(value == null ? "" : value).replace(/&quot;/g, "\"").split(/\s*[\/\\·•|,;]\s*|\s+[-–—]\s+/).map(s => s.trim()).filter(Boolean)) {
      if (!out.chain && looksLikeLength(t)) out.chain = t;
      else if (!out.align && ALIGN_WORD.test(t)) out.align = t;
      else out.words.push(t);
    }
    return out;
  }
  const bigrams = k => { const s = new Map(); for (let i = 0; i < k.length - 1; i++) { const g = k.slice(i, i + 2); s.set(g, (s.get(g) || 0) + 1); } return s; };
  const likeness = (a, b) => { if (!a || !b) return 0; if (a === b) return 1; const x = bigrams(a), y = bigrams(b); let hit = 0, n = 0; for (const [g, c] of x) { hit += Math.min(c, y.get(g) || 0); n += c; } for (const c of y.values()) n += c; return n ? 2 * hit / n : 0; };
  /** The app's fonts, the most alike first (for the one plain line and the buttons of the question): [{ id, name }]. */
  const fontChoices = asked => { const k = fontKey(asked); return ENGRAVING_FONTS.map((f, i) => ({ f, i, s: Math.max(...f.names.map(n => likeness(k, fontKey(n)))) })).sort((a, b) => b.s - a.s || a.i - b.i).map(x => ({ id: x.f.id, name: x.f.name })); };
  /** What ONE option says about a font: null when its NAME is not a font's or its value holds no font word; else
   *  { asked (the font as the buyer's option writes it), chain, align, id, name (the app's font when `asked` names exactly one), pick: [{ id, name }] }. */
  function fontRead(name, value) {
    if (!isFontOption(name)) return null;
    const p = fontParts(value); if (!p.words.length) return null;
    const asked = p.words.join(" / "), key = fontKey(asked);
    const hits = key ? ENGRAVING_FONTS.filter(f => f.names.some(n => fontKey(n) === key)) : [], font = hits.length === 1 ? hits[0] : null;
    return { asked, chain: p.chain, align: p.align, id: font ? font.id : "", name: font ? font.name : "", pick: fontChoices(asked) };
  }
  const fontWhy = fr => `font “${clip(fr.asked, 40)}” is not one of the engraving fonts (${fr.pick.length === 1 ? "the app engraves in " : "the app has "}${fr.pick.map(f => f.name).join(", ")}): a person picks`;

  /** { field, value, source } for one option, or null when nothing deterministic applies. */
  function optionLookup(maps, listingId, name, value) {
    const n = norm(name), v = norm(value);
    const tryMap = (m, src) => { if (!m) return null; const byName = m[n] || m[Object.keys(m).find(k => norm(k) === n)]; if (!byName) return null; const hit = byName[v] || byName[Object.keys(byName).find(k => norm(k) === v)]; return hit ? Object.assign({ source: src }, hit) : null; };
    return tryMap(maps && maps[String(listingId)], "listing") || tryMap(maps && maps["*"], "shop") || tryMap(DEFAULT_OPTION_MAP["*"], "default");
  }

  /* ═══ 3 · SKUs that are not charms ═════════════════════════════════════ */
  /** noDesign = { patterns: [regex source strings], skus: [strings] } — Charm_Sku_NoDesign flattened. */
  function isNoDesign(sku, noDesign) {
    const s = String(sku || "").trim().toUpperCase(); if (!s) return false;
    if ((noDesign && noDesign.skus || []).some(x => String(x).trim().toUpperCase() === s)) return true;
    for (const p of (noDesign && noDesign.patterns) || []) { try { if (new RegExp(p, "i").test(s)) return true; } catch (_) { /* a bad pattern never matches */ } }
    return false;
  }
  /** The transaction's SKU, or the alias learned for its listing. The alias also stands in for a SKU of the line's own
   *  that no master file holds (when the master can be asked): "Use this charm" on an unknown SKU saved an alias that the
   *  unknown SKU then always beat, so the decision said "remembered" and the line stayed unmatched. */
  /* Etsy puts on each transaction the SKU of the variation bought: a listing whose variations each have a SKU gives each
     line its variation's own, one whose variations have none gives every line the listing's (Paul, 27 Sep: nobody keeps a
     list of which listings are which, so each line is read as it comes).
     · A variation SKU that is a catalogue SKU with the shop's charm-only mark ("MAPLE_8065-CO": MAPLE_8065 bought without a
       chain) is that catalogue design.
     · "Use this charm" is remembered for the listing and the SKU the line came with (bySku): on a listing whose variations
       each have a SKU, one variation's answer used to stand in for every other SKU of the listing the master did not hold.
       One saved for a line with no SKU is the listing's own (sku); one saved before 27 Sep (no v) stands in as it did.
     · A listing whose variations share one SKU while an option picks the charm (Zodiac Sign: Pisces) is answered under
       Options, once per listing and value (optionDesign). */
  /* Charm-only listings (Paul, 27 Sep; about 1,000 of them): one SKU for all three Charm Type choices. "Necklace CHARM" and
     "CHARM + Engraving" are the necklace charm, the listing's SKU; "Huggie CHARM SET" is the small charms that hang from a
     pair of huggie hoops, drawn in the master under the same SKU with " (HUGGIE)" after it ("BUNNY_42980 (HUGGIE)"). A
     huggie set is never cut as the necklace charm: without its huggie design it is asked about under that name.
     · "X-CO" (the charm-only mark) bought as a huggie set is "X (HUGGIE)": "X-CO (HUGGIE)" is in no master file.
     · A huggie set with no SKU has its own "Use this charm" answer (the listing's huggie), never the listing's necklace, and
       its own "Nothing to cut": its title is read as "<title> (HUGGIE)", so a rule saved for it never reaches the necklace
       lines (one saved for the necklace's title still covers it, as before). */
  const HUGGIE_SET = /\bhuggie\s+charms?\s+set\b/i;
  const huggieSet = line => (line.variations || []).some(v => HUGGIE_SET.test(String(v.value != null ? v.value : v.formatted_value || "")));
  // (a real SKU that ends in "-CO", one the master already loaded holds itself or as a huggie, keeps its "-CO")
  const huggieSku = (raw, masterEntry) => { const s = String(raw || "").trim().toUpperCase().replace(/\s+/g, " "); if (!s || /\(HUGGIE\)$/.test(s)) return s;
    const base = variationBase(s), real = !!base && !!masterEntry && !!(masterEntry(s) || masterEntry(s + " (HUGGIE)")); return (base && !real ? base : s) + " (HUGGIE)"; };
  const variationBase = raw => { const m = /^(.+?)[\s_-]+CO$/i.exec(String(raw || "").trim()); return m ? m[1].trim().toUpperCase() : ""; };
  /* The SKU Etsy keeps for the variation bought comes before anything else (Paul, 9 Oct: a listing whose drop-down options
     carry SKUs, "the choice the user made … will directly correlate to the charm that has to be located from the repository").
     Etsy puts that SKU on the receipt transaction; the listing's inventory (one table per listing, cached by the cloud:
     charmNestLibrary listingSkus) gives the same for a transaction that came without it or with only the listing's own. Then:
     · the master is asked for the transaction's SKU as Etsy wrote it, then as the master spells it (spacing and punctuation
       only: "SPORTS 12- BULLSEYE" is the master's "SPORTS 12 - BULLSEYE", when exactly one master SKU reads that way);
     · a SKU that is a design's own never raises the question "what does this option decide?" for the option it is tied to,
       and it outranks a charm a person picked for that option: tied means the inventory never gives that SKU to another
       value of the option (a SKU shared by every sign says nothing about which sign was bought, so the option is still asked);
     · a SKU no master file holds waits for a person, as before, and never becomes a sibling's design. */
  const upSku = s => String(s == null ? "" : s).trim().toUpperCase();
  /** A SKU without its spacing and punctuation: the key under which two spellings of one SKU meet. */
  const looseKey = s => upSku(s).replace(/[^A-Z0-9]+/g, "");
  /* KNOWN_SKU_TYPOS: the ONE definition of a SKU Etsy carries misspelled that Paul named as an exception to "never a look-alike" (10 Oct 2026:
     "On Etsy, fix the SKU nitial_Disc_4571 (the I is missing on all 180 products of that listing) … adapt the system to understand all 180 as they
     currently are"). Etsy listing 1008014571 (the initial disc necklace) keeps "nitial_Disc_4571" on every one of its products; the master design is
     INITIAL_DISC_4571. An entry is an exact SKU as Etsy writes it → the master SKU it stands for, compared as every SKU is (looseKey: case, spacing and
     punctuation set aside, nothing fuzzier): never a rule that guesses a missing letter, so any other misspelling (nitial_Disc_9999, NITIAL_8391) still
     waits for a person. It stands only when the master really holds the target, only for a SKU that is exactly the typo (not "NITIAL_DISC_4571-CO"),
     and after a person's saved answer: a SKU alias, an option map or a listing's alias for it still wins. Nothing stored is rewritten: the lines read it
     again, and the Etsy listing keeps its spelling until Paul fixes it there. Every place that turns a SKU into a master design reads THIS table:
     masterSku (resolveSku, the listing inventory's SKU, a pair's members), the bridge's re-read signature, the order search, the Design Station's
     live thumbnails (netlify/functions/_stationLive.js requires this module). */
  const KNOWN_SKU_TYPOS = Object.freeze({ "NITIAL_DISC_4571": "INITIAL_DISC_4571" });
  const TYPO_IX = new Map(Object.keys(KNOWN_SKU_TYPOS).map(bad => [looseKey(bad), upSku(KNOWN_SKU_TYPOS[bad])]));
  /** The correctly spelled SKU a SKU stands for when it is exactly a known typo, else "" (the master is not asked). */
  const knownTypo = sku => TYPO_IX.get(looseKey(sku)) || "";
  /** The master's SKU a SKU is, by its own spelling ("" when the master holds none): itself, or the one master SKU that is the same SKU
   *  spelled with other spacing or punctuation (masterLoose: the loaded master's index by looseKey, "" when two read alike). */
  function spelling(sku, masterEntry, masterLoose) {
    const s = upSku(sku); if (!s || !masterEntry) return "";
    if (masterEntry(s)) return s;
    const w = masterLoose ? upSku(masterLoose(s)) : "";
    return w && w !== s && masterEntry(w) ? w : "";
  }
  /** The master's SKU for a SKU that is exactly a known typo ("" when it is not, or the master does not hold the right one). */
  const typoSku = (sku, masterEntry, masterLoose) => { const t = knownTypo(sku); return t ? spelling(t, masterEntry, masterLoose) : ""; };
  /** The master's SKU a SKU stands for ("" when the master holds none): its own spelling, else the right spelling of a known typo. */
  const masterSku = (sku, masterEntry, masterLoose) => spelling(sku, masterEntry, masterLoose) || typoSku(sku, masterEntry, masterLoose);
  const idOf = x => (x == null || x === "" ? "" : String(x));
  const pairKey = p => idOf(p[0]) + ":" + idOf(p[1]);
  // the (property, value) ids Etsy puts on each variation of a transaction; a question with no value id (Personalization) has none
  const varIds = line => (line.variations || []).map(v => [idOf(v.propertyId != null ? v.propertyId : v.property_id), idOf(v.valueId != null ? v.valueId : v.value_id)]).filter(p => p[0] && p[1] && p[1] !== "0");
  /** The SKU a listing's inventory table gives the product bought: { sku, by } or null. table = { uni } (every product of the
   *  listing has the one SKU) or { products: [{ id, sku, d, pv: [[propertyId, valueId]…] }] }. by: "product" (the transaction's
   *  product id), "options" (the one product with those option values), "listing" (the listing's only SKU). */
  function inventorySku(line, table) {
    if (!line || !table || typeof table !== "object") return null;
    if (!Array.isArray(table.products)) return table.uni ? { sku: upSku(table.uni), by: "listing" } : null;
    const ps = table.products.filter(p => p && typeof p === "object"), pid = idOf(line.productId != null ? line.productId : line.product_id);
    if (pid) { const p = ps.find(x => idOf(x.id) === pid); if (p) return upSku(p.sku) ? { sku: upSku(p.sku), by: "product" } : null; }
    const want = varIds(line).map(pairKey).sort().join("|"); if (!want) return null;
    const hits = ps.filter(p => !p.d && (p.pv || []).map(pairKey).sort().join("|") === want), skus = [...new Set(hits.map(p => upSku(p.sku)))];
    return hits.length && skus.length === 1 && skus[0] ? { sku: skus[0], by: "options" } : null;
  }
  /** Is this SKU the listing's own: held by several products that differ in their options? */
  const listingWide = (table, sku) => !!table && Array.isArray(table.products) && table.products.filter(p => p && !p.d && upSku(p.sku) === sku).length > 1;
  /** Does the inventory tie this SKU to this value of its option: is it never given to a product with another value of the
   *  option? (v: the transaction's variation, with the ids Etsy gave it.) */
  function tiesToOption(table, v, sku) {
    if (!table || !Array.isArray(table.products) || !v || !sku) return false;
    const p = idOf(v.propertyId != null ? v.propertyId : v.property_id), val = idOf(v.valueId != null ? v.valueId : v.value_id);
    if (!p || !val || val === "0") return false;
    const has = x => (x.pv || []).some(a => idOf(a[0]) === p && idOf(a[1]) === val);
    const live = table.products.filter(x => x && !x.d), mine = live.filter(x => upSku(x.sku) === sku);
    return mine.length > 0 && mine.every(has) && live.some(x => !has(x));
  }
  /* ── family: what a SKU and a line say they are (necklace, earrings…) ─────────────────────────────────────────────────
     Paul, 10 Oct: each drop-down option's own SKU decides the charm. Etsy's SKU for the product bought is exact evidence only
     when it is also the right KIND of design for the line: the Greek Goddess EARRINGS listing carries "GreekGoddess_Necklace_4"
     for Vesta (a necklace pendant with a loop, 4 pieces), the master's earring is GREEKGODDESS_EARRING_4. A SKU of one family
     on a line of another is never cut as it stands and never swapped for its twin by the sorter: a person is asked, with the
     twin offered first (inventoryPicks). Only the words NECKLACE / EARRING / STUD / HUGGIE / HOOP / BRACELET / ANKLET (as whole
     words of the SKU) name a family; a SKU with none, or with two, says nothing. */
  const SKU_FAMILIES = [["necklace", /(^|[^A-Z])(NECKLACES?|CHOCKERS?|CHOKERS?)(?=[^A-Z]|$)/], ["earring", /(^|[^A-Z])(EARRINGS?|STUDS?|HUGGIES?|HOOPS?)(?=[^A-Z]|$)/], ["bracelet", /(^|[^A-Z])(BRACELETS?)(?=[^A-Z]|$)/], ["anklet", /(^|[^A-Z])(ANKLETS?)(?=[^A-Z]|$)/]];
  const FAMILY_TOKENS = { necklace: ["NECKLACE", "NECKLACES"], earring: ["EARRING", "EARRINGS", "STUD", "STUDS"], bracelet: ["BRACELET"], anklet: ["ANKLET"] };
  const FAMILY_WORDS = [["necklace", /\bnecklaces?\b|\bchokers?\b/i], ["earring", /\bearrings?\b|\bstuds?\b|\bhoops?\b|\bhuggies?\b/i], ["bracelet", /\bbracelets?\b/i], ["anklet", /\banklets?\b/i]];
  /** "necklace" | "earring" | "bracelet" | "anklet" | "" for a SKU. */
  function skuFamily(sku) {
    const s = upSku(sku), f = SKU_FAMILIES.filter(x => x[1].test(s)).map(x => x[0]);
    return f.length === 1 ? f[0] : "";
  }
  /** The family a line is: what the buyer chose in an option ("Necklace", "Stud earrings"), else the one its title names; "" when it
   *  is a charm sold alone, or says two families, or none. */
  function lineFamily(line) {
    line = line || {};
    const vals = purchaseOptions(line, {}).filter(v => !isMetalOption(v.name)).map(v => v.value);
    if (vals.some(v => /\bcharms?\s*(?:only|set)\b|\b(?:pendant|charm)s?\s+only\b|\bno\s+chain\b|\bloose\s+charms?\b/i.test(v))) return "";
    const fams = text => [...new Set(FAMILY_WORDS.filter(x => x[1].test(text)).map(x => x[0]))];
    const chosen = [...new Set(vals.flatMap(fams))];
    if (chosen.length === 1) return chosen[0];
    if (chosen.length > 1) return "";
    const t = fams(String(line.title || ""));
    return t.length === 1 ? t[0] : "";
  }
  /** Is this SKU of another family than the line it is on (both known)? */
  const skuFamilyConflict = (sku, line) => { const a = skuFamily(sku), b = a ? lineFamily(line) : ""; return !!a && !!b && a !== b; };
  /** The same SKU written for another family ("GREEKGODDESS_NECKLACE_4" → "GREEKGODDESS_EARRING_4"), when the master holds it. */
  function familyTwins(sku, family, masterEntry, masterLoose) {
    const s = upSku(sku), from = skuFamily(s), toks = FAMILY_TOKENS[family];
    if (!from || !toks || from === family) return [];
    const m = SKU_FAMILIES.find(x => x[0] === from)[1].exec(s); if (!m) return [];
    const at = m.index + m[1].length, was = m[2], out = [];
    for (const t of toks) { const w = masterSku(s.slice(0, at) + t + s.slice(at + was.length), masterEntry, masterLoose); if (w && !out.includes(w)) out.push(w); }
    return out;
  }
  /** Etsy's SKUs for a whole listing all name another family than the line, and the master holds, for each of them, exactly one SKU that is
   *  the same name and number in the line's family (the Greek Goddess EARRINGS listing: GreekGoddess_Necklace_0..4, and the master's
   *  GREEKGODDESS_EARRING_0..4): the listing's SKUs were copied from its sibling listing, and each one's twin is its own SKU in the line's
   *  family. → the master SKU for `etsySku`, or "" when any one of the listing's SKUs has no single twin (nothing is then inferred). */
  function listingTwin(line, table, etsySku, masterEntry, masterLoose) {
    const fam = lineFamily(line); if (!fam || !table || !Array.isArray(table.products) || !masterEntry) return "";
    const skus = [...new Set(table.products.filter(p => p && !p.d && upSku(p.sku)).map(p => upSku(p.sku)))];
    if (skus.length < 2 || !skus.includes(upSku(etsySku))) return "";
    let mine = "";
    for (const k of skus) {
      const f = skuFamily(k); if (!f || f === fam) return "";
      const t = familyTwins(k, fam, masterEntry, masterLoose); if (t.length !== 1) return "";
      if (k === upSku(etsySku)) mine = t[0];
    }
    return mine;
  }
  /** What the listing inventory offers for the product bought, as master SKUs a person can pick, best first: Etsy's own SKU for the
   *  option when it is the right family; else its twin in the line's family, then Etsy's own (flagged). → [{ sku, why, conflict? }] */
  function inventoryPicks(line, table, masterEntry, masterLoose) {
    const inv = inventorySku(line, table); if (!inv || !inv.sku || !masterEntry || inv.by === "listing") return [];   // (one SKU for the whole listing says nothing about an option)
    const out = [], add = (sku, why, more) => { if (sku && !out.some(x => x.sku === sku)) out.push(Object.assign({ sku, why }, more || {})); };
    const own = masterSku(inv.sku, masterEntry, masterLoose), fam = lineFamily(line), conflict = !!own && skuFamilyConflict(own, line);
    if (own && !conflict) add(own, "Etsy's own SKU for this option");
    else for (const t of familyTwins(inv.sku, fam, masterEntry, masterLoose)) add(t, `the ${fam} version of Etsy's SKU ${upSku(inv.sku)} (same number)`);
    if (own && conflict) add(own, `Etsy's own SKU for this option, but it is a ${skuFamily(own)} design and this line is ${fam === "earring" ? "earrings" : fam}`, { conflict: true });
    return out;
  }
  /** One plain line on why the listing inventory did not settle an option (nothing when it did). table: what the page holds for the
   *  listing (undefined: not read yet); known: the page has a table store at all (an older page has none: nothing is said). */
  function inventoryWhy(line, table, known, masterEntry, masterLoose) {
    if (!known || !line || !line.listingId) return "";
    const hasIds = !!(line.productId || line.product_id || varIds(line).length); if (!hasIds) return "";
    if (!table) return `Etsy's SKU list for listing ${line.listingId} is not loaded yet`;
    if (table.gone) return "Etsy no longer has this listing";
    if (!Array.isArray(table.products)) return table.uni ? `Etsy keeps one SKU (${upSku(table.uni)}) for every choice of this listing: map this option once` : "Etsy keeps no SKU on this listing's options: map this option once";
    const inv = inventorySku(line, table);
    if (!inv) {   // no SKU to read: is there no product, or one with a blank SKU?
      const ps = table.products.filter(p => p && typeof p === "object"), pid = idOf(line.productId != null ? line.productId : line.product_id), want = varIds(line).map(pairKey).sort().join("|");
      const prod = (pid && ps.find(x => idOf(x.id) === pid)) || (want && ps.find(p => !p.d && (p.pv || []).map(pairKey).sort().join("|") === want));
      return prod && !upSku(prod.sku) ? "Etsy has no SKU on this option" : "Etsy's list has no single product for these option values";
    }
    const own = masterEntry ? masterSku(inv.sku, masterEntry, masterLoose) : "";
    if (!own) return `Etsy's SKU for this option, ${upSku(inv.sku)}, is not in any master file`;
    if (skuFamilyConflict(own, line)) { const f = lineFamily(line); return `Etsy's SKU for this option, ${own}, is a ${skuFamily(own)} design and this line is ${f === "earring" ? "earrings" : f}`; }
    return `Etsy's SKU ${own} is shared by several choices of this option`;
  }
  /** The charms a person is offered for an option that picks the charm, best first (Review › Options › "The charm itself…"):
   *  0 · what Etsy's inventory gives the product bought, in the line's family (Etsy's own SKU, or its twin when Etsy's is another family's)
   *  1 · a master SKU named exactly by the value ("Libra" → LIBRA)  2 · one whose words hold all the value's words  3 · Etsy's own SKU
   *  when it is another family's design  4 · one that holds the value's lead word ("Algiz" of "Algiz - Divine Plan").
   *  Inside a rank: the line's family or none before another family, a "(HUGGIE)" twin after the plain design unless the line is a
   *  huggie, then the shorter name. Every master SKU is read (the list used to stop at the thirteenth hit and show three).
   *  skus: the master's SKUs (any iterable). → [{ sku, why }] (at most `max`). */
  function suggestCharms(o) {
    o = o || {}; const max = o.max || 8, fam = lineFamily(o.line), huggieLine = /\bhuggies?\b/i.test(String(o.line && o.line.title || "") + " " + purchaseOptions(o.line || {}, {}).map(v => v.value).join(" "));
    const wordsOf = t => upSku(t).split(/[^A-Z0-9]+/).filter(Boolean), keep = w => w.length >= 3 && !/^\d+$/.test(w);
    const value = optionText(o.value), vw = wordsOf(norm(value)).filter(keep), lead = wordsOf(norm(value).split(/\s+[-–—:]\s+|\s*[:(]\s*/)[0]).filter(keep)[0] || "";
    const found = new Map(), put = (sku, rank, why) => { const k = upSku(sku); if (!k) return; const was = found.get(k); if (!was || rank < was.rank) found.set(k, { sku: k, rank, why }); };
    for (const x of o.picks || []) put(x.sku, x.conflict ? 3 : 0, x.why);
    if (vw.length) for (const k of o.skus || []) {
      const w = wordsOf(k), plain = w.filter(x => x !== "HUGGIE"), set = new Set(w);
      if (plain.length === vw.length && vw.every(x => plain.includes(x))) put(k, 1, "named exactly like the option");
      else if (vw.every(x => set.has(x))) put(k, 2, "its name holds the option's words");
      else if (lead && lead.length >= 4 && set.has(lead) && vw.length > 1) put(k, 4, `its name holds “${lead.toLowerCase()}”`);
    }
    const bad = k => (skuFamilyConflict(k, o.line) ? 2 : 0) + (/\(HUGGIE\)\s*$/i.test(k) && !huggieLine ? 1 : 0);
    return [...found.values()].sort((a, b) => a.rank - b.rank || (a.rank ? bad(a.sku) - bad(b.sku) : 0) || a.sku.length - b.sku.length || a.sku.localeCompare(b.sku)).slice(0, max).map(x => ({ sku: x.sku, why: x.why }));
  }
  function resolveSku(line, aliases, masterEntry, noDesign, masterLoose) {
    const raw = String(line.sku || "").trim().toUpperCase();
    const a = aliases && aliases[String(line.listingId)], up = s => s ? String(s).trim().toUpperCase() : "";
    // (the listing's answer is its necklace charm's: never a huggie set's, which has its own for a line with no SKU)
    const own = raw && a && a.bySku ? up(a.bySku[raw]) : "", whole = !a ? "" : huggieSet(line) ? (!raw ? up(a.huggie) : "") : a.sku && (!raw || !a.v) && !/\(HUGGIE\)$/.test(raw) ? up(a.sku) : "";
    // a blocked SKU's card offers "Use this charm" too: the answer for that SKU is its design (the blocked entry beat it)
    const held = raw && masterEntry ? masterEntry(raw) : null;
    if (raw && (!masterEntry || (held && !(own && held.blocked)) || isNoDesign(raw, noDesign))) return { sku: raw, source: "transaction" };
    if (own) return { sku: own, source: "alias" };
    // the master's own spelling of the SKU (a person's answer for the SKU as written came first), then the charm-only mark
    const spelt = raw && spelling(raw, masterEntry, masterLoose);
    if (spelt) return { sku: spelt, source: "spelling" };
    const base = raw && variationBase(raw), under = base && spelling(base, masterEntry, masterLoose);
    if (under) return { sku: under, source: "variation" };
    if (whole) return { sku: whole, source: "alias" };
    // a SKU that is exactly a known typo (KNOWN_SKU_TYPOS) is its master design: after every answer a person saved for it, before "unknown SKU"
    const typo = raw && typoSku(raw, masterEntry, masterLoose);
    if (typo) return { sku: typo, source: "typo" };
    return raw ? { sku: raw, source: "transaction" } : { sku: "", source: null };
  }
  /** The charm an option picks on this listing ({ sku, name, value, variation }), when a person said so under Options, or null. */
  function optionDesign(line, maps) {
    for (const v of line.variations || []) {
      const name = v.name || v.formatted_name, value = String(v.value != null ? v.value : v.formatted_value || "").replace(/&quot;/g, "\"").trim();
      if (!name || !value) continue;
      const hit = optionLookup(maps, line.listingId, name, value);
      if (hit && hit.field === "design" && hit.value) return { sku: String(hit.value).trim().toUpperCase(), name, value, variation: v };
    }
    return null;
  }

  /* ═══ 3b · special orders: not a regular listing purchase ══════════════
     Custom charms and custom pieces, rework, chain-only orders, add-ons and other one-off purchases are made or handled
     by hand, not picked from the catalogue: each is one card under Review → Custom Orders. Read from what the shop itself
     wrote (the SKU it gave the listing, the listing title, a "Price" option, a buyer's "Chain only" choice), deterministic,
     no model. A title alone never makes a line special when its SKU has a design in a master file: shop titles say
     "custom" for personalised catalogue charms. Only chain only is never cut; everything else waits for a person. */
  const SPECIAL = {
    customCharm:    { label: "Custom charm", group: "custom" },
    customNecklace: { label: "Custom necklace", group: "custom" },
    customHuggies:  { label: "Custom huggies", group: "custom" },
    customEarrings: { label: "Custom earrings", group: "custom" },
    customBracelet: { label: "Custom bracelet", group: "custom" },
    customOther:    { label: "Custom order", group: "custom" },
    rework:         { label: "Rework", group: "rework" },
    chainOnly:      { label: "Chain only", group: "chain", notCut: true },
    addOn:          { label: "Add-on", group: "addon" },
    special:        { label: "Special order", group: "other" }
  };
  const SEP_ = "(?:[\\s_\\-.]|\\d|$)";
  const SKU_RULES = [
    // CUSTOM_6673, CUSTOM-N-001-665441, CUSTOM-H-020-660181, CSTM-…, CUST_…
    ["custom", new RegExp("^(?:CUSTOM|CSTM|CUST)" + SEP_), false],
    // RE_5460, RE-…, RW_…, REWORK, REPAIR, MOD_…: only without a design of their own (a catalogue SKU is its design)
    ["rework", /^R[EW][\s_\-.]?\d/, true],
    ["rework", new RegExp("^(?:RE|RW)[\\s_\\-.]|^(?:REWORK|RE-WORK|REPAIR|MODIFICATION|MODIFY|MOD|FIX|REMAKE)" + SEP_), true],
    // CHAIN_8941, CHAIN-17IN, CHAIN_ONLY, EXT-2IN: a chain product's SKU and nothing else, because chain only is never cut
    // (CHAIN-HEART, a charm whose design is not indexed yet, stays an Unknown SKU question)
    ["chainOnly", /^(?:CHAIN|CHN|EXTENDER|EXT)(?:[\s_\-.]*(?:\d+(?:\.\d+)?(?:IN|INCH|INCHES|CM|MM)?|ONLY|REPL|REPLACE|REPLACEMENT|GF|SS|RG|YG|WG|14K|10K|GOLD|SILVER|ROSE|STERLING))*$/, true],
    ["addOn", new RegExp("^(?:ADD|ADDON|ADD-ON|EXTRA|UPGRADE|UPG|GIFT|GIFTBOX|GIFTWRAP|GIFTBAG|BOX|POUCH|BAG|WRAP|RUSH|EXPRESS|PRIORITY|INSURANCE|SHIP|SHIPPING|CARD)" + SEP_), true],
    ["special", new RegExp("^(?:SPECIAL|SPCL|PRIVATE|RESERVED|MTO|BESPOKE|DEPOSIT|PAYMENT|BALANCE|DIFF|DIFFERENCE|ORDER)" + SEP_), true]
  ];
  /* [kind, phrase, how]. 0: the phrase names the purchase itself wherever it stands in the title. 1: it must start the
     title (a "Chain Replacement" listing, not "Butterfly Charm Necklace, Chain Only Option"), with or without a SKU —
     chain only is never cut, so it is read only where it cannot be a charm's title. 2: the phrase is also how shop
     titles describe regular listings ("… Charm Necklace with Gift Box", "Custom Charm Necklace, Personalized…", "Made to
     Order"): it counts only for a line with no SKU at all, and only as the start of the title (a "Gift Box" or "Rush
     Order Fee" listing), so an unknown catalogue SKU stays an Unknown SKU question. */
  const TITLE_RULES = [
    ["chainOnly", /\bchain\s*only\b|\bonly\s+(?:the\s+)?chain\b|\bjust\s+(?:the\s+)?chain\b|\bchain\s+replacement\b|\breplacement\s+chain\b|\b(?:chain|necklace)\s+extender\b|\bextender\s+chain\b|\bchain\s+without\s+(?:a\s+)?(?:charm|pendant)\b/, 1],
    ["rework", /\bre-?work\b|\bmodification\b|\brepair\b|\bre-?engrav\w*|\balteration\b/, 0],
    ["rework", /\bmodify\b|\bre-?make\b|\bre-?siz(?:e|ing)\b/, 2],
    ["custom", /\bcustom(?:i[sz]ed)?\s+(?:order|request|listing|commission)\b|\bbespoke\b|\bcommission(?:ed)?\s+(?:piece|work|order|charm|design)\b/, 0],
    ["custom", /\bcustom(?:i[sz]ed)?\s+(?:design|piece|work|made|charm|jewel(?:le)?ry|necklace|earrings?|huggies?|hoops?|bracelet)\b|\bmade\s+to\s+order\b|\bone\s+of\s+a\s+kind\b/, 2],
    ["addOn", /\badditional\s+(?:item|charm|piece|pendant|letter|initial|birthstone|disc|name)s?\b|\bextra\s+(?:charm|item|piece|pendant|letter|initial|birthstone|disc)s?\b|\bgift\s*(?:wrap(?:ping)?|box|bag|pouch)\b|\brush\s+(?:order|processing|fee|service)\b|\b(?:express|expedited|priority)\s+(?:shipping|processing|delivery)\b|\bshipping\s+upgrade\b|\bupgrade\b/, 2],
    ["special", /\bspecial\s+order\b|\bprivate\s+listing\b|\breserved\s+(?:for|listing)\b|\bdeposit\b|\bbalance\s+(?:payment|due)\b|\bprice\s+difference\b|\bdifference\s+in\s+price\b|\bpayment\s+for\b/, 0]
  ];
  // the product name a title starts with: Etsy titles are lists of phrases, the first is what the listing is
  const titleLead = lower => lower.split(/\s*[,|•·:;–—(\[]\s*|\s+-\s+|\s+\/\s+/)[0].replace(/^\s*(?:add(?:\s+an?)?|optional|\+)\s+/, "").trim();
  // a buyer's own choice of no charm at all, on a charm listing
  const CHAIN_ONLY_VALUE = /^(?:(?:necklace\s+)?chain\s*only|(?:just|only)\s+(?:the\s+)?chain|chain\s+(?:without|no)\s+(?:a\s+)?(?:charm|pendant)s?|no\s+(?:charm|pendant)s?|chain\s*\(\s*no\s+(?:charm|pendant)s?\s*\))$/i;
  const CUSTOM_LETTER = { N: "customNecklace", H: "customHuggies", E: "customEarrings", S: "customEarrings", B: "customBracelet", A: "customBracelet", C: "customCharm", P: "customCharm" };
  function customKind(sku, title, options) {
    const m = /^(?:CUSTOM|CSTM|CUST)[\s_\-.]([A-Z])[\s_\-.]/.exec(sku || "");
    if (m && CUSTOM_LETTER[m[1]]) return CUSTOM_LETTER[m[1]];
    const t = (String(title || "") + " " + (options || []).map(o => o.value).join(" ")).toLowerCase();
    if (/\bhuggies?\b/.test(t)) return "customHuggies";
    if (/\bnecklaces?\b|\bpendant necklace\b/.test(t)) return "customNecklace";
    if (/\bearrings?\b|\bstuds?\b|\bhoops?\b/.test(t)) return "customEarrings";
    if (/\bbracelets?\b|\banklets?\b/.test(t)) return "customBracelet";
    if (/\bcharms?\b|\bpendants?\b/.test(t)) return "customCharm";
    return "customOther";
  }
  /* ADD-ON LISTINGS (Paul, 10 Oct 2026, order 4176744752: "This should be classified as a custom order because it's in addition of a
     charm by itself, and it has the classical blue Custom thumbnail"). The shop sells some charms as a listing of their own that is
     bought on top of a charm bought separately ("Huggie Charm + Shipping", Etsy photo "ADD A HUGGIE CHARM"; "Custom Charm + Shipping").
     What the buyer wants is told in their words, never by a master design: the SKU of such a listing is its name plus the last four digits
     of its listing id (Huggie_3722, Custom_6673), and the master file happens to hold designs named like that (HUGGIE_3722 is a ladybug),
     so the line used to be read as that design and asked about its "Price" option. These lines are custom orders. Three things say so,
     all of them plain words a person can read and change, none of them a guess about look-alikes:
       1 · the listing's id in a person's list: Firebase, Charm_Sku_Aliases/{listing id}.listingKind (charmNestLibrary listingKindPut), which
           also says "regular" to take a listing out again, and names another kind (rework, chain only…) for the same kind of listing;
       2 · the listing's id in ADDON_LISTINGS (what was found in the shop's own orders when this rule was made);
       3 · the title's first phrase, nothing but "Add a <up to three words> Charm" or "<up to three words> Charm + Shipping".
     The words "add on" in a title are still no rule (Paul, 25 Sep: many regular charm-only listings say it somewhere in a long title). */
  const ADDON_LISTINGS = { "1777293722": "Huggie Charm + Shipping" };
  const ADDON_TITLE = [
    ["Add a … Charm", /^add\s+an?\s+(?:[a-z0-9'’&-]+\s+){0,3}charms?$/],
    ["… Charm + Shipping", /^(?:[a-z0-9'’&-]+\s+){0,3}charms?\s*\+\s*shipping$/]
  ];
  const KIND_WORDS_ = { custom: "a custom order", rework: "a rework", addOnToOrder: "an add-on to an order", chainOnly: "chain only", other: "a special order" };
  /** What says this line's listing is an add-on listing: { kind, why } (kind: custom, rework…), { regular: true } when a person took the listing out, or null. */
  function addOnOf(line, listingKind) {
    line = line || {};
    const lid = String(line.listingId || "").replace(/\D/g, ""), lk = listingKind && typeof listingKind === "object" ? listingKind : null;
    if (lk && lk.kind) {
      if (lk.kind === "regular") return { regular: true };
      if (KIND_WORDS_[lk.kind]) return { kind: lk.kind, why: `listing ${lid}: ${lk.by || "a person"} said every order of it is ${KIND_WORDS_[lk.kind]}` };
    }
    if (lid && ADDON_LISTINGS[lid]) return { kind: "custom", why: `listing ${lid} “${ADDON_LISTINGS[lid]}” is an add-on listing: a charm made to the buyer's own request, not a master design` };
    const title = String(line.title || "").replace(/&#0*39;/g, "'").replace(/&amp;/g, "&").trim(), lead = title.toLowerCase().split(/\s*[,|•·:;–—(\[]\s*|\s+-\s+|\s+\/\s+/)[0].trim();
    for (const [name, re] of ADDON_TITLE) if (re.test(lead)) return { kind: "custom", why: `listing “${title.length > 48 ? title.slice(0, 47) + "…" : title}” is an add-on listing (“${name}”): a charm made to the buyer's own request, not a master design` };
    return null;
  }
  /* Claude's reading of a line (Charm Sorter › Custom Orders, charmNestLibrary customRead) and a person's decision name
     one of these kinds; each is one of the special kinds above, or none ("regular"). */
  const READ_KINDS = { custom: null, rework: "rework", addOnToOrder: "addOn", chainOnly: "chainOnly", other: "special", regular: null };
  const READ_LABEL = { addOnToOrder: "Add-on to an order" };
  /**
   * Is this line a special (non-catalogue) purchase? → null, or
   * { kind, label, group, notCut, why, signals[], read? }. opts: { sku (the resolved SKU), masterEntry?, optionMaps?,
   * read? (Claude's reading of the line), decided? (a person's decision), listingKind? (a person's word for the listing: aliases[listing].listingKind) }.
   * A SKU with a design in a master file is a catalogue charm unless its own SKU or the buyer says otherwise. A line with
   * no design is read by Claude (the words "add on" in a title never decide it: Paul, 25 Sep, many regular charm-only
   * listings say it); until it has been read, the shop's own SKU codes and the few unmistakable title phrases below stand.
   * A person's decision always wins.
   */
  function specialOf(line, opts) {
    line = line || {}; opts = opts || {};
    const raw = String(line.sku || "").trim().toUpperCase(), sku = String(opts.sku || raw).trim().toUpperCase();
    const known = !!(sku && opts.masterEntry && opts.masterEntry(sku));
    const title = String(line.title || ""), lower = title.toLowerCase();
    const vars = (line.variations || []).map(v => ({ name: String(v.name != null ? v.name : v.formatted_name || ""), value: String(v.value != null ? v.value : v.formatted_value || "").replace(/&quot;/g, "\"").trim() })).filter(v => v.name && v.value);
    const make = (kind, why, signal) => Object.assign({ kind, why, signals: [signal] }, SPECIAL[kind]);
    // a kind Claude or a person named, as the special kind it is (a custom piece takes its form from its SKU or title)
    const asKind = k => k === "custom" ? customKind(sku || raw, title, vars) : READ_KINDS[k] || null;
    // 0 · a person's decision stands
    const dec = opts.decided;
    if (dec && dec.kind && !known) {
      const k = asKind(dec.kind); if (!k) return null;
      return Object.assign(make(k, `decided by ${dec.by || "a person"}`, "person"), READ_LABEL[dec.kind] ? { label: READ_LABEL[dec.kind] } : {}, { decided: dec });
    }
    // 1 · the buyer chose no charm (a stored map for that value is a person's decision and stands)
    for (const v of vars) {
      if (!CHAIN_ONLY_VALUE.test(v.value.trim())) continue;
      const hit = optionLookup(opts.optionMaps, line.listingId, v.name, v.value);
      if (hit && hit.source !== "default") continue;
      return make("chainOnly", `option “${v.name}: ${v.value}”`, "option");
    }
    // 1b · the listing is an add-on listing (addOnOf): the charm is the buyer's own request, whatever master design its SKU happens to match.
    // A person's word for the listing and the listing's own wording stand over Claude's reading and over the SKU codes; `own` tells interpretLine the design is not a master's
    const addOn = addOnOf(line, opts.listingKind);
    if (addOn && !addOn.regular) {
      const k = addOn.kind === "custom" ? customKind(sku || raw, title, vars) : READ_KINDS[addOn.kind] || null;
      if (k) return Object.assign(make(k, addOn.why, "listing"), READ_LABEL[addOn.kind] ? { label: READ_LABEL[addOn.kind] } : {}, { own: true });
    }
    // 2 · Claude's reading of everything the shop holds about the line. Chain only is never cut, so an unsure reading of
    // it waits for a person as a special order instead.
    const rd = opts.read;
    if (rd && rd.kind && !known) {
      let k = asKind(rd.kind); if (!k) return null;
      if (k === "chainOnly" && !(rd.confidence >= 0.85)) k = "special";
      return Object.assign(make(k, rd.summary || "read by Claude", "ai"), READ_LABEL[rd.kind] ? { label: READ_LABEL[rd.kind] } : {}, { read: rd });
    }
    // 2 · the SKU the shop gave the listing
    for (const s of [...new Set([raw, sku].filter(Boolean))]) {
      for (const [kind, re, onlyLoose] of SKU_RULES) {
        if (!re.test(s) || (onlyLoose && known)) continue;
        return make(kind === "custom" ? customKind(s, title, vars) : kind, `SKU ${s}`, "sku");
      }
    }
    // 3 · the listing title, only for a line with no design of its own
    const lead = titleLead(lower), leads = re => { const m = re.exec(lead); return !!m && m.index === 0; };
    const noSku = !raw && !sku;
    if (!known) for (const [kind, re, how] of TITLE_RULES) {
      if (how === 0 ? !re.test(lower) : how === 1 ? !leads(re) : !(noSku && leads(re))) continue;
      return make(kind === "custom" ? customKind("", title, vars) : kind, `listing “${title.length > 48 ? title.slice(0, 47) + "…" : title}”`, "title");
    }
    // 4 · a price chosen as an option: a made-to-order or private listing
    const price = vars.find(v => isPriceOption(v.name) && looksLikePrice(v.value));
    if (price) return make("special", `“${price.name}” option`, "price");
    return null;
  }

  specialOf.addOn = { of: addOnOf, listings: ADDON_LISTINGS, titles: ADDON_TITLE, wait: "custom add-on listing: no master design, waits for the order's own designs (Send to Sheet) or Complete Order" };   // (for the tests and for a reader: what says a listing is an add-on listing)
  /* The Team's workflow stamps ("DESIGNED :)", "QA1", "QA2", "PE", "am") are on nearly every order: they say where the
     order is in the shop, never what to engrave. Counted as engraving evidence, they sent every line to the engraving
     reader and every charm of a sheet showed a back engraving (Paul, 25 Sep: "Backs 69" on a sheet of 69 charms). A Team
     message counts only when it says more than such a stamp. */
  const STAMP_WORDS = new Set(("designed design done qa pe am pm ok okay printed print packed pack shipped ship cut lasered laser checked " +
    "check ready sent fixed redo remade polished plated assembled labeled labelled sorted complete completed approved recut reprint " +
    "sanded tumbled finished final verified good fine yes thanks thank you").split(" "));
  function engravingNote(text) {
    const words = String(text || "").toLowerCase().replace(/[^\p{L}\p{N}\s]/gu, " ").split(/\s+/).filter(Boolean);
    if (!words.length) return false;
    return !(words.length <= 3 && words.every(w => STAMP_WORDS.has(w) || /^[a-z]{1,3}\d{0,2}$/.test(w) || /^\d{1,2}$/.test(w)));
  }

  /* ═══ 3c · pieces: how many cut pieces ONE unit of a line makes, which ear each is, and what a line says about it ═══════
     (Paul, 9 Oct 2026, 18:46: "Pairs always make two pieces whether they are matching or mismatched ... some necklaces have options
     for the amount of charms attached: each charm is an individual charm that must be on a sheet ... the system must check what
     drop-down options the user chose and whether there is a quantity pertaining to them, and consider those as individual charms
     that are part of just one necklace." And 18:47: each pair is a LEFT and a RIGHT earring, the Right the mirror of the Left.)
     THE rule, in one place (every caller reads pieceCountOf / spec.pieceCount, nothing else counts a line's pieces):
       pieces = Etsy quantity (units) x pieces per unit
       an earring PAIR line (stud, hoop, huggie hoops, "earrings", Huggie CHARM SET): 2 per unit, a Left then a Right, matching or not;
       a line whose chosen option or title says SINGLE: 1 per unit (side only if the line says left or right);
       an option that names how many discs / charms / tags a necklace carries ("2 Disc", "Number of Discs: 3"): that many per unit,
         each its own piece of the same group, never a multiple of the necklace;
       an option that may name a count but does not say what is counted (letters, initials, "Set of 3", a range) or that disagrees
         with the buyer's note: NOT guessed. The line waits for a person with a one-line question (a needsMapping problem with
         `count`), whose answer is kept for that listing and value like every other option answer ({ field: "count", value: "3" });
       a mismatched DESIGN (two bodies under one label) sold as EARRINGS is 2 per unit, L and R, each cut from its own body (the pool's per-body pieces, PAIRPOOL);
         sold as anything else (a necklace, pendant, bracelet, key ring, charm only: Paul, 10 Oct, "only earring pairs are Left and Right") it is no pair at all: 1 per unit,
         the whole file as the app always read it, no side, never mirrored (spec.pair.twoBodies says so);
         when the pool cannot tell its two bodies apart it is made as the one glued copy per unit the app always made (glue(), spec.pair.glued),
         and PIECE_RULES.mismatchedMakesTwo = false brings that back for every mismatched design at once.
     Old records: a line already pooled keeps the pieces it has (Orders pins spec.pieceCount to its pool ids and notes the shortfall in
     spec.pieceNote); the new count is for lines pooled from now on. */
  const PIECE_RULES = { pairFormsMakeTwo: true, optionCountsMake: true, mismatchedMakesTwo: true };
  const lvName = v => String(v && (v.name != null ? v.name : v.formatted_name) || "");
  const lvValue = v => String(v && (v.value != null ? v.value : v.formatted_value) || "").replace(/&quot;/g, "\"");
  const clip = (s, n = 60) => { s = String(s || "").replace(/\s+/g, " ").trim(); return s.length > n ? s.slice(0, n - 1) + "…" : s; };
  // what a listing, an option or a note says when the buyer wants two different charms (the shop's own staff fact stud-mismatch:
  // "order the earrings and leave a note naming the two designs"; the snapshot's "Mismatched Tennis Ball and Raquet Huggie Hoops",
  // "Silver • 2 symbols"). "left" or "right" alone never counts (a necklace font, "Left-facing wolf"): only an option NAMED for a side.
  const MIS_WORD = /\bmis-?match(?:ed)?\b/i;
  const TWO_DESIGNS = /\b(?:2|two)\s+(?:different\s+)?(?:symbols?|designs?|signs?)\b|\bdifferent\s+(?:designs?|charms?|symbols?)\b|\b(?:one|1)\s+of\s+each\b/i;
  const SIDE_NAME = /\b(?:left|right)\b/i, SIDE_THING = /\b(?:ear(?:ring)?s?|charm|design|stud|huggie|hoop|side)\b/i;
  const MISMATCH_NAME = /^MISMATCH(?:ED)?(?:[\s_.\-]|\d|$)/;
  const DISC_WORDS = { one: 1, two: 2, three: 3, four: 4, five: 5 };
  /** n for "3 discs" / "2 Disc" / "three discs" in the title or an option VALUE (never a note), else 0: information only (the count of
   *  pieces is countRead's, from the options). */
  function discsIn(line) {
    const re = /\b([1-9]|one|two|three|four|five)\s*-?\s*discs?\b/i, texts = [String(line && line.title || "")].concat(((line && line.variations) || []).filter(v => !isPersonalisation(lvName(v))).map(lvValue));
    for (const t of texts) { const m = re.exec(t); if (m) return +m[1] || DISC_WORDS[m[1].toLowerCase()] || 0; }
    return 0;
  }

  /* ── how many does an OPTION say (the drop-down a buyer chose) ───────────────────────────────────────────────────────
     Read from option NAMES and VALUES only; a note is never a count (it is only compared, see noteCountOf). What is counted decides:
       piece   discs, tags, charms, pendants, beads, pieces: separate cut pieces, so "2 Disc" is 2, settled (rules count:unit, count:name)
       text    letters, initials, names, words, digits: marks on a piece. One piece each (a disc per letter) or all on one piece (a bar)? ASK
       design  symbols, designs, signs: on an earring line the number of different designs (1 = the same on both ears, 2 = a mismatched
               pair: no change of count); on any other line, does each symbol make a piece? ASK
       length  characters: how long the engraving is (a price tier, "1-5 Character"), never a count of pieces: ignored
       none    no unit named ("Quantity: 3", "Set of 3 ", "How many?: 2"), or a range ("1-3 discs"), or two numbers in one value
               ("4 Silver / 2 Gold"): ASK
     A number inside a length, a size or a purity ("16 inches", "8.5mm", "14K") is not a count. */
  const NUMWORDS = { one: 1, two: 2, three: 3, four: 4, five: 5, six: 6, seven: 7, eight: 8, nine: 9, ten: 10, eleven: 11, twelve: 12 };
  const NUMRE = "(\\d{1,2}|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve)";
  const U_PIECE = "discs?|disks?|tags?|charms?|pendants?|beads?|pieces?|pcs?", U_TEXT = "letters?|initials?|names?|words?|monograms?|digits?", U_DESIGN = "symbols?|designs?|signs?|icons?", U_LENGTH = "characters?|chars?";
  const U_ALL = [U_PIECE, U_TEXT, U_DESIGN, U_LENGTH].join("|");
  const unitClass = w => { w = String(w || "").toLowerCase(); return new RegExp("^(?:" + U_PIECE + ")$").test(w) ? "piece" : new RegExp("^(?:" + U_TEXT + ")$").test(w) ? "text" : new RegExp("^(?:" + U_DESIGN + ")$").test(w) ? "design" : new RegExp("^(?:" + U_LENGTH + ")$").test(w) ? "length" : ""; };
  const numOf = s => { s = String(s || "").toLowerCase(); return /^\d+$/.test(s) ? +s : NUMWORDS[s] || 0; };
  // "2 Disc", "three discs", "2-disc", "2x discs", "x3 discs", "Discs: 3", "Discs x 3", "1-5 Character" (the range is kept: from, to)
  // (up to two describing words may sit between the number and the unit: "3 Gold Discs", "2 Rose Gold Plated Charms"; "2 Tone" or "2 Initial" are not such words)
  const U_ADJ = "(?:gold|silver|rose|rosegold|rose-gold|white|yellow|black|plain|blank|small|large|big|mini|tiny|matching|engraved|round|heart|star|custom|personali[sz]ed|dainty|sterling|filled|plated|solid|separate|different|individual|gold-filled)\\s+";
  const COUNT_BEFORE = () => new RegExp("(?:^|[^\\w.])(?:[x×]\\s*)?(?:(\\d{1,2})\\s*[-–]\\s*)?" + NUMRE + "\\s*(?:[x×]\\s*|-\\s*)?(?:" + U_ADJ + "){0,3}(" + U_ALL + ")\\b", "gi");
  const COUNT_WORD = () => new RegExp("\\b(double|triple|duo|trio|twin|dual|quad)[\\s-]+(?:of\\s+)?(?:" + U_ADJ + "){0,3}(" + U_ALL + ")\\b", "gi"), WORD_N = { double: 2, triple: 3, duo: 2, trio: 3, twin: 2, dual: 2, quad: 4 };
  const COUNT_AFTER = () => new RegExp("\\b(" + U_ALL + ")\\s*(?:[:=]|[x×])\\s*" + NUMRE + "\\b(?!\\s*(?:mm|cm|in\\b|inch|\"|”|k\\b))", "gi");
  const SET_OF = () => new RegExp("\\b(?:set|pack|bundle|lot)\\s+of\\s+" + NUMRE + "\\b|\\b" + NUMRE + "\\s*(?:-\\s*)?(?:piece|pc|pcs|pack)\\b", "gi");
  const SECOND_OPT = "Two designs on this line";   // (the pseudo option a person's answer about a line that says two designs is kept under; its value is "<receipt>/<transaction>": one answer per line)
  const NOTE_OPT = "Buyer note";   // (the pseudo option a count asked about a NOTE is kept under: one answer per wording, per listing)
  const NAME_CUE = /\bhow\s+many\b|\b(?:number|count|quantity|qty|amount|no\.?|#)\s+of\b|^\s*(?:quantity|qty|count)\b|\b(?:qty|quantity|count)\b/i;
  // an option that ADDS a piece ("Add a second charm: Yes +$12", "2 extra charms", "Add another disc") or says "2 of 3" does not say how many the line makes in all: asked
  const ADD_WORD = /\b(?:add(?:ed|ing|itional)?|extra|another|second|2nd|third|3rd|fourth|4th)\b/i, PIECE_WORD = /\b(?:discs?|disks?|tags?|charms?|pendants?|beads?)\b/i, NO_ANSWER = /^\s*(?:no\b|none\b|n\/a|without\b|skip\b|not needed|no thanks)/i;
  // a name that is a size, a colour, a style: a bare number under it is never a count of pieces
  const NOT_COUNT_NAME = /\b(?:size|length|width|height|diameter|thickness|gauge|weight|font|colou?r|style|finish|chain|type|material|metal|shape)\b/i;
  const BARE_COUNT = new RegExp("^\\s*(?:[x×]\\s*)?" + NUMRE + "\\s*(?:[x×]|pcs?|pieces?)?\\s*$", "i");
  const SAFE_NUM = /\b\d+(?:\.\d+)?\s*(?:mm|cm|in\b|inch(?:es)?|"|”|''|k\b|kt\b|karat|ct\b|g\b|oz\b|us\b|st\b|nd\b|rd\b|th\b)/gi;
  /** Every count a piece of text names: [{ n, from, cls, range }] (cls: piece | text | design | length | "" for no unit). */
  function countsIn(text) {
    const t = String(text || ""), out = [];
    for (let m, re = COUNT_BEFORE(); (m = re.exec(t));) { const n = numOf(m[2]); if (n) out.push({ n, cls: unitClass(m[3]), unit: m[3].toLowerCase(), range: !!m[1] && numOf(m[1]) !== n, text: m[0].trim() }); }
    for (let m, re = COUNT_WORD(); (m = re.exec(t));) out.push({ n: WORD_N[m[1].toLowerCase()], cls: unitClass(m[2]), unit: m[2].toLowerCase(), range: false, text: m[0].trim() });
    for (let m, re = COUNT_AFTER(); (m = re.exec(t));) { const n = numOf(m[2]); if (n) out.push({ n, cls: unitClass(m[1]), unit: m[1].toLowerCase(), range: false, text: m[0].trim() }); }
    for (let m, re = SET_OF(); (m = re.exec(t));) { const n = numOf(m[1] || m[2]); if (n) out.push({ n, cls: "", unit: "", range: false, text: m[0].trim() }); }
    return out;
  }
  /** What ONE option (name, value) says about a count, or null. { n, cls, rule, certain, ask, why, unit, text }:
   *  certain: n is settled; ask: a person must say (why is the one-line reason); design: n is a number of designs, not of pieces. */
  function optionCount(name, value) {
    const nm = String(name || ""), val = String(value || "").trim();
    if (!val || isPersonalisation(nm) || isPriceOption(nm)) return null;
    // 1 · the value names a number of something ("2 Disc", "3 discs • gold", "Silver • 2 symbols", "1-5 Character-SILVER")
    let hits = countsIn(val.replace(SAFE_NUM, " "));
    const lens = hits.filter(h => h.cls === "length");
    hits = hits.filter(h => h.cls !== "length");
    const cue = NAME_CUE.test(nm), nameUnit = (new RegExp("\\b(" + U_ALL + ")\\b", "i").exec(nm) || [])[1] || "", nameCls = unitClass(nameUnit);
    // 0 · an option that ADDS a piece ("Add a second charm: Yes +$12", "2 extra charms", "Add another disc"), or says "Disc 2 of 3": the number in it is not what the
    //     line makes in all (the base piece is not in it, or the whole count is the second number). A person says.
    const both = (nm + " " + val).replace(/\bextra[\s-]*(?:small|large|big|long|short|thin|wide|tiny|light|heavy|thick|dainty|sm|lg|xl)\b/gi, " ");
    if (ADD_WORD.test(both) && PIECE_WORD.test(both) && !/\badd[\s-]*ons?\b/i.test(both) && !NO_ANSWER.test(val)) return { n: 0, cls: "", rule: "count:add", certain: false, ask: true, why: `“${clip(nm, 40)}: ${clip(val, 40)}” adds a piece: how many pieces does the line make in all?`, unit: "", text: val };
    const ord = /\b(\d{1,2})\s+of\s+(\d{1,2})\b/i.exec(both);
    if (ord && PIECE_WORD.test(both)) return { n: +ord[2], cls: "", rule: "count:ordinal", certain: false, ask: true, why: `“${clip(nm, 40)}: ${clip(val, 40)}” says ${ord[1]} of ${ord[2]}: is the line ${ord[2]} pieces in all?`, unit: "", text: val };
    if (!hits.length && !lens.length && (cue || (nameCls && nameCls !== "length" && !NOT_COUNT_NAME.test(nm) && BARE_COUNT.test(val.replace(SAFE_NUM, " "))))) {
      // 2 · the NAME asks "how many" / "number of" and the value is a bare number or a number word ("Number of Discs: 3", "How many charms?: Two")
      const nums = [...new Set((val.replace(SAFE_NUM, " ").replace(/[x×]\s*(?=\d)/gi, " ").match(new RegExp("\\b" + NUMRE + "\\b", "gi")) || []).map(numOf).filter(Boolean))];
      const w = /\b(single|double|triple)\b/i.exec(val), words = w ? { single: 1, double: 2, triple: 3 }[w[1].toLowerCase()] : 0;
      const n = nums.length === 1 ? nums[0] : !nums.length && words ? words : 0, cls = nameCls;
      if (nums.length > 1) return { n: 0, cls, rule: "count:two numbers", certain: false, ask: true, why: `“${clip(nm, 40)}: ${clip(val, 40)}” names two numbers`, unit: nameUnit, text: val };
      if (cls === "length") return null;
      if (n) return finish({ n, cls, unit: nameUnit, range: false, text: val }, "count:name");
      return null;
    }
    if (lens.length && !hits.length) return null;                                         // "1-5 Character": how long the engraving is
    if (!hits.length) return null;
    if (new Set(hits.map(h => h.n)).size > 1) return { n: 0, cls: "", rule: "count:two numbers", certain: false, ask: true, why: `“${clip(nm, 40)}: ${clip(val, 40)}” names more than one number`, unit: "", text: val };
    return finish(hits[0], "count:unit");
    function finish(h, rule) {
      const base = { n: h.n, cls: h.cls, rule, certain: false, ask: false, why: "", unit: h.unit, text: h.text };
      if (h.range) return Object.assign(base, { n: 0, ask: true, why: `“${clip(nm, 40)}: ${clip(val, 40)}” gives a range, not a number`, rule: "count:range" });
      if (h.n === 1) return Object.assign(base, { certain: true });                          // one of anything is one piece
      if (h.cls === "piece") return Object.assign(base, { certain: true });
      if (h.cls === "design") return Object.assign(base, { rule: "count:designs" });         // decided with the line: an earring pair's designs, or a question
      if (h.cls === "text") return Object.assign(base, { ask: true, rule: "count:text", why: `“${clip(nm, 40)}: ${clip(val, 40)}” counts ${h.unit}: separate pieces, or all on one?` });
      return Object.assign(base, { ask: true, rule: "count:unclear", why: `“${clip(nm, 40)}: ${clip(val, 40)}” may count pieces, but does not say of what` });
    }
  }
  /** What the buyer's NOTE says about a count of pieces ("Three discs", "2 charms", "Tag 1: J, Tag 2: Q"): { n, text } or null. Only
   *  separate pieces (discs, tags, charms, pendants, beads) and numbered markers; never settles a count, it is compared with the options. */
  function noteCountOf(line) {
    const texts = [].concat(((line && line.variations) || []).filter(v => isPersonalisation(lvName(v))).map(lvValue), (line && line.personalization) || [], (line && (line.buyerMessage || line.message_from_buyer)) || []).map(s => visible(s));
    const found = new Map();
    for (const t of texts) {
      for (const h of countsIn(t.replace(SAFE_NUM, " "))) if (h.cls === "piece" && h.n >= 2 && !h.range) found.set(h.n, h.text);
      const marks = new Set(); for (let m, re = /\b(?:tag|disc|disk|charm|pendant)s?\s*#?\s*(\d)\s*[:=)\-.]/gi; (m = re.exec(t));) marks.add(+m[1]);
      if (marks.size >= 2 && Math.max(...marks) === marks.size) found.set(marks.size, `${[...marks].map(k => "Tag " + k).join(", ")}`);
    }
    return found.size === 1 ? { n: [...found.keys()][0], text: [...found.values()][0] } : null;
  }
  /** The count a line's options give for ONE unit of the product: { n (0 = none), certain, answered, opts: [{ name, value, ...optionCount }],
   *  asks: [{ name, value, guess, why, rule }], designs (n symbols/designs named, 0 = none), note }.
   *  A person's answer for that listing and option value ({ field: "count", value: "3" }) settles it (1 = "just one piece"). */
  function countRead(line, o) {
    o = o || {}; const opts = [], asks = []; let answered = 0;
    for (const v of (line && line.variations) || []) {
      const name = lvName(v), value = lvValue(v).trim(); if (!name || !value || isPersonalisation(name)) continue;
      const hit = optionLookup(o.optionMaps, line.listingId, name, value);
      if (hit && hit.field === "count") { const n = Math.max(1, Math.min(12, Math.floor(+hit.value) || 1)); opts.push({ name, value, n, cls: "", rule: "answer", certain: true, ask: false, why: "", answered: true }); if (!answered) answered = n; continue; }
      const c = optionCount(name, value); if (c) opts.push(Object.assign({ name, value }, c));
    }
    const settled = opts.filter(c => c.certain && !c.answered && c.cls !== "design" && c.n > 1);
    const ns = [...new Set(settled.map(c => c.n))];
    const read = { n: answered || (ns.length === 1 ? ns[0] : 0), certain: !!answered || ns.length === 1, answered: !!answered, opts, asks, designs: 0, note: noteCountOf(line) };
    // the buyer's note names several pieces and no option gives a count: a person's answer for that wording (kept under the pseudo option "Buyer note"), else asked below
    if (!answered && read.note) {
      const hit = optionLookup(o.optionMaps, line.listingId, NOTE_OPT, read.note.text);
      if (hit && hit.field === "count") { const n = Math.max(1, Math.min(12, Math.floor(+hit.value) || 1)); opts.push({ name: NOTE_OPT, value: read.note.text, n, cls: "", rule: "answer", certain: true, ask: false, why: "", answered: true }); answered = n; Object.assign(read, { n, certain: true, answered: true }); }
    }
    if (!answered) {
      if (ns.length === 0 && read.note && !opts.some(c => c.ask)) asks.push({ name: NOTE_OPT, value: read.note.text, guess: read.note.n, why: `the buyer's note says “${clip(read.note.text, 30)}” (${read.note.n} pieces), but no option gives a count`, rule: "count:note-only" });
      for (const c of opts) if (c.ask) asks.push({ name: c.name, value: c.value, guess: c.n || 0, why: c.why, rule: c.rule });
      if (ns.length > 1) { const c = settled[1]; asks.push({ name: c.name, value: c.value, guess: 0, why: `two options name different counts (${ns.join(" and ")})`, rule: "count:conflict" }); }
      // the buyer's note names a different number of pieces than the option: a person reads the note
      if (ns.length === 1 && read.note && read.note.n !== ns[0]) { const c = settled[0]; asks.push({ name: c.name, value: c.value, guess: ns[0], why: `the option says ${ns[0]}, the buyer's note says ${read.note.n} (“${clip(read.note.text, 30)}”)`, rule: "count:note" }); }
    }
    read.designs = Math.max(0, ...opts.filter(c => c.cls === "design" && !c.answered).map(c => c.n));
    return read;
  }

  // (EARWORDS) words around the earring words, read by lineSignals
  const CHARM_ONLY_CHOICE = /\b(?:charms?|pendants?)\s*only\b|\bonly\s+(?:the\s+)?(?:charms?|pendants?)\b|\b(?:no|without)\s+(?:a\s+)?(?:chain|hoops?|earrings?)\b|\bloose\s+charms?\b|^\s*(?:charm|pendant)s?(?:\s*[+&]\s*engraving)?\s*$/i;   // (the same choices the Design Station's purchase type calls "Charm only")
  const NAME_STRONG = /\b(?:ear[\s-]?rings?|earings?|studs?|huggies)\b|\b(?:left|right)\s+ears?\b/i, NAME_SKIP = /\b(?:add(?:s|ed|ing|itional)?|extra|upgrade|bonus|free|gift|also|include[sd]?|matching|optional|engrav\w*|text|message|note|inscription|initials?|names?)\b/i;
  const SKU_EAR = /(?:^|[^a-z0-9_])huggies?(?![a-z0-9_])|(?:^|[^a-z0-9])huggie[\s_-]*hoops?(?![a-z0-9])|^\s*hoops?\s*-|(?:^|[^a-z0-9])(?:earrings?|studs?)(?:[^a-z0-9]|$)|^\s*cust(?:om)?[\s_.-][hes][\s_.-]/i;   // the shop's own earring SKUs: "Huggie Hoops- X", "X (HUGGIE)", "X - HUGGIE", "HOOPS- X", an EARRING or STUD word, CUSTOM-H = huggies, -E and -S = earrings (see customKind); never "Huggie_3722" (a numbered design that happens to be named Huggie)
  const PAIR_ASK_TITLE = /\bpair\s+of\b|\b(?:a|one|1|matching|mismatched)\s+pair\b|\bset\s+of\s+(?:2|two)\b/i;
  const EARS_OPT = "Earrings in this listing's words";   // (the pseudo option a person's answer to "is this a pair of earrings?" is kept under; its value is the listing title: one answer per title; form = earrings / huggie, ignore = not earrings)
  /** What a line's own words say: { says (the listing, an option or a note says "mismatched" / two designs / left and right), signals[],
   *  soldAs ("pair" | "single" | null: earrings, studs, huggies are sold as a pair; an option or title that says Single is one),
   *  soldBy ("option" | "title" | "words" | null), side ("L" | "R" | null: a single earring that names its ear), discs }.
   *  Pure; takes the sorter's line or a raw Etsy transaction (formatted_name / formatted_value, message_from_buyer). No master, no network. */
  function lineSignals(line) {
    line = line || {};
    const vars = (line.variations || []).map(v => ({ name: lvName(v), value: lvValue(v) })).filter(v => v.name || v.value);
    const title = String(line.title || ""), signals = [];
    if (MIS_WORD.test(title)) signals.push(`title “${clip(title)}”`);
    for (const v of vars) {
      if (isPersonalisation(v.name)) { if (MIS_WORD.test(v.value)) signals.push(`note “${clip(v.value)}”`); continue; }
      if (MIS_WORD.test(v.value) || TWO_DESIGNS.test(v.value)) signals.push(`option “${clip(v.name, 30)}: ${clip(v.value, 40)}”`);
      else if (SIDE_NAME.test(v.name) && SIDE_THING.test(v.name)) signals.push(`option “${clip(v.name, 40)}”`);
    }
    for (const s of [].concat(line.personalization || [], line.buyerMessage || line.message_from_buyer || [])) if (MIS_WORD.test(visible(s))) signals.push(`note “${clip(visible(s))}”`);
    const opts = vars.filter(v => !isPersonalisation(v.name)).map(v => v.value), text = [title].concat(opts).join(" ");
    /* The words of an earring line (EARWORDS, 10 Oct; Paul: "ALL earrings must be mirror versions of each other", a pair is ALWAYS two pieces: no earring wording may leave a pair as one piece).
       STRONG words say it alone: earring(s) (also "ear rings", the misspelling "earings"), stud(s), huggies, huggie hoops, huggie charm set(s).
       WEAK words, a bare "hoop" or a bare "huggie" ("Poodle Huggie Charm"), say it unless the TITLE names another product ("Gold Hoop Charm Necklace Pendant", a hoop charm bracelet, anklet or key ring;
       ADVSTATION, 9 Oct): such a line is one piece unless a chosen option has a weak word or a strong word stands anywhere. A word in a chosen OPTION always counts, a word in a note or a gift text never.
       An option NAME says it too ("Earring Style: Huggie", "Hoop Size: 8.5mm", "Left Ear Charm") when the option has an answer and the name is not an extra to add ("Add matching earrings"), and the
       shop's own earring SKU does ("Huggie Hoops- X", "X (HUGGIE)", an EARRING or STUD word, CUSTOM-H / -E / -S): both only while the buyer did not choose the loose charm, and a SKU not under a title that names
       another product. A negated ear word ("Charm only (no hoops)") says nothing. NOT guessed, a person is asked once (the answer is kept for the listing's title, `ask`): "huggie" in a title that also names
       another product, an earring SKU under such a title, and "Pair of ... charms" / "Set of 2" with no earring word at all. */
    const EAR_STRONG = /\b(?:ear[\s-]?rings?|earings?|studs?|huggies|hugg(?:ie|y)\s+(?:hoops?|charms?\s+sets?|sets?))\b/i, WEAK = /\b(?:hoops?|hugg(?:ie|y))\b/i, HUGG = /\bhugg(?:ie|y)\b/i, OTHER_PRODUCT = /\b(?:necklaces?|pendants?|bracelets?|anklets?|key\s*(?:chains?|rings?)|keychains?|chokers?)\b/i;
    const otherInTitle = OTHER_PRODUCT.test(title), weakOnlyInTitle = otherInTitle && !WEAK.test(notEar(opts.join(" ")));
    const EAR = { test: str => { const t = notEar(str); return EAR_STRONG.test(t) || (WEAK.test(t) && !weakOnlyInTitle); } };
    const charmOnly = opts.some(x => CHARM_ONLY_CHOICE.test(x));
    const nameEar = !charmOnly && vars.some(v => {
      // (a name that offers two products, "Earrings or Necklace", or a value that is another product, says nothing: the value chosen decides)
      if (isPersonalisation(v.name) || !String(v.value).trim() || NO_ANSWER.test(v.value) || NAME_SKIP.test(v.name) || /\bor\b|\//i.test(v.name) || OTHER_PRODUCT.test(v.name) || OTHER_PRODUCT.test(v.value)) return false;
      const nm = notEar(v.name); return NAME_STRONG.test(nm) || (WEAK.test(nm) && !otherInTitle);
    });
    const lineSku = String(line.sku || line.__sku || ""), skuEar = !charmOnly && SKU_EAR.test(lineSku), earText = EAR.test(text), skuUnderOther = skuEar && otherInTitle && !earText && !nameEar;
    const earOutside = nameEar || (skuEar && !skuUnderOther), earAll = earText || earOutside;
    const SINGLE_TXT = /\bsingle\s+(?:stud\s+|huggie\s+|hoop\s+)?(?:earring|stud|huggie|charm)\b|\b(?:1|one)\s+(?:single\s+)?(?:earring|stud|huggie)\b(?!s)/i, SINGLE_END = /\bsingle(?:\s+(?:earring|stud|huggie|hoop|charm))?\s*$/i;
    // a TITLE that says Single: next to the earring word, or a few words before it ("Custom Single Replacement Silver Cat Huggie Earring Left Ear"), or at its end
    // ("Huggie Earring, Single", "Poodle Earring Single"). "Single Pearl Stud Earrings" is a pair of earrings with one pearl each, not a single earring: the plural word after it settles that.
    const SING_WORD = "(?:single|individual|solo)", SING_EAR = /\b(?:earring|stud|huggie|hoop)\b(?!s)(?!\s+(?:earrings|studs|huggies|hoops)\b)/i, PLURAL_OR_PAIR = /\b(?:earrings|studs|huggies|hoops|pairs?|sets?|both)\b/i;
    const TITLE_SINGLE = t => SINGLE_TXT.test(t) || ((EAR.test(t) || earOutside) && (new RegExp("\\b" + SING_WORD + "\\b(?:\\s+[\\w'’&.-]+){0,6}?\\s+(?:earring|stud|huggie|hoop)\\b(?!s|\\s+(?:earrings|studs|huggies|hoops)\\b)", "i").test(t) || new RegExp("(?:^|[,;|(\\-–—]\\s*)" + SING_WORD + "\\s*\\)?\\s*$", "i").test(t)
      || new RegExp("\\b(?:earring|stud|huggie|hoop)\\s*[,;:|(\\[\\-–—]*\\s*" + SING_WORD + "\\s*[)\\]]?\\s*$", "i").test(t)
      // no "single" at all, but ONE ear word and ONE ear named ("Right Earring Replacement", "Replacement Stud Earring for Left Ear", "Poodle Earring (Right)")
      || (SING_EAR.test(t) && !PLURAL_OR_PAIR.test(t) && !!singleSideOf({ title: t }))));
    const PAIR_OPT = /\bpair\b|\b(?:2|two)\s+(?:earrings|studs|huggies|hoops)\b|\bset\s+of\s+(?:2|two)\b|\bboth\s+ears?\b/i;
    // an option whose whole value is an ear ("Left", "Right ear only", "Side: Left ear") on an earring line is a single earring for that ear, when it names only one
    const SIDE_ONLY = /^\s*(?:the\s+)?(left|right)(?:\s+(?:ear|earring|side|one|only))*\s*$/i, sidesOnly = new Set(opts.map(x => (SIDE_ONLY.exec(x) || [])[1]).filter(Boolean).map(x => x.toLowerCase()));
    const optSingle = opts.some(x => SINGLE_TXT.test(x) || SINGLE_END.test(x) || (earAll && /\b(?:single|individual|solo)\b/i.test(x))) || (earAll && sidesOnly.size === 1), optPair = !optSingle && earAll && opts.some(x => PAIR_OPT.test(x));
    const soldBy = optSingle || optPair ? "option" : TITLE_SINGLE(title) ? "title" : earAll ? "words" : null;
    const soldAs = optSingle ? "single" : optPair ? "pair" : TITLE_SINGLE(title) ? "single" : earAll ? "pair" : null;
    const side = singleSideOf(line), sideBy = side ? (singleSideOf(Object.assign({}, line, { personalization: [], buyerMessage: null, message_from_buyer: null, variations: (line.variations || []).filter(v => !isPersonalisation(lvName(v))) })) === side ? "listing" : "note") : null;   // (listing: the title or an option names the ear, so it is every unit's; note: the buyer's words)
    // the wordings that are NOT guessed (see above): what a person is asked, once per listing title
    let ask = null;
    if (!soldAs && !charmOnly) {
      const t0 = notEar(title), other = (OTHER_PRODUCT.exec(title) || [])[0] || "", key = norm(title).slice(0, 190) || norm(opts.join(" ")).slice(0, 190);
      const pairPhrase = (PAIR_ASK_TITLE.exec(t0) || [])[0] || "";   // (a TITLE only: an option that says "Pair" is an unmapped option, already asked about)
      if (key && skuUnderOther) ask = { by: "sku", value: key, why: `the SKU “${clip(lineSku, 30)}” is an earring SKU, but the title names a ${other.toLowerCase()}: is this a pair of earrings (a Left and a Right)?` };
      else if (key && HUGG.test(t0) && otherInTitle && !earAll) ask = { by: "title", value: key, why: `the title says “huggie” and also “${other.toLowerCase()}”: is this a pair of earrings (a Left and a Right)?` };
      else if (key && pairPhrase && !earAll && !otherInTitle) ask = { by: "pair", value: key, why: `“${clip(pairPhrase, 40)}” says a pair but names no earring: is this a pair of earrings (a Left and a Right)?` };
    }
    return { says: signals.length > 0, signals: [...new Set(signals)], soldAs, soldBy, side, sideBy, discs: discsIn(line), ask, other: OTHER_PRODUCT.test(title) };
  }
  /** Which ear a SINGLE earring is for, when the line names it: an option value ("Single - Left", "Right ear") or the buyer's note ("left ear
   *  only", "for my right ear"). Both ears named, or neither: null (unspecified: flagged, never guessed). */
  function singleSideOf(line) {
    const got = new Set();
    const tell = (t, strict) => {
      t = visible(t);
      for (const m of t.matchAll(/\b(left|right)\s+(?:ear(?:ring)?|stud|hoop|huggie|side)\b|\b(?:only|just|single)\s+(?:the\s+|a\s+|one\s+)?(left|right)\b|\b(?:for|on|in)\s+(?:my|the|her|his)\s+(left|right)\b|\b(left|right)\s+(?:one\s+)?only\b|\b(?:earring|stud|huggie|hoop)\s*[,;:|(\[\-–—]*\s*\(?\s*(left|right)\s*\)?(?:\s+(?:ear|side))?\s*$/gi)) got.add((m[1] || m[2] || m[3] || m[4] || m[5]).toLowerCase());   // (the last: the ear named after a singular ear word at the end, "Poodle Earring (Right)")
      if (strict && /^\s*(left|right)(?:\s+(?:ear(?:ring)?|side|one))?\s*$/i.test(t)) got.add(/left/i.test(t) ? "left" : "right");
      if (strict) for (const m of t.matchAll(/\b(?:single|1)\b.*\b(left|right)\b|\b(left|right)\b.*\bsingle\b/gi)) got.add((m[1] || m[2]).toLowerCase());
    };
    for (const v of (line && line.variations) || []) { const name = lvName(v), value = lvValue(v); if (isPersonalisation(name)) tell(value, false); else if (/side|ear|earring/i.test(name) || /\b(?:single|ear|earring|stud|hoop|huggie)\b/i.test(value) || /^\s*(?:left|right)\b/i.test(value)) tell(value, true); }
    for (const s of [].concat((line && line.personalization) || [], (line && (line.buyerMessage || line.message_from_buyer)) || [])) tell(s, false);
    if (!got.size && line && line.title) tell(line.title, false);   // (the title names the ear only when nothing else did: "... Earring Left Ear")
    return got.size === 1 ? (got.has("left") ? "L" : "R") : null;
  }
  /** Does this line say it is a mismatched pair? A line only: for pages that hold raw Etsy transactions and no master. */
  // (only a line sold as an earring pair can be a mismatched pair: a necklace charm SKU "CAT(+FISH) - Cat Only" or "Cute Triceratops w/ hearts" is one design)
  const lineMismatched = line => { const sig = lineSignals(line); return sig.says || (sig.soldAs === "pair" && !!splitSkus(line && line.sku)); };
  /** A SKU that names two designs ("MITTENS 1 + MITTENS 2", "A / B", "A & B", "A, B", "A and B"): [first, second], else null. Only a
   *  reading of the text: a caller accepts it only when both parts are designs in the master. */
  function splitSkus(sku) {
    const parts = String(sku == null ? "" : sku).replace(/\bw\//gi, "w ").split(/\s*(?:\/|\+|&|;|,)\s*|\s+AND\s+/i).map(s => s.trim()).filter(Boolean);
    return parts.length === 2 ? parts : null;
  }
  /* ── two designs named in WORDS (Paul, 10 Oct 2026: "Mismatched Tennis Ball and Raquet Huggie Hoops", SKU "Huggie Hoops-Tennis Ball/Racket3") ─────────────
     A line that says two designs and names both, each reading as exactly ONE master design, is a mismatched pair: the first named is the Left, the second the Right.
     Exact only: a name is the master SKU spelled with other spacing or punctuation (masterSku), nothing that merely looks like it. On a huggie-hoops line a name is
     "HUGGIE HOOPS- NAME", else the same design drawn small, "NAME (HUGGIE)" (the huggie charm of the master, the size those hoops carry); never the necklace-size
     charm of that name. The second name may leave out what the first starts with ("Tennis Ball and Racket": the Racket of Tennis). A shop's own spelling of a word
     (Raquet, Racquet) is read as the master's; a number a SKU ends its name with ("Racket3") is dropped only when the TITLE names the same two designs. */
  const SPELLING = { raquet: "racket", racquet: "racket", raquets: "rackets", racquets: "rackets", tenis: "tennis" };
  const FAMILY_HEAD = /^\s*huggie\s*hoops?\s*[-–:]*\s*/i;
  const wordsOfName = s => String(s == null ? "" : s).toLowerCase().replace(/&amp;/g, " ").replace(/[^a-z0-9]+/g, " ").trim().split(/\s+/).filter(Boolean).map(w => SPELLING[w] || w);
  const bareWords = ws => ws.map((w, i) => (i === ws.length - 1 ? w.replace(/^([a-z]+)\d+$/, "$1") : w));
  const agreeWords = (a, b) => { const x = bareWords(a).join(" "), y = bareWords(b).join(" "); return !!x && !!y && (x === y || x.endsWith(" " + y) || y.endsWith(" " + x)); };
  /** The two design names a SKU gives ("Huggie Hoops-Tennis Ball/Racket3" → ["Tennis Ball", "Racket3"]), or null. */
  function skuNames(line) {
    const parts = splitSkus(line && line.sku); if (!parts) return null;
    const a = parts[0].replace(FAMILY_HEAD, "").trim(), b = parts[1].replace(FAMILY_HEAD, "").trim();
    return a && b ? [a, b] : null;
  }
  /** The two design names a title that says "mismatched" gives ("Mismatched Tennis Ball and Raquet Huggie Hoops Charm, …" → ["Tennis Ball", "Raquet"]), or null.
   *  Only the title's first phrase; the product words (huggie, hoops, studs, earrings, charm, set, pair…) end the names. */
  function titleNames(title) {
    const lead = String(title || "").split(/\s*[,|•·:;–—(\[]\s*|\s+-\s+/)[0].replace(MIS_WORD, " ").replace(/\bmix(?:ed)?\s*(?:and|&|n)?\s*match(?:ed)?\b/gi, " ");
    const cut = /\b(?:huggies|huggie|hoops?|studs?|earrings?|charms?|sets?|pairs?|dangles?|drops?|jewell?e?ry)\b/i.exec(lead);
    const names = (cut ? lead.slice(0, cut.index) : lead).trim().replace(/^(?:a|an|the|one|1)\s+/i, "").split(/\s+(?:and|&|\+)\s+|\s*\/\s*/i).map(x => x.replace(/^(?:a|an|the|one|1)\s+/i, "").trim()).filter(Boolean);
    return names.length === 2 ? names : null;
  }
  /** The master SKU a design name stands for, in the line's family, "" when none. */
  function designByName(ws, me, loose, huggie) {
    const name = ws.join(" ").toUpperCase(); if (!name) return "";
    for (const c of huggie ? ["HUGGIE HOOPS- " + name, name + " (HUGGIE)"] : [name]) { const k = masterSku(c, me, loose); if (k) return k; }
    return "";
  }
  /** Both names of a pair, each as a master SKU or "". digits: the SKU's trailing number may be dropped (the title agrees). → { found: [sku, sku], ok } */
  function resolveNames(names, me, loose, huggie, digits) {
    const w = names.map(wordsOfName), found = ["", ""];
    for (let i = 0; i < 2; i++) {
      const tries = [w[i]]; if (digits) tries.push(bareWords(w[i]));
      if (i === 1 && w[0].length > 1) for (const t of tries.slice()) tries.push(w[0].slice(0, -1).concat(t));   // (the first name's lead words: "Tennis" Ball, so "Racket" is "Tennis Racket")
      for (const t of tries) { found[i] = designByName(t, me, loose, huggie); if (found[i]) break; }
    }
    return { found, ok: !!found[0] && !!found[1] && found[0] !== found[1] };
  }
  const HUGGIE_LINE = /\bhuggie|\bhoops?\b/i;
  /** { members, source: "skus" | "title", names, found } when the SKU's two names or the title's two names are each exactly one master design (and agree when both
   *  are there); else { names, found, why } saying in one plain line which design is missing; null when the line names no two designs. */
  function nameMembers(line, me, loose, said) {
    const sn = skuNames(line), tn = MIS_WORD.test(String(line.title || "")) ? titleNames(line.title) : null;
    if (!sn && !tn) return null;
    const huggie = HUGGIE_LINE.test([String(line.title || "")].concat(((line && line.variations) || []).filter(v => !isPersonalisation(lvName(v))).map(lvValue)).join(" ") + " " + String(line.sku || ""));
    const agree = !!sn && !!tn && agreeWords(wordsOfName(sn[0]), wordsOfName(tn[0])) && agreeWords(wordsOfName(sn[1]), wordsOfName(tn[1]));
    const tries = [];
    if (tn) tries.push({ from: "title", names: tn, r: resolveNames(tn, me, loose, huggie, false) });
    if (sn) tries.push({ from: "skus", names: sn, r: resolveNames(sn, me, loose, huggie, agree) });
    const good = tries.filter(t => t.r.ok);
    if (good.length === 2 && (good[0].r.found[0] !== good[1].r.found[0] || good[0].r.found[1] !== good[1].r.found[1]))
      return { names: good[0].names, found: good[0].r.found, why: `the SKU and the title name different designs (${good[1].r.found.join(" + ")} and ${good[0].r.found.join(" + ")}): name the right second design, or say it is the same on both ears` };
    // one source is exact, the other names a design that is one of the master's but not the same: the line contradicts itself, nothing is chosen between them
    if (good.length === 1 && tries.length === 2) { const o = tries.find(t => !t.r.ok), a = good[0].r.found, b = o.r.found || []; const hit = b.map((f, i) => f && a[i] && f !== a[i] ? f : "").filter(Boolean);
      if (hit.length) return { names: good[0].names, found: a, why: `the ${o.from === "skus" ? "SKU" : "title"} names ${hit.map(h => `“${h}”`).join(" and ")} but the ${good[0].from === "skus" ? "SKU" : "title"} names ${a.join(" + ")}: the line says two different designs that do not agree. Name the right second design, or say it is the same on both ears` }; }
    if (good.length) { const g = good[good.length - 1]; return { members: [{ side: "L", sku: g.r.found[0] }, { side: "R", sku: g.r.found[1] }], source: g.from, names: g.names, found: g.r.found }; }
    const t = tries[0], lack = t.names.map((n, i) => t.r.found[i] ? "" : `“${clip(n, 30)}”`).filter(Boolean), have = t.names.map((n, i) => t.r.found[i] ? `“${clip(n, 30)}” is ${t.r.found[i]}` : "").filter(Boolean);
    return { names: t.names, found: t.r.found, why: `the line says two different designs (${said || "its words"}) and names ${t.names.map(n => `“${clip(n, 30)}”`).join(" and ")}, but ${have.length ? have.join(", ") + " and " : ""}no single master design reads as ${lack.length === 2 ? "either" : lack[0]}: name the missing design, or say it is the same on both ears` };
  }

  /* ── a Zodiac line that says 2 symbols (Paul, 10 Oct 2026: "Silver • 2 symbols", Zodiac Sign Libra, note "balance et lion") ────────────────────────────
     The second symbol is looked for in the line: another option value that is a sign, then the buyer's note and personalisation, in English and the common
     shop languages (balance = Libra, lion = Leo). Two different signs make a pair, the first named the Left. Each sign's charm is what that value of the Zodiac Sign
     option decides on this listing (a person's answer, or the SKU Etsy ties to the value): a sign with no charm yet is asked once like any option value, never
     picked by looks. A second sign found nowhere is not invented: the line waits and says so. */
  const SIGN_WORDS = { Aries: "aries belier ariete widder", Taurus: "taurus taureau tauro stier toro touro", Gemini: "gemini gemeaux geminis zwillinge gemelli gemeos tweelingen", Cancer: "cancer cancro krebs",
    Leo: "leo lion leone loewe lowe leao leeuw", Virgo: "virgo vierge jungfrau vergine virgem maagd", Libra: "libra balance waage bilancia weegschaal", Scorpio: "scorpio scorpion escorpio skorpion scorpione escorpiao schorpioen",
    Sagittarius: "sagittarius sagittaire sagitario schuetze schutze sagittario boogschutter", Capricorn: "capricorn capricorne capricornio steinbock capricorno steenbok", Aquarius: "aquarius verseau acuario wassermann acquario aquario waterman", Pisces: "pisces poissons piscis fische pesci peixes vissen" };
  const SIGN_OF = new Map(); for (const [sign, ws] of Object.entries(SIGN_WORDS)) for (const w of ws.split(" ")) SIGN_OF.set(w, sign);
  const plainText = s => String(s == null ? "" : s).normalize("NFD").replace(/[̀-ͯ]/g, "").toLowerCase().replace(/ß/g, "ss");
  /** The signs a piece of text names, in order, each once: ["Libra", "Leo"] for "balance et lion". */
  function signsOfText(text) {
    const out = []; for (const w of plainText(visible(text)).split(/[^a-z]+/)) { const s = w && SIGN_OF.get(w); if (s && !out.includes(s)) out.push(s); }
    return out;
  }
  /** { signs: [{ sign, name, value, from: "option" | "note", variation }], optionName } for a line whose options name a sign, or null. */
  function signsOfLine(line) {
    const list = []; let optionName = "";
    const add = (sign, rec) => { if (!list.some(x => x.sign === sign)) list.push(Object.assign({ sign }, rec)); };
    for (const v of (line && line.variations) || []) {
      const name = lvName(v), value = lvValue(v).trim(); if (!name || !value || isPersonalisation(name)) continue;
      const ss = value.split(/\s+/).length <= 4 ? signsOfText(value) : []; if (!ss.length) continue;
      if (!optionName) optionName = name;
      for (const sg of ss) add(sg, { from: "option", name, value, variation: v });
    }
    if (!list.length) return null;
    const notes = [].concat(((line && line.variations) || []).filter(v => isPersonalisation(lvName(v))).map(lvValue), (line && line.personalization) || [], (line && (line.buyerMessage || line.message_from_buyer)) || []);
    const said = [];   // (every sign any note names, each note's own words: a sign the drop-down already has is the same sign again, not a second one)
    for (const t of notes) for (const sg of signsOfText(t)) { said.push(sg); add(sg, { from: "note", name: optionName, value: sg, text: visible(t).trim() }); }
    const own = list.find(x => x.from === "option" && x.variation), pid = own ? idOf(own.variation.propertyId != null ? own.variation.propertyId : own.variation.property_id) : "";
    const text = [...new Set(notes.map(t => visible(t).trim()).filter(Boolean))].join(" · ");
    return { signs: list, optionName, propertyId: pid, said, noteText: text };
  }
  /** What the buyer's note says, as one short quoted piece for a row: “balance et lion”, or "there is no buyer's note". */
  const noteSays = text => text ? `the buyer's note says “${clip(text, 80)}”` : "there is no buyer's note";

  /* The charm a SIGN stands for on a listing, from the listing's OWN inventory (ZODIACTWO, 10 Oct 2026). The buyer wrote the second sign in a note ("balance et lion"); the
     drop-down of the listing has one value per sign and Etsy keeps the SKU of the product bought with each value. So: the table's value names (Etsy's text for each value of
     the option, kept with the table) are read with the SAME closed list of the twelve signs the note is read with (signsOfText: whole words, exact, never fuzzy): the one value of
     this option whose text names exactly this sign; the SKU of the live products with that value (one SKU, given to no other value of the option: tiesToOption's test); the
     master design that SKU is, of the line's own kind of piece (a necklace pendant is never an earring). Never the title, never the listing's description, never a look-alike,
     never AI. → { sku } or { why, needNames } saying in one plain line what stops it. */
  function signValueSku(table, line, optionName, propertyId, sign, me, loose) {
    if (!table || typeof table !== "object") return { why: "Etsy's list of this listing's options is not loaded yet", needNames: false };
    if (!Array.isArray(table.products)) return { why: table.uni ? `Etsy keeps one SKU (${upSku(table.uni)}) for every choice of this listing, so it cannot say which charm ${sign} is` : "Etsy keeps no SKU on this listing's options" };
    if (!table.names || typeof table.names !== "object") return { why: `Etsy's names for this listing's “${optionName}” values are not loaded yet`, needNames: true };
    let pid = idOf(propertyId);
    if (!pid) { const hit = Object.keys(table.props || {}).filter(k => norm(table.props[k]) === norm(optionName)); if (hit.length === 1) pid = hit[0]; }
    if (!pid) return { why: `Etsy's list has no option called “${optionName}”` };
    const vids = Object.keys(table.names).filter(k => k.startsWith(pid + ":")).filter(k => { const ss = signsOfText(table.names[k]); return ss.length === 1 && ss[0] === sign; }).map(k => k.slice(pid.length + 1));
    if (!vids.length) return { why: `no value of “${optionName}” on this listing is named ${sign}` };
    if (vids.length > 1) return { why: `more than one value of “${optionName}” on this listing is named ${sign}` };
    const vid = vids[0], live = (table.products || []).filter(p => p && !p.d), has = (p, x) => (p.pv || []).some(a => idOf(a[0]) === pid && idOf(a[1]) === x);
    const mine = live.filter(p => has(p, vid)), skus = [...new Set(mine.map(p => upSku(p.sku)).filter(Boolean))];
    if (!skus.length) return { why: `Etsy keeps no SKU on the ${sign} value of “${optionName}”` };
    if (skus.length > 1) return { why: `Etsy gives the ${sign} value of “${optionName}” more than one SKU (${skus.slice(0, 3).join(", ")})` };
    if (live.some(p => !has(p, vid) && upSku(p.sku) === skus[0])) return { why: `Etsy's SKU ${skus[0]} is shared by other values of “${optionName}”, so it does not say ${sign}` };
    const own = me ? masterSku(skus[0], me, loose) : skus[0];
    if (!own) return { why: `Etsy's SKU for ${sign}, ${skus[0]}, is not in any master file` };
    if (skuFamilyConflict(own, line)) { const f = lineFamily(line); return { why: `Etsy's SKU for ${sign}, ${own}, is a ${skuFamily(own)} design and this line is ${f === "earring" ? "earrings" : f}` }; }
    return { sku: own, etsy: skus[0] };
  }

  /** The pair of designs a person saved for this line's listing and SKU ({ L, R }, aliasPut pair), or null. The listing's own record and the SKU as the line carries it: no other listing, no other SKU, no title. */
  function pairLinkOf(line, aliases) {
    const a = aliases && line && aliases[String(line.listingId)], raw = String(line && line.sku || "").trim().toUpperCase(), k = a && a.pairBySku && raw && a.pairBySku[raw];
    return k && typeof k === "object" && k.L && k.R ? { L: upSku(k.L), R: upSku(k.R) } : null;
  }
  /** The two designs of a mismatched pair a line names, when both are in the master: [{ side: "L", sku }, { side: "R", sku }] or null.
   *  A person's "the same on both ears" for the line is kept and ends the search. Else, in this order: the pair of designs a person saved for this listing and this SKU (o.aliases,
   *  aliasPut pair → pairBySku: both designs must be in the master, else nothing is changed and report.why says so); the SKU naming two ("Huggie Hoops-Tennis Ball/Racket3": the
   *  second part may leave out the first's prefix); a person's second design for this line; two options named for the left and the right (an option map answer, or a
   *  value that is a master SKU); two designs named in words, by the SKU or a "Mismatched …" title, each exactly one master design (nameMembers); two signs of a
   *  "2 symbols" line (signsOfLine). → { members, source: "alias" | "skus" | "answer" | "options" | "title" | "signs" } or null.
   *  o.report, when given, is filled with what is missing: { why: one plain line, asks: [{ name, value, why }] (option values to answer), signs }. o.designOf(variation) gives the
   *  master design a value of an option decides on this listing ("" when none yet). */
  function pairMembers(line, o) {
    o = o || {}; const me = o.me; if (!me || !line) return null;
    const has = s => masterSku(s, me, o.loose), rep = o.report || {};
    // a person's answer for this line ("the same on both ears", kept under SECOND_OPT) is the last word: nothing below changes it
    const said = o.lineId ? optionLookup(o.optionMaps, line.listingId, SECOND_OPT, o.lineId) : null;
    if (said && said.field === "ignore") return null;
    // a pair a person saved for this listing and this SKU (the SKU Etsy gives every product of the listing): their answer beats any reading of words. Both designs must be in the master NOW:
    // a link never names a design that is not there and never stands in for another (it then says so, and the line is read as it was)
    const link = pairLinkOf(line, o.aliases);
    if (link) {
      const la = has(link.L), lb = has(link.R);
      if (la && lb && la !== lb) return { members: [{ side: "L", sku: la }, { side: "R", sku: lb }], source: "alias" };
      const miss = [!la ? link.L : "", !lb ? link.R : ""].filter(Boolean);
      rep.why = `the pair saved for this listing and SKU names ${link.L} (Left) and ${link.R} (Right), but ${miss.length ? `${miss.join(" and ")} ${miss.length > 1 ? "are" : "is"} not in any master file` : "both are the same design"}: name the designs again, or say it is the same on both ears`;
    }
    const parts = splitSkus(line.sku);
    if (parts && !has(String(line.sku || ""))) {
      // (the second part may leave out what the first starts with: "Huggie Hoops-Tennis Ball/Racket3" is "Huggie Hoops-Racket3")
      const a = has(parts[0]); let b = has(parts[1]);
      for (let i = parts[0].length - 1; !b && i > 0; i--) if (/[-_\s]/.test(parts[0][i])) b = has(parts[0].slice(0, i + 1) + parts[1]);
      if (a && b && a !== b) return { members: [{ side: "L", sku: a }, { side: "R", sku: b }], source: "skus" };
    }
    // a person named the second design for this line (Review: "Which second design goes on the other ear?"): the line's own design is the Left, theirs the Right
    if (o.lineId && o.sku) { const ans = said; if (ans && ans.field === "design" && ans.value) { const a = has(o.sku), b = has(ans.value); if (a && b && a !== b) return { members: [{ side: "L", sku: a }, { side: "R", sku: b }], source: "answer" }; } }
    const side = { L: "", R: "" };
    for (const v of line.variations || []) {
      const name = lvName(v), value = lvValue(v).trim(); if (!value || !SIDE_THING.test(name)) continue;
      const k = /\bleft\b/i.test(name) ? "L" : /\bright\b/i.test(name) ? "R" : ""; if (!k || side[k]) continue;
      const hit = optionLookup(o.optionMaps, line.listingId, name, value);
      side[k] = hit && hit.field === "design" && hit.value ? upSku(hit.value) : has(value);
    }
    if (side.L && side.R && side.L !== side.R) return { members: [{ side: "L", sku: side.L }, { side: "R", sku: side.R }], source: "options" };
    // two designs named in words (the SKU's or a "Mismatched …" title's), each exactly one master design: a mismatched pair; else the one missing design is told (report.why)
    const sig = lineSignals(line);
    if (sig.soldAs === "pair" && (sig.says || parts)) {   // (only a line sold as an earring pair: a necklace that names two charms is not two ears)
      const nm = nameMembers(line, me, o.loose, sig.signals[0]);
      if (nm && nm.members) return { members: nm.members, source: nm.source };
      if (nm && nm.why && sig.says && !rep.why) rep.why = nm.why;
    }
    // "Silver • 2 symbols" and a sign in the options: the second sign, from the line's own words (the options, then the buyer's note). The note is shown with every wait.
    if (sig.says && sig.soldAs === "pair" && o.designOf && !rep.why) {
      const sl = signsOfLine(line);
      if (sl) {
        rep.signs = sl.signs.map(x => x.sign);
        const first = sl.signs[0], note = noteSays(sl.noteText);
        if (sl.signs.length === 1) {
          // the note names the SAME sign as the drop-down (or only repeats it): both ears are that sign, nothing to ask (a Left and a Right, the Right the mirror of the Left)
          if (sl.said.length && sl.said.every(x => x === first.sign)) rep.same = { sign: first.sign, text: sl.noteText, why: `the buyer's note repeats ${first.sign}: the same sign on both ears` };
          else rep.why = `the line says two different designs (${sig.signals[0] || "an option"}) but names one (${first.sign}): ${note}. Write the second symbol in the order's note, name the second design, or say it is the same on both ears`;
        } else if (sl.signs.length > 2) rep.why = `the line says two different designs but names ${sl.signs.length} signs (${sl.signs.map(x => x.sign).join(", ")}): ${note}. Name the right two, or say it is the same on both ears`;
        else {
          rep.lack = rep.lack || {};
          const designs = sl.signs.map(x => { const virt = x.from === "option" ? x.variation : { name: sl.optionName, value: x.sign, propertyId: sl.propertyId }; return { x, sku: has(o.designOf(virt, x.from === "option") || "") }; });
          if (designs.every(d => d.sku) && designs[0].sku !== designs[1].sku) return { members: [{ side: "L", sku: designs[0].sku }, { side: "R", sku: designs[1].sku }], source: "signs", signs: rep.signs, note: sl.noteText };
          rep.asks = designs.filter(d => !d.sku && d.x.from === "note").map(d => ({ name: sl.optionName, value: d.x.sign, lack: (rep.lack[d.x.sign] || {}).why || "", needNames: !!(rep.lack[d.x.sign] || {}).needNames, why: `second symbol of a 2-symbols line: ${d.x.sign}, from the buyer's words “${clip(d.x.text, 40)}”; the line's own symbol is ${first.sign}` }));
          if (designs.every(d => d.sku) && designs[0].sku === designs[1].sku) rep.why = `the line says two different designs (${sl.signs.map(x => x.sign).join(" and ")}) but both signs are the same charm (${designs[0].sku}): ${note}. Name the second design, or say it is the same on both ears`;
          else if (!rep.asks.length) rep.why = `the line says two different designs (${sl.signs.map(x => x.sign).join(" and ")}): “${designs.filter(d => !d.sku).map(d => d.x.sign).join(", ")}” has no charm chosen on this listing yet (${note})`;
          else rep.needNames = rep.asks.some(a => a.needNames);
        }
      }
    }
    return null;
  }
  /** What a line is as a group of pieces. line: the sorter's line (or a raw Etsy transaction). o: { spec (form), sku, entry (the master index
   *  entry of the design), members (pairMembers), optionMaps, count (a countRead already made) }.
   *  mismatched is a fact about the DESIGN or two named designs (never about words alone: a line whose note says "mismatched" while its SKU is
   *  one ordinary design has says:true and mismatched:false, and is made as a matching pair, with a note saying so).
   *  → { earring (an earring pair line: a Left and a Right per unit), single, soldAs, mismatched, source, members, says, signals, discs, perUnit,
   *      glued (a mismatched design counted as one glued copy per unit until the pool cuts a piece per ear), split, sideSaid, count, asks[], notes[] } */
  function pairInfo(line, o) {
    o = o || {}; const sig = lineSignals(line), form0 = o.form !== undefined ? o.form : o.spec && o.spec.form, e = o.entry, dp = e && e.pair && typeof e.pair === "object" ? e.pair : null;
    const cr = o.count || countRead(line, { optionMaps: o.optionMaps });
    // a wording that is not guessed ("huggie" in a necklace title, "Pair of ... charms"): a person's answer, kept for the listing's title, is a form (earrings / huggie) or "not earrings" (ignore); no answer = a question
    const askWords = sig.ask && !form0 ? sig.ask : null, ansWords = askWords && o.optionMaps ? optionLookup(o.optionMaps, line.listingId, EARS_OPT, askWords.value) : null;
    const earForm = ansWords && ansWords.field === "form" && (ansWords.value === "earrings" || ansWords.value === "huggie") ? ansWords.value : null, form = form0 || earForm || form0;
    // the option or title that says Single/Pair is the buyer's choice and wins; then the form the options gave; then the title's own words
    const soldAs = sig.soldBy === "option" ? sig.soldAs : form === "earring-single" ? "single" : (form === "earrings" || form === "huggie") ? "pair" : form ? null : sig.soldAs;
    const info = { earring: false, single: soldAs === "single", soldAs, mismatched: false, source: null, members: null, says: sig.says, signals: sig.signals, discs: sig.discs, perUnit: 1, glued: false, split: PIECE_RULES.mismatchedMakesTwo, sideSaid: soldAs === "single" ? sig.side : null, sideBy: soldAs === "single" && sig.side ? sig.sideBy : null, count: null, asks: [], notes: [] };
    if (dp && +dp.bodies > 1 && dp.mismatched) { info.mismatched = true; info.source = "design"; }
    else if (!dp && e && MISMATCH_NAME.test(upSku(o.sku))) { info.mismatched = true; info.source = "name"; }   // until the catalogue carries `pair`: MISMATCHED, MISMATCHED_6849, MISMATCHED_7134
    else if (o.members) { info.mismatched = true; info.source = o.members.source; info.members = o.members.members; }
    /* A design (or two named designs) that draws two bodies is a Left and a Right ONLY on a line sold as earrings (Paul, 10 Oct 2026: only earring pairs are Left and Right,
       the Right the exact mirror of the Left; necklaces, pendants, bracelets, key rings, charm-only lines and singles are never split into ears or mirrored). The line's own
       words or form say earrings (soldAs "pair": earrings, studs, huggies, hoops, a Pair option, form earrings or huggie); a SKU the shop itself names MISMATCHED says it too,
       unless the line chose another form or names another product. Anything else is NOT a pair: one piece per unit cut from the design's file exactly as the app always read it
       (the whole drawing, both bodies together, no side, no mirror) and a plain note says so. A line sold as Single is unchanged (one ear, its side only when it names one). */
    info.earring = soldAs === "pair" || (info.mismatched && info.source === "name" && soldAs == null && !form && !sig.other);
    if (info.mismatched && !info.earring && soldAs !== "single") {
      const named = info.source !== "design" && info.source !== "name";   // (two designs the line names, not one design file that draws two bodies)
      info.mismatched = false; info.source = null; info.members = null; if (named) info.twoNamed = true; else info.twoBodies = true;
    }
    if (earForm) info.earForm = earForm;
    if (askWords && !earForm && !(ansWords && ansWords.field === "ignore")) info.asks.push({ name: EARS_OPT, value: askWords.value, guess: 0, why: askWords.why, rule: "ear:words" });
    // a line that says two different designs but names ONE waits for a person (never guessed): the second design, or "the same on both ears" (the answer under SECOND_OPT)
    if (sig.says && !info.mismatched && info.earring) {
      const ans = o.lineId ? optionLookup(o.optionMaps, line.listingId, SECOND_OPT, o.lineId) : null, same = !!ans && ans.field === "ignore", rep = o.report || {};
      // (two signs named but one has no charm yet: the question is that option value's, asked like every option value; else one plain line of what is missing)
      const viaOption = !!(rep.asks && rep.asks.length);
      // (the buyer's note names the same sign as the drop-down, or repeats it: both ears are that sign, a person is not asked)
      info.second = same ? { answered: "same" } : rep.same ? { answered: "same", by: "note", sign: rep.same.sign, why: rep.same.why }
        : { answered: "", why: viaOption ? rep.asks[0].why : rep.why || `the line says two different designs (${sig.signals[0] || "its words"}) but names one: name the second, or say it is the same on both ears`, viaOption, needNames: !!rep.needNames };
    }
    if (cr.n > 1 || cr.answered) info.count = { n: cr.n, answered: cr.answered, rule: (cr.opts.find(c => c.certain && c.n === cr.n) || {}).rule || "", from: (cr.opts.find(c => c.certain && c.n === cr.n) || {}).name || "" };
    // the questions: what the options cannot settle by themselves
    for (const a of cr.asks) if (!(a.rule === "count:note-only" && (info.earring || info.single))) info.asks.push(a);   // (a note on an earring line is no count of pieces)
    if (!cr.answered) {
      const piece = cr.opts.find(c => c.certain && c.n > 1 && c.cls === "piece");
      if (info.earring && piece && !info.asks.length) info.asks.push({ name: piece.name, value: piece.value, guess: 0, why: `an earring line names ${piece.n} ${piece.unit}: is that ${piece.n} pieces on each earring?`, rule: "count:earring" });
      const des = cr.opts.find(c => c.cls === "design" && c.n > 1 && !c.answered);
      if (des && !info.earring && !info.asks.length) info.asks.push({ name: des.name, value: des.value, guess: des.n, why: `“${clip(des.name, 40)}: ${clip(des.value, 40)}” names ${des.n} ${des.unit}: does each make a piece?`, rule: "count:designs" });
    }
    // pieces per unit
    if (cr.answered) info.perUnit = cr.n === 1 && info.earring && !info.single ? (info.mismatched && !PIECE_RULES.mismatchedMakesTwo ? 1 : PIECE_RULES.pairFormsMakeTwo ? 2 : 1) : cr.n;   // ("Just 1" on an earring line says the option is no count: the pair stays a Left and a Right)
    else if (info.mismatched && info.earring) { if (PIECE_RULES.mismatchedMakesTwo) info.perUnit = 2; else info.glued = true; }
    else if (info.earring && PIECE_RULES.pairFormsMakeTwo) info.perUnit = 2;
    else if (!info.earring && !info.single && PIECE_RULES.optionCountsMake && cr.certain && cr.n > 1) info.perUnit = cr.n;
    // what a person should see, plainly
    if (info.asks.some(a => a.rule === "ear:words")) info.notes.push("it waits until a person says whether this is a pair of earrings (" + askWords.why + ")");
    else if (earForm) info.notes.push("a person said this listing is " + (earForm === "huggie" ? "huggie hoops" : "a pair of earrings"));
    if (info.single && !info.sideSaid) info.notes.push("single earring: the line does not say left or right");
    else if (info.single && info.sideBy === "note" && +line.quantity > 1) info.notes.push(`${Math.round(+line.quantity)} single earrings and the buyer's note names one ear: which of them is which ear is not guessed`);
    if (info.second) info.notes.push(info.second.answered === "same" ? (info.second.by === "note" ? `the line says two different designs: ${info.second.why}` : "the line says two different designs: a person said the same design on both ears") : info.second.viaOption ? `waits for the second symbol's charm: ${info.second.why}` : "the line says two different designs but names one: it waits until a person names the second design or says it is the same on both ears");
    else if (sig.says && !info.mismatched) info.notes.push("the line says two different designs but is not an earring pair: nothing is changed");
    if (info.mismatched && info.source === "alias") info.notes.push("the Left and the Right are the two designs a person saved for this listing and SKU");
    if (info.twoBodies) info.notes.push("the design draws two bodies (a left and a right charm) but the line is not an earring pair: made as one piece with both bodies, no Left or Right, nothing mirrored");
    else if (info.twoNamed) info.notes.push("the line names two designs but is not an earring pair: made as one piece of the first design, no Left or Right");
    if (cr.note && !cr.answered && !(cr.certain && cr.n === cr.note.n) && !info.asks.length && !info.earring) info.notes.push(`the buyer's note says “${clip(cr.note.text, 30)}” but no option gives that count: made as ${info.perUnit}`);
    return info;
  }
  const unitsOf = x => { const s = x && x.spec, l = x && x.line; return Math.max(1, Math.round(+(s && s.quantity) || +(x && x.quantity) || +(l && l.quantity) || 1)); };
  const perUnitOf = p => Math.max(1, Math.floor(+(p && p.perUnit)) || 1);
  /** The form a RAW line's options choose (necklace, earrings, huggie, charm...), by the deterministic rules interpretLine applies to a line with no
   *  learned answers: a Huggie CHARM SET value, the default option map, a form word in a Type / Style / chain option. null when none. */
  function formOfLine(line) {
    for (const v of (line && line.variations) || []) {
      const name = lvName(v), value = lvValue(v).trim(); if (!name || !value || isMetalOption(name) || isPersonalisation(name)) continue;
      if (HUGGIE_SET.test(value)) return "huggie";
      const hit = optionLookup(null, line.listingId, name, value); if (hit && hit.field === "form") return hit.value;
      const byName = isChainOption(name) || isFormOption(name) || isEarFormOption(name), asForm = byName ? formByWords(value) : EAR_NEG_TEST.test(value) && formByWords(value) === "charm" ? "charm" : null;   // ("Charm only (no hoops)" is the loose charm under any name)
      if (asForm) return asForm;
    }
    return null;
  }
  /** the pair facts of anything that is a line (a spec, a row, a raw Etsy line): the spec's own when interpretLine made it, else read from the words */
  function pairOf(x) {
    if (x && x.spec && x.spec.pair) return x.spec.pair;
    if (x && x.pair && typeof x.pair === "object") return x.pair;
    const line = (x && x.line) || x || {}, sku = String(line.sku || "");
    return pairInfo(line, { form: x && x.spec ? x.spec.form : formOfLine(line), sku, entry: MISMATCH_NAME.test(upSku(sku)) ? {} : null });
  }
  /** How many pieces a line makes: THE count every caller reads. x: a line spec (interpretLine), a row { spec, line, poolIds }, or a raw
   *  Etsy line / transaction. An explicit x.pieceCount or spec.pieceCount wins (Orders pins it to the pieces an old line already has).
   *  Else the units bought times the pieces per unit the line's words and options give. */
  function pieceCountOf(x) {
    if (!x || typeof x !== "object") return 1;
    const own = Math.floor(+x.pieceCount || +(x.spec && x.spec.pieceCount) || 0);
    if (own > 0) return own;
    return unitsOf(x) * perUnitOf(pairOf(x));
  }
  /** The pieces of a line in order: [{ n, of, unit, side, bodyIndex }] (flat). An earring pair is a Left then a Right for every unit (L R L R),
   *  matching or mismatched (bodyIndex 0 then 1 for a mismatched design's own bodies); a single earring has the side its line names, or null;
   *  a glued mismatched copy, discs, letters and charms: no side. A piece an old line already has carries no side. */
  function piecesOf(x) {
    const p = pairOf(x), total = pieceCountOf(x), units = unitsOf(x), per = Math.max(1, Math.round(total / units)), natural = total === units * perUnitOf(p), out = [];
    for (let i = 0; i < total; i++) {
      let side = null, bodyIndex = 0;
      if (natural && p.earring && !p.glued && per % 2 === 0) { side = i % 2 === 0 ? "L" : "R"; if (p.mismatched) bodyIndex = i % 2; }
      else if (natural && p.single && (units === 1 || p.sideBy === "listing")) side = p.sideSaid || null;
      out.push({ n: i + 1, of: total, unit: Math.floor(i / per) + 1, side, bodyIndex });
    }
    return out;
  }
  const sidesOf = x => piecesOf(x).map(p => p.side);
  /** A mismatched pair whose two bodies the pool could not tell apart is made as the ONE glued copy per unit the app always made (both ears in one
   *  copy, no side), never as two copies of the folded charm. Sets the count, the sides and the kind, and says so in spec.pair.notes. */
  function glue(spec) {
    if (!spec || !spec.pair || !spec.pair.mismatched || spec.pair.glued) return spec;
    spec.pair.glued = true; spec.pair.perUnit = 1; spec.pair.split = false; delete spec.pieceRule; delete spec.pieceNote;
    spec.pieceCount = unitsOf(spec); spec.pair.sides = sidesOf(spec); spec.pair.kind = kindFor(spec.pair, spec.pieceCount);
    spec.pair.notes.push("made as one glued piece for each unit (the pool could not cut its two bodies apart)");
    return spec;
  }
  /** single | pair | mismatched | multi, as CharmNestPair.kindOf says it, from the pair facts and the number of pieces */
  const kindFor = (p, n) => {
    if (!p || n < 2) return p && p.mismatched ? "mismatched" : "single";                  // (one glued copy of a mismatched design is still the mismatched kind)
    if (p.earring && !p.glued && !p.legacy && n === 2) return p.mismatched ? "mismatched" : "pair";
    return "multi";                                                                      // 2 discs, 2 singles, a pinned old line, several pairs: pieces of one line, not one earring pair
  };
  /** An old line keeps the pieces it already has: spec.pieceCount becomes the number of pool ids, the count the rule gives is kept in
   *  spec.pieceRule, and a shortfall (or a surplus) is said in plain words in spec.pieceNote. Nothing is added to or taken from the pool. */
  function pinPieces(spec, have) {
    have = Math.floor(+have) || 0; if (!spec || have < 1) return spec;
    const rule = spec.pieceRule || spec.pieceCount || pieceCountOf(spec);
    if (have === rule) { delete spec.pieceRule; delete spec.pieceNote; spec.pieceCount = have; if (spec.pair) { delete spec.pair.legacy; spec.pair.sides = sidesOf(spec); spec.pair.kind = kindFor(spec.pair, have); } return spec; }
    spec.pieceRule = rule; spec.pieceCount = have; if (spec.pair) spec.pair.legacy = true;
    spec.pieceNote = have < rule ? `pooled as ${have} piece${have === 1 ? "" : "s"} before the pair and count rule; the rule now gives ${rule}. Left as it was: tell a person if the other${rule - have === 1 ? "" : "s"} must be cut`
      : `pooled as ${have} pieces; the rule now gives ${rule}. Left as it was`;
    if (spec.pair) { spec.pair.sides = Array.from({ length: have }, () => null); spec.pair.kind = kindFor(spec.pair, have); }
    return spec;
  }

  /* ═══ 4 · the line spec ════════════════════════════════════════════════ */
  /**
   * order: the bridge order (design §4.4); line: one of its lines
   * ctx: { optionMaps, aliases, noDesign, masterEntry?: (sku) => entry|null }
   * → { designSku, skuSource, material, materialKey, materialLabel, form, size, chain, quantity, personalization[],
   *     buyerMessage, staffNote, messages[], updateTs, options[], problems[], noDesign, engraveCandidate, sources }
   */
  function interpretLine(order, line, ctx) {
    ctx = ctx || {};
    const problems = [];
    const me = ctx.masterEntry, loose = ctx.masterLoose;
    // 0 · the SKU Etsy keeps for the product bought, in the listing's inventory table (when the page holds one): it stands in
    // for a transaction SKU that is missing or names no design, and for one that is the listing's own (held by products that
    // differ in their options) while the product has a SKU of its own. Never for a SKU that names a design and is no one
    // else's, and a product SKU no master file holds is taken only over a listing's own SKU that names no design (the person
    // is then asked about the product's SKU, not the listing's, which every other option of the listing shares).
    const table = ctx.listingSkus && ctx.listingSkus[String(line.listingId)], listing = inventorySku(line, table);
    let viaInventory = null, heldFamily = null;
    if (listing && listing.sku) {
      const tx = upSku(line.sku), names = s => !!s && (isNoDesign(s, ctx.noDesign) || !!masterSku(s, me, loose) || !!spelling(variationBase(s), me, loose));
      // wide: the transaction came with the listing's own SKU (several products that differ in their options hold it), not the product's
      const wide = listing.by !== "listing" && !!tx && listingWide(table, tx);
      // old: the transaction's SKU names a design (often the listing's umbrella design: "GREEK GODDESS" holds one symbol's file) but no
      // product of the listing carries it any more, while the product bought has a SKU of its own that is tied to an option nobody has
      // answered. That option's own SKU decides (Paul, 9 Oct); a line that already resolves (an answered option, a SKU that is some
      // product's own) is never read differently.
      const old = !!tx && listing.by !== "listing" && Array.isArray(table.products) && names(tx) && !table.products.some(p => p && !p.d && upSku(p.sku) === tx) && !isNoDesign(listing.sku, ctx.noDesign) &&
        (line.variations || []).some(v => { const nm = v.name || v.formatted_name, val = String(v.value != null ? v.value : v.formatted_value || "").replace(/&quot;/g, "\"").trim();
          return !!nm && !!val && !isMetalOption(nm) && !isPersonalisation(nm) && !optionLookup(ctx.optionMaps, line.listingId, nm, val) && tiesToOption(table, v, listing.sku); });
      if (listing.sku !== tx && (names(listing.sku) ? (!tx || !names(tx) || wide || old) : (wide && !names(tx)))) {
        // Etsy's SKU for the option must also be the right kind of design for the line. A necklace SKU on an earrings listing is read as its
        // earring twin only when the whole listing's SKUs have one (listingTwin) and nobody has picked a charm for the option; else the line waits
        if (skuFamilyConflict(masterSku(listing.sku, me, loose) || listing.sku, line)) {
          const twin = optionDesign(line, ctx.optionMaps) ? "" : listingTwin(line, table, listing.sku, me, loose);
          if (twin) { viaInventory = { tx, sku: twin, by: listing.by, etsy: upSku(listing.sku), twin: true }; line = Object.assign({}, line, { sku: twin }); }
          else heldFamily = { tx, sku: listing.sku, by: listing.by };
        } else { viaInventory = { tx, sku: listing.sku, by: listing.by }; line = Object.assign({}, line, { sku: listing.sku }); }
      }
    }
    // the SKU as bought: a charm-only listing's Huggie CHARM SET is its SKU's huggie design
    const set = huggieSet(line), bought = set ? huggieSku(line.sku, me) : String(line.sku || "").trim().toUpperCase();
    let { sku, source: skuSource } = resolveSku(set ? Object.assign({}, line, { sku: bought }) : line, ctx.aliases, me, ctx.noDesign, loose);
    if (viaInventory && skuSource !== "alias") skuSource = "inventory";
    // the SKU of the variation bought, when it is a design's own and the inventory ties it to an option's value, is that
    // option's answer: the option is not asked, and no charm a person once picked for it stands over the SKU
    const sold = viaInventory && viaInventory.etsy || upSku(line.sku), named = !!(sku && me && me(sku)), tied = v => named && tiesToOption(table, v, sold);
    // an option that picks the charm (a person's answer for this listing) wins over the SKU the variations share
    const picked = optionDesign(line, ctx.optionMaps), viaPick = !!picked && !tied(picked.variation);
    if (viaPick) { sku = picked.sku; skuSource = "option"; }
    // two designs named by the line (a SKU that names two, or options for the left and the right earring), both in the master: the pair's
    // members. Pooled as those two only once the pool makes a piece for each (PIECE_RULES.mismatchedMakesTwo); until then the line is
    // read and held exactly as before, and the members are only told in spec.pair.
    const secondId = order && line && line.transactionId != null ? String(order.receiptId) + "/" + String(line.transactionId) : "";
    // the charm a value of an option decides on this listing (a person's answer for it, or the SKU Etsy ties to that value alone), "" when none yet
    // (a sign read from the line's words, the buyer's note included, has no SKU on the transaction: its charm is the one the listing's own inventory keeps for that value, signValueSku; what stops it is told in report.lack)
    const report = {};
    const designOf = v => {
      const value = String(v.value != null ? v.value : v.formatted_value || "").replace(/&quot;/g, "\"").trim(), name = v.name || v.formatted_name;
      const hit = optionLookup(ctx.optionMaps, line.listingId, name, value);
      if (hit && hit.field === "design" && hit.value) return upSku(hit.value);
      if (!viaPick && tied(v)) return sku;
      const ss = signsOfText(value); if (ss.length !== 1) return "";
      const r = signValueSku(table, line, name, idOf(v.propertyId != null ? v.propertyId : v.property_id), ss[0], me, loose);
      if (r.sku) return r.sku;
      (report.lack = report.lack || {})[ss[0]] = { why: r.why, needNames: !!r.needNames };
      return "";
    };
    const members = pairMembers(line, { me, loose, optionMaps: ctx.optionMaps, aliases: ctx.aliases, sku, lineId: secondId, report, designOf });
    if (members && PIECE_RULES.mismatchedMakesTwo) { sku = members.members[0].sku; skuSource = "pair"; }
    const listed = isNoDesign(sku, ctx.noDesign) || (!sku && isNoDesign(set ? line.title + " (HUGGIE)" : line.title, ctx.noDesign));
    // a special purchase (custom, rework, chain only, add-on…): chain only is never cut, and a special line a person
    // finished by hand (its QR label printed from Custom Orders) is done; either reads as the no-design list does
    const lk = lineKey(order, line);
    const alias = ctx.aliases && ctx.aliases[String(line.listingId)];
    const special = specialOf(line, { sku, masterEntry: ctx.masterEntry, optionMaps: ctx.optionMaps, read: ctx.customRead && ctx.customRead[lk], decided: ctx.customDecided && ctx.customDecided[lk], listingKind: alias && alias.listingKind });
    const done = (ctx.customDone && ctx.customDone[lk]) || null;
    const noDesign = listed || !!(special && special.notCut) || !!done;
    // an add-on listing's line (specialOf: own): its design is the order's own, never the master design its SKU happens to match. It is not "no design" (nothing cuts it and
    // nothing closes its order until its own designs are sent to a sheet or a person completes it), and nothing is asked about it: no metal, SKU or option question, no engraving
    const own = !!(special && special.own && !special.notCut);
    const quiet = noDesign || own;
    const metalKey = String(line.metalKey || "");
    const material = METAL_TO_CARD[metalKey] || null;
    if (!quiet && !material) problems.push({ kind: "needsMaterial", metalKey: metalKey || null, metalLabel: line.metalLabel || "", listingId: String(line.listingId || ""), options: (line.variations || []).map(v => `${v.name}: ${v.value}`), title: line.title || "" });
    const spec = { designSku: sku || null, skuSource, material, materialKey: metalKey || null, materialLabel: line.metalLabel || (material ? CARD_LABEL[material] : ""), form: null, size: null, chain: null, quantity: Math.max(1, Math.round(+line.quantity || 1)), personalization: (line.personalization || []).map(s => visible(s).trim()).filter(Boolean), buyerMessage: visible(line.buyerMessage || order.buyerMessage || ""), staffNote: String(line.staffNote || order.staffNote || ""), messages: (line.messages || order.messages || []).slice(-5), updateTs: +order.updateTs || 0, options: [], problems, noDesign, sources: { material: "station classifier (Metal/Colour option first)", sku: skuSource } };
    if (special) spec.special = special;
    if (done) spec.customDone = done;
    spec.boughtSku = bought; if (set) spec.huggieSet = true;
    if (viaInventory) spec.viaInventory = viaInventory;   // the transaction's SKU and the listing inventory's SKU for the product bought, when that one was used
    if (heldFamily) spec.inventoryHeld = heldFamily;      // the inventory's SKU for the product bought is another family's design than the line: not used, a person decides
    // a line with no design of its own (not on the no-design list, not finished by hand) is one Claude reads to tell a
    // custom order from a regular listing whose SKU is not indexed yet (Custom Orders)
    const byOption = special && special.signals[0] === "option";
    spec.readable = !listed && !done && !byOption && !own && !set && !(sku && ctx.masterEntry && ctx.masterEntry(sku)) && !!ctx.masterEntry;
    if (own) spec.ownDesign = true;
    if (noDesign) spec.noDesignWhy = done ? "completed by hand (Custom Orders)" : listed ? "on the no-design list" : special.label.toLowerCase() + " · not laser cut";
    // the options that name how many pieces ONE unit makes (2 Disc, Number of Discs: 3, a person's answer): read once, for the options below and for the count
    const cr = countRead(line, { optionMaps: ctx.optionMaps });
    // what a person is told about an unmapped option: why Etsy's own SKU did not settle it, and the master SKUs the inventory offers for it
    // (once per line; the same for each of its unmapped options, which share the product bought)
    let help = null;
    const optionHelp = () => {
      if (!help) {
        const why = inventoryWhy(line, table, !!ctx.listingSkus, me, loose), picks = inventoryPicks(line, table, me, loose);
        help = Object.assign({}, why ? { why } : {}, picks.length ? { picks } : {});
      }
      return help;
    };
    for (const v of line.variations || []) {
      const name = v.name || v.formatted_name, value = String(v.value != null ? v.value : v.formatted_value || "").replace(/&quot;/g, "\"").trim();
      if (!name || !value || isMetalOption(name) || isPersonalisation(name)) continue;
      const hit = optionLookup(ctx.optionMaps, line.listingId, name, value);
      let mapped = hit ? { field: hit.field, value: hit.value, source: hit.source } : null;
      // a font option (Font: Stylish, Fonts: 16"/ Typewriter): read once; a person's answer for the FONT alone ("typewriter") serves every length it comes with
      const fr = fontRead(name, value);
      if (!mapped && fr && fr.asked !== value) { const h = optionLookup(ctx.optionMaps, line.listingId, name, fr.asked); if (h) mapped = { field: h.field, value: h.value, source: h.source }; }
      // a Left-ear / Right-ear option whose value is a master design is that side's design (the pair's members): nothing to ask about it
      if (!mapped && members && members.source === "options" && SIDE_THING.test(name)) { const k = /\bleft\b/i.test(name) ? "L" : /\bright\b/i.test(name) ? "R" : "", m = k && members.members.find(x => x.side === k); if (m) mapped = { field: "design", value: m.sku, source: "rule:pair-side" }; }
      if (!mapped) {                                                            // deterministic name rules, no free-text reading
        const asForm = formByWords(value);
        if (HUGGIE_SET.test(value)) mapped = { field: "form", value: "huggie", source: "rule:huggie-set" };
        else if (isSizeOption(name) && looksLikeSize(value)) mapped = { field: "size", value: SIZE_VALUES[bareValue(value)] || bareValue(value).toUpperCase().replace(/\s+/g, ""), source: "rule:size" };
        else if (isChainOption(name) && looksLikeLength(value)) mapped = { field: "chain", value, source: "rule:length" };
        else if (isChainOption(name) && asForm) mapped = { field: "form", value: asForm, source: "rule:form" };
        else if (isFormOption(name) && looksLikeLength(value)) mapped = { field: "chain", value, source: "rule:length" };
        else if (isFormOption(name) && asForm) mapped = { field: "form", value: asForm, source: "rule:form" };
        else if (isEarFormOption(name) && asForm) mapped = { field: "form", value: asForm, source: "rule:form" };
        else if (asForm === "charm" && EAR_NEG_TEST.test(value)) mapped = { field: "form", value: "charm", source: "rule:charm-only" };   // "Charm only (no hoops)", under any name
        else if (isPriceOption(name) && looksLikePrice(value)) mapped = { field: "ignore", value: null, source: "rule:price" };
        // the SKU is this value's own (the inventory never gives it to another value of the option) and a design: it answers the option
        else if (!viaPick && tied(v)) mapped = { field: "design", value: sku, source: "sku" };
        // a font option names an app font exactly: it answers itself (never a look-alike); any other font waits below with its one plain line, unless FONT_RULES lets it through
        else if (fr && fr.id) mapped = { field: "font", value: fr.id, source: "rule:font" };
        else if (fr && !FONT_RULES.unknownHolds) mapped = { field: "font", value: null, source: "rule:font-asked" };
        // an option that names a number of separate pieces answers itself ("ROSEGOLD - 2 Disc"); one that may, but does not say of what
        // (letters, a range, "Set of 3"), is asked about below, once, in its own plain words, not as a generic unmapped option
        const cx = cr.opts.find(c => c.name === name && c.value === value);
        if (!mapped && cx && cx.ask) { spec.options.push({ name, value, mapped: null }); continue; }
        if (!mapped && cx && cx.certain && cx.n > 1 && cx.cls !== "design") mapped = { field: "count", value: String(cx.n), source: "rule:" + cx.rule };
        // "GOLD - 1 Disc" (one of the 15 choices of the 180-product disc listing) answers itself as ONE piece too: it used to fall to the generic "not mapped" hold (DISCREAD)
        else if (!mapped && cx && cx.certain && cx.n === 1 && cx.cls === "piece") mapped = { field: "count", value: "1", source: "rule:" + cx.rule };
      }
      spec.options.push({ name, value, mapped });
      // (an unmapped font option is asked about the FONT: the question and its answer carry the font alone, the length and alignment are read apart)
      if (!mapped) { if (!quiet) problems.push(fr ? { kind: "needsMapping", listingId: String(line.listingId || ""), optionName: name, optionValue: fr.asked, title: line.title || "", font: { asked: fr.asked, chain: fr.chain, align: fr.align, raw: value, pick: fr.pick, why: fontWhy(fr) } } : Object.assign({ kind: "needsMapping", listingId: String(line.listingId || ""), optionName: name, optionValue: value, title: line.title || "" }, optionHelp())); continue; }
      if (mapped.field === "font") {   // the font the buyer asked for and the app font it is (none: asked only); the length it came with is the line's chain
        const af = fontById(mapped.value);
        if (!spec.font) spec.font = { asked: fr ? fr.asked : value, id: af ? af.id : "", name: af ? af.name : "", source: mapped.source };
        if (fr && fr.chain && !spec.chain) spec.chain = fr.chain;
        continue;
      }
      if (mapped.field === "ignore" || mapped.field === "design" || mapped.field === "count") continue;
      if (mapped.field === "form" && !spec.form) spec.form = mapped.value;
      else if (mapped.field === "size" && !spec.size) spec.size = mapped.value;
      else if (mapped.field === "chain" && !spec.chain) spec.chain = mapped.value;
    }
    // an unknown SKU waits while an option is unanswered: the option may be what picks the charm (Zodiac Sign: Pisces on a
    // listing whose signs share one SKU), and a charm given to the SKU instead would be every sign's
    const optionOpen = problems.some(p => p.kind === "needsMapping");
    if (!quiet && !sku && !optionOpen) problems.push(set ? { kind: "unmatchedSku", reason: "no SKU on the transaction · Huggie CHARM SET: pick its huggie design", huggie: true, listingId: String(line.listingId || ""), title: line.title || "" }
      : { kind: "unmatchedSku", reason: "no SKU on the transaction and no alias for the listing", listingId: String(line.listingId || ""), title: line.title || "" });
    if (ctx.masterEntry && sku && !quiet) {
      const entry = ctx.masterEntry(sku);
      if (!entry) { if (!optionOpen) problems.push({ kind: "unmatchedSku", reason: set && skuSource === "transaction" ? "Huggie CHARM SET: its huggie design is not in any master file" : "not in any master file", sku, listingId: String(line.listingId || ""), title: line.title || "" }); }
      else if (entry.blocked) problems.push({ kind: "blockedSku", reason: entry.blocked, sku });
      else if (entry.sizes && Object.keys(entry.sizes).length) { if (!spec.size || !entry.sizes[spec.size]) problems.push({ kind: "missingSize", sku, size: spec.size, available: Object.keys(entry.sizes) }); }
    }
    // pairs (Paul, 9 Oct): what the line says about being a pair, and the one count of its pieces (pieceCountOf: every caller reads it)
    // a 2-symbols line whose second symbol (from the buyer's words) has no charm yet: that value of the option is asked once, like any option value
    if (!quiet && !members) for (const a of report.asks || []) if (!problems.some(p => p.kind === "needsMapping" && p.optionName === a.name && p.optionValue === a.value)) problems.push(Object.assign({ kind: "needsMapping", listingId: String(line.listingId || ""), optionName: a.name, optionValue: a.value, title: line.title || "", note: a.why }, a.lack ? { why: a.lack } : {}));
    spec.pair = pairInfo(line, { spec, sku, entry: me && sku ? me(sku) : null, members, optionMaps: ctx.optionMaps, count: cr, lineId: secondId, report });
    spec.pieceCount = pieceCountOf(spec);
    spec.pair.kind = kindFor(spec.pair, spec.pieceCount);
    spec.pair.sides = sidesOf(spec);
    // a count the options cannot settle waits for a person: one plain question per option, like an unmatched SKU
    if (!quiet) for (const a of spec.pair.asks) if (!problems.some(p => p.kind === "needsMapping" && p.optionName === a.name && p.optionValue === a.value && (p.count || p.earWords))) problems.push(Object.assign({ kind: "needsMapping", listingId: String(line.listingId || ""), optionName: a.name, optionValue: a.value, title: line.title || "" }, a.rule === "ear:words" ? { earWords: { why: a.why } } : { count: { guess: a.guess || 0, why: a.why, rule: a.rule } }));
    // a person said this listing's words are a pair of earrings: the form it is (what an answered option does too)
    if (!spec.form && spec.pair.earForm) spec.form = spec.pair.earForm;
    // a line that says two different designs but names one waits: the second design, or "the same on both ears" (one plain question, like an unmatched SKU)
    if (!quiet && spec.pair.second && !spec.pair.second.answered && !spec.pair.second.viaOption && secondId) problems.push({ kind: "needsMapping", listingId: String(line.listingId || ""), optionName: SECOND_OPT, optionValue: secondId, title: line.title || "", pair: { second: true }, pairSecond: { why: spec.pair.second.why } });
    spec.engraveCandidate = !quiet && (spec.personalization.length > 0 || !!spec.buyerMessage.trim() || !!spec.staffNote.trim() || spec.messages.some(m => engravingNote(m && m.text)));
    return spec;
  }
  /** A line held (or unmatched) because of a question its fresh reading no longer raises: true when it may go back to the intake. `prev` is the spec it was read as before (the question it
   *  held for); the row must have no person's hold, no pieces made, and a clean reading now. DISCREAD: a stored or restored "held ... not mapped" kept its state after the option was read. */
  function questionGone(row, prev) {
    if (!row || !row.spec || !["held", "unmatched"].includes(row.state) || row.hold || (row.poolIds && row.poolIds.length) || row.fromRecord) return false;
    if (!prev || !Array.isArray(prev.problems) || !prev.problems.length) return false;
    return !(row.spec.problems && row.spec.problems.length) && !row.spec.noDesign && !row.spec.special && !(row.problems && row.problems.length);
  }
  /** The same for a line built from the run's stored record (the order window of an order outside the pull): its stored reason names a question, its fresh spec has none. */
  const QUESTION_REASON = /^(?:option "|SKU |needs material|no design for size|oversize for|not in any master file|font \u201c|the SKU \u201c|the buyer's note|Options:)/i;
  function staleQuestionHold(row) {
    if (!row || !row.spec || !["held", "unmatched"].includes(row.state) || row.hold || (row.poolIds && row.poolIds.length) || !QUESTION_REASON.test(String(row.reason || ""))) return false;
    return !(row.spec.problems && row.spec.problems.length) && !row.spec.noDesign && !row.spec.special;
  }
  const lineKey = (order, line) => `${order.receiptId}_${line.transactionId}`;
  const poolId = (order, line, copy) => `${order.receiptId}_${line.transactionId}_${copy}`;

  /* ═══ 5 · sets, dates, names ═══════════════════════════════════════════ */
  const MON = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  /** One local clock: the calendar day of the machine, never UTC. */
  const localDay = (d = new Date()) => `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
  /** "Sep.16.26" for the same local day. */
  const dateTag = (d = new Date()) => `${MON[d.getMonth()]}.${String(d.getDate()).padStart(2, "0")}.${String(d.getFullYear()).slice(-2)}`;
  const dateTagOfDay = day => { const [y, m, dd] = String(day).split("-").map(Number); return dateTag(new Date(y, m - 1, dd, 12)); };
  const completionDay = s => s.completionDay || (s.completedAt || s.committedAt ? localDay(new Date(s.completedAt || s.committedAt)) : s.day) || "";
  const completionTime = s => +(s.completedAt || s.committedAt) || (completionDay(s) ? new Date(completionDay(s) + "T12:00:00").getTime() : 0);
  const compareCompleted = (a,b) => completionDay(b).localeCompare(completionDay(a)) || completionTime(b)-completionTime(a) || (+b.seq || 0)-(+a.seq || 0);
  const completedTitle = s => `Complete Set #${s.seq || s.setSeq || String(s.setId || "").split("-").pop()}, ${completionDay(s) ? dateTagOfDay(completionDay(s)).replace(/\./g,"-") : "Date unavailable"}`;
  const setId = (day, seq) => `set-${day}-${seq}`;
  const setLabel = seq => `Set-${seq}`;
  const setFolder = (day, seq) => `charmnest/sets/${day}/Set-${seq}`;
  const sheetName = (card, day, seq, n) => `${CARD_TAG[card] || card}_${dateTagOfDay(day)}_Set-${seq}_Sheet-${n}`;
  const sheetFolder = (card, day, seq, n) => `${setFolder(day, seq)}/${sheetName(card, day, seq, n)}`;

  /* ═══ 6 · labels: byte-identical to the Design Station's encoder ═══════ */
  function toB36(numStr) {
    const clean = String(numStr).trim();
    if (!/^\d+$/.test(clean)) return clean;
    let n = BigInt(clean);
    if (n === 0n) return "0";
    const digits = "0123456789abcdefghijklmnopqrstuvwxyz";
    let out = "";
    while (n > 0n) { out = digits[Number(n % 36n)] + out; n = n / 36n; }
    return out;
  }
  function encodeOrderList(ids, metal) {
    const b36 = (ids || []).map(x => { const d = String(x).replace(/\D/g, ""); return d ? toB36(d) : String(x); });
    return `B36|${metal}|` + b36.join(".");
  }
  function safeChunks(arr, metal, maxChars = 1000, startSize = 50, minSize = 8) {
    const out = []; let i = 0, size = startSize;
    while (i < arr.length) {
      const slice = arr.slice(i, i + size);
      if (encodeOrderList(slice, metal).length > maxChars && size > minSize) { size = Math.max(minSize, Math.floor(size * 0.75)); continue; }
      out.push(slice); i += slice.length;
    }
    return out;
  }
  /** The station's metal key for a sorter card (labels carry the station's vocabulary: gold, silver, rose, 10k, 14k). */
  const CARD_TO_METAL = Object.fromEntries(Object.entries(METAL_TO_CARD).map(([k, v]) => [v, k]));

  /* ═══ 7 · holding an order whole ═══════════════════════════════════════ */
  /**
   * rows: this order's line rows { spec, state, reason, engrave: { needed, approved }, poolIds }
   * An order is committable only when every line is: nested and written; nested, written and its engraving approved;
   * or on the no-design list. Anything else holds the whole order with the first unresolved line's reason.
   */
  // Shop-local receipt dates, never the time a historical order was imported.
  function orderPlacedAt(row) { return (+row.order?.createTs || 0) * 1000 || +row.arrivedAt || 0; }
  /* The queue's one order (Paul, 5 Oct 2026: "When an order gets released from hold it must go back in queue and be placed on
     the next available placement ... ahead of the incoming orders from Etsy"). An order released from hold carries `frontAt`
     (the release time in ms) on its lines, pieces and pool rows. Every placement path puts what carries it first, the order
     released first first, and the rest as before: oldest order first. frontOf reads a line, a piece or a queue entry;
     byQueue(dateOf) is the comparator for lines (dateOf: their order date); rankDate(piece) is the date the nest's FIFO reads
     for a piece: a front piece ranks by its release time over a billion, which is a few minutes past 1970 in the solver's
     seconds, so it stays ahead of every real order date and in release order among its own kind. */
  const frontOf = x => { const v = +(x && (x.frontAt ?? (x.row && x.row.frontAt))); return v > 0 ? v : 0; };
  const byQueue = dateOf => (a, b) => { const fa = frontOf(a), fb = frontOf(b); if (fa || fb) { if (!fa) return 1; if (!fb) return -1; if (fa !== fb) return fa - fb; } return (dateOf(a) || 0) - (dateOf(b) || 0); };
  const rankDate = c => { const f = frontOf(c); return f ? f / 1e9 : +(c && c.orderDate) || 0; };
  // one set of formatters per time zone: making one costs far more than using it, and each Orders list drew three per line
  const dayFormats = new Map();
  function dayFormat(timeZone) {
    let f = dayFormats.get(timeZone);
    if (!f) dayFormats.set(timeZone, f = { parts: new Intl.DateTimeFormat('en-CA',{timeZone,year:'numeric',month:'2-digit',day:'2-digit'}), label: new Intl.DateTimeFormat('en-CA',{timeZone,weekday:'long',year:'numeric',month:'long',day:'numeric'}), time: new Intl.DateTimeFormat('en-CA',{timeZone,hour:'numeric',minute:'2-digit',timeZoneName:'short'}) });
    return f;
  }
  function orderDay(row, timeZone = "America/Toronto") {
    const at=orderPlacedAt(row);if(!at)return {key:"unknown",label:"Date unavailable",time:""};
    const f=dayFormat(timeZone),date=new Date(at),parts=f.parts.formatToParts(date);
    const get=k=>parts.find(p=>p.type===k).value;
    return {key:`${get('year')}-${get('month')}-${get('day')}`,label:f.label.format(date),time:f.time.format(date)};
  }
  function intakePlan({count, area=0, capacity=0, threshold=85, pressure=0.85, budgetS=180, force=false, density=0, target=.74, optimized=false, append=false}) {
    const targetMet=density+1e-6>=target;
    // Arrivals first try the saved gaps. Only the measured result can decide
    // whether that addition still needs the final unrestricted search.
    const full=!targetMet && !append && (force || count>=Math.max(1,threshold) || !optimized && capacity>0 && area>=capacity*pressure);
    const phase=full?(count>=Math.max(1,threshold)?'final':'repack'):'fill';
    return {phase,targetMet,budgetMs:Math.max(1000,full?budgetS*1000:Math.min(budgetS,12)*1000)};
  }
  const DONE_STATES = new Set(["written", "labelled", "committed"]);
  function evaluateOrder(rows) {
    const lines = rows.map(r => {
      if (r.changePending || r.hold) return {key:r.key,ok:false,why:r.reason || r.hold || "Etsy changes need review"};
      // completed by hand (Review → Complete Order, or its QR label printed: the custom order's own record, as CharmNestReadiness.isHand reads it) is resolved:
      // it needs no sheet and does not hold its order's set (a held one, above, still does)
      const cd = r.spec && r.spec.customDone;
      if (cd && cd.state !== "open" && cd.how !== "sheet") return { key: r.key, ok: true, why: "completed by hand (Custom Orders)" };
      if (r.spec && r.spec.noDesign) return { key: r.key, ok: true, why: "no design (chain/packaging)" };
      if (r.state === "gone") return { key: r.key, ok: false, why: "order gone from Etsy" };
      if (r.problems && r.problems.length) return { key: r.key, ok: false, why: r.problems[0].kind === "needsMaterial" ? "needs material" : r.problems[0].kind === "needsMapping" ? "needs an option mapped" : r.problems[0].kind === "unmatchedSku" ? "SKU not in a master" : r.problems[0].kind === "missingSize" ? "no design for that size" : r.problems[0].kind };
      if (!DONE_STATES.has(r.state)) return { key: r.key, ok: false, why: r.reason || `piece is ${r.state || "not nested"}` };
      if (r.engrave && r.engrave.needed && !r.engrave.approved) return { key: r.key, ok: false, why: r.engrave.state === "skipped" ? null : (r.engrave.state === "review" ? "engraving awaiting review" : r.engrave.state === "words" ? "engraving words need a decision" : r.engrave.reason || "engraving not approved") };
      return { key: r.key, ok: true, why: null };
    }).map(l => (l.why === null && !l.ok ? Object.assign(l, { ok: true }) : l));
    const first = lines.find(l => !l.ok);
    return { committable: !first, held: first ? { line: first.key, why: first.why } : null, lines };
  }

  /* ═══ 7b · what goes to the laser today, and what waits ═══════════════════════════════════════════════════════════
     A sheet costs the same to cut whether it carries ninety charms or nine, so a partial sheet is money and machine
     time thrown away. Two of the materials sell fast enough to fill a sheet most days; three do not, and could wait a
     week for a full one, which the customers who ordered them would not thank us for. So there are two regimes:

       fast (SS, GF 14/20)   — only full sheets go. A partial remainder waits for the next pull to top it up, unless a
                               piece on it is due or belongs to an order that is going anyway (below), in which case
                               the sheet is cut partial and filled as far as it can be.
       slow (RG, 10K, 14K)   — whatever there is goes, dates are not looked at, but only every `cadenceDays` days, so
                               the laser is not started for two pieces. A person can release a material early.

     And one rule over both: an order is never split across sets. An order with a 14K piece and a GF piece travels as
     one — so on a day 14K is closed, its GF piece waits too, and on the day 14K opens, its GF piece rides along even
     if that makes the GF sheet partial. Sets are then simply the groups of materials tied together by such orders:
     a material no order ties to another is a set of its own.

     This is a pure function of what it is given and is tested as one. It touches nothing. */
  const FAST_MATERIALS = new Set(["silver", "gold"]);
  const SLOW_MATERIALS = new Set(["rose", "gold10k", "gold14k"]);
  const dayDiff = (a, b) => Math.round((Date.parse(b + "T12:00:00Z") - Date.parse(a + "T12:00:00Z")) / 86400000);
  const addDays = (day, n) => new Date(Date.parse(day + "T12:00:00Z") + n * 86400000).toISOString().slice(0, 10);
  /**
   * lines: [{ key, orderId, material, areaPt2 (footprint incl. clearance), shipBy (unix s, 0 = unknown) }] — only lines
   *        that are otherwise ready to be pooled.
   * opts:  { today: "YYYY-MM-DD", capacity: { material: usablePt2 × ceiling }, lastReleased: { material: "YYYY-MM-DD" },
   *          released: { material: "YYYY-MM-DD" } (a person opened it that day), forceFill: { material: true } (a person
   *          said cut the partial), cadenceDays (2), lateDays (2) }
   * →      { take: Set(key), wait: Map(key → { kind, why, material, until, pct }), materials: { material → summary } }
   */
  function planRelease(lines, opts) {
    const today = opts.today, cadence = Math.max(1, +opts.cadenceDays || 2), lateDays = Math.max(0, opts.lateDays == null ? 2 : +opts.lateDays);
    const lastRel = opts.lastReleased || {}, released = opts.released || {}, forceFill = opts.forceFill || {}, cap = opts.capacity || {};
    const take = new Set(), wait = new Map(), materials = {};
    const dueSoon = l => l.shipBy > 0 && (l.shipBy * 1000 - Date.parse(today + "T00:00:00")) <= lateDays * 86400000;
    // 1 · is a slow material open today?
    const openSlow = {};
    for (const m of SLOW_MATERIALS) {
      const forced = released[m] === today;
      const last = lastRel[m] || null;
      const open = forced || !last || dayDiff(last, today) >= cadence;
      openSlow[m] = open;
      materials[m] = { regime: "slow", open, forced, lastReleased: last, next: open ? today : addDays(last, cadence), pieces: 0, taken: 0 };
    }
    for (const m of FAST_MATERIALS) materials[m] = { regime: "fast", pieces: 0, taken: 0, full: 0, pct: 0, partial: false, forcedBy: [] };
    // 2 · an order travels whole: it goes only if every slow material it touches is open
    const byOrder = new Map();
    for (const l of lines) { if (!byOrder.has(l.orderId)) byOrder.set(l.orderId, []); byOrder.get(l.orderId).push(l); }
    const gated = [];
    for (const [oid, ls] of byOrder) {
      const mats = new Set(ls.map(l => l.material));
      const closed = [...mats].filter(m => SLOW_MATERIALS.has(m) && !openSlow[m]);
      if (closed.length) { for (const l of ls) wait.set(l.key, { kind: "slow", material: closed[0], until: materials[closed[0]].next, why: `${closed[0]} opens ${materials[closed[0]].next}` }); continue; }
      for (const l of ls) gated.push(Object.assign({ multi: mats.size > 1, urgent: dueSoon(l) }, l));
    }
    for (const l of lines) if (materials[l.material]) materials[l.material].pieces++;
    // 3 · slow materials that are open: everything goes
    for (const l of gated) if (SLOW_MATERIALS.has(l.material)) { take.add(l.key); materials[l.material].taken++; }
    // 4 · fast materials: full sheets go; the remainder waits unless something on it has to travel
    for (const m of FAST_MATERIALS) {
      // (a line released from hold is never made to wait for a full sheet: it goes now, ahead of the others)
      const cand = gated.filter(l => l.material === m).sort((a, b) => byQueue(x => x.createTs || 0)(a, b) || (b.multi - a.multi) || (b.urgent - a.urgent) || ((a.shipBy || 1e12) - (b.shipBy || 1e12)));
      const usable = +cap[m] || 0, sum = materials[m];
      if (!cand.length) continue;
      if (!(usable > 0)) { cand.forEach(l => take.add(l.key)); sum.taken = cand.length; continue; }   // no plate known: never hold on a guess
      const total = cand.reduce((s, l) => s + (+l.areaPt2 || 0), 0);
      const full = Math.floor(total / usable); sum.full = full;
      const fullCap = full * usable;
      let acc = 0; const inFull = [], rest = [];
      for (const l of cand) { if (acc + (+l.areaPt2 || 0) <= fullCap + 1e-6) { acc += +l.areaPt2 || 0; inFull.push(l); } else rest.push(l); }
      inFull.forEach(l => take.add(l.key));
      const forced = rest.filter(l => l.multi || l.urgent);
      const restArea = rest.reduce((s, l) => s + (+l.areaPt2 || 0), 0);
      sum.pct = Math.round(Math.min(1, restArea / usable) * 100);
      if (forced.length || forceFill[m]) {
        // a sheet that has to be cut is filled as far as it will go: the whole remainder rides, up to one more sheet
        let acc2 = 0; for (const l of rest) { if (acc2 + (+l.areaPt2 || 0) <= usable + 1e-6) { acc2 += +l.areaPt2 || 0; take.add(l.key); } else wait.set(l.key, { kind: "fill", material: m, pct: sum.pct, why: `waits for a full ${m} sheet` }); }
        sum.partial = true; sum.forcedBy = forceFill[m] ? ["operator"] : [...new Set(forced.map(l => l.multi ? `order ${l.orderId} travels with another material` : `order ${l.orderId} is due`))];
      } else for (const l of rest) wait.set(l.key, { kind: "fill", material: m, pct: sum.pct, why: `waits for a full ${m} sheet · ${sum.pct}% so far` });
      for (const l of cand) if (frontOf(l)) { take.add(l.key); wait.delete(l.key); }
      sum.taken = cand.filter(l => take.has(l.key)).length;
    }
    return { take, wait, materials };
  }
  // Set membership is decided from the verified layout, never from “all queued pieces placed”.
  function sheetRelease(sheet, opts = {}) {
    if (!sheet.verified || !sheet.placed || sheet.stopped || sheet.dirty) return { include: false, reason: "Nest and verify first" };
    if (FAST_MATERIALS.has(sheet.material)) return sheet.full
      ? { include: true, reason: "Full sheet" } : { include: false, reason: sheet.topup ? (typeof sheet.topup === "object" ? `Filling its gaps · ${sheet.topup.tried} of ${sheet.topup.of} later orders tried` : "Topping up · later orders fill its gaps first") : "Partial · held for a later set" };
    if (sheet.material === "rose" && typeof opts.selected?.rose === "boolean") return opts.selected.rose
      ? {include:true,reason:"Included by you"} : {include:false,reason:"Not selected for this set"};
    if (sheet.material === "rose") return opts.seq > 0 && opts.seq % 2 === 0
      ? { include: true, reason: "Rose gold · even-numbered set" } : { include: false, reason: "Held for Set 2, 4, 6…" };
    if (["gold10k", "gold14k"].includes(sheet.material)) return opts.selected?.[sheet.material] === true
      ? { include: true, reason: "Included by you" } : { include: false, reason: "Not selected for this set" };
    return { include: false, reason: "Unknown material" };
  }
  /** Which materials must travel together: those tied by an order with pieces in more than one. Each group is a set.
   *  → { material → groupKey }, groupKey = the group's materials sorted and joined with "+". */
  function kinGroups(lines) {
    const parent = {}; const find = x => { while (parent[x] !== x) { parent[x] = parent[parent[x]]; x = parent[x]; } return x; };
    const union = (a, b) => { const ra = find(a), rb = find(b); if (ra !== rb) parent[ra] = rb; };
    for (const l of lines) if (l.material) parent[l.material] = parent[l.material] || l.material;
    const byOrder = new Map();
    for (const l of lines) { if (!l.material) continue; if (!byOrder.has(l.orderId)) byOrder.set(l.orderId, new Set()); byOrder.get(l.orderId).add(l.material); }
    for (const mats of byOrder.values()) { const arr = [...mats]; for (let i = 1; i < arr.length; i++) union(arr[0], arr[i]); }
    const groups = {}; for (const m of Object.keys(parent)) { const r = find(m); (groups[r] = groups[r] || []).push(m); }
    const out = {}; for (const ms of Object.values(groups)) { const key = ms.slice().sort().join("+"); for (const m of ms) out[m] = key; }
    return out;
  }

  /* ═══ 8 · the run ══════════════════════════════════════════════════════ */
  const RUN_STEPS = ["pull", "claim", "pool", "plan", "nest", "checkpoint", "engrave", "revalidate", "labels", "commit", "complete"];
  const HALF = { pull: "A", claim: "A", pool: "A", plan: "A", nest: "A", checkpoint: "A", engrave: "B", revalidate: "B", labels: "B", commit: "B", complete: "B" };
  const nextStep = s => { const i = RUN_STEPS.indexOf(s); return i < 0 || i === RUN_STEPS.length - 1 ? null : RUN_STEPS[i + 1]; };
  const stepIndex = s => RUN_STEPS.indexOf(s);

  /* ═══ 9 · the run record and its line archive ═════════════════════════
     A run's record is one Firestore document: 1 MiB and 40,000 index entries at most. A run left in Auto stays open for
     days (new orders join it), and every line it ever took stayed in its record at about 0.9 KB each, until the record
     could not be saved and the run stopped (at 1,100–1,400 lines). The lines of an order the run is done with go to the
     run's line archive instead (Charm_Nest_Run_Lines, written by the page, read by the server under the record). These
     say which orders those are, and measure a record the way Firestore will. */
  const RUN_RECORD = { bytes: 1048576, entries: 40000, warn: 0.7, partBytes: 262144, keepSheets: 100 };
  // the states the run is done with: the complete step lets go of these orders' station dots too
  const FINISHED_LINE = new Set(["committed", "gone", "skipped", "noDesign"]);
  /** The orders a run is done with, as orderId → their line keys: every line finished, and the order closed (committed
      at the station, or every line gone from Etsy). An order still open at the station (only skipped or no-design
      lines, or a single line gone) stays in the record: a run resumed from its record would otherwise meet it again
      as a new arrival and cut it. A line without an order id is never counted. */
  function closedOrders(lines) {
    const by = new Map();
    for (const [key, l] of Object.entries(lines || {})) {
      const id = l && l.orderId != null ? String(l.orderId) : "";
      if (!id) continue;
      const o = by.get(id) || { keys: [], done: true, committed: false, gone: true };
      o.keys.push(key);
      if (!FINISHED_LINE.has(l.state)) o.done = false;
      if (l.state === "committed") o.committed = true;
      if (l.state !== "gone") o.gone = false;
      by.set(id, o);
    }
    return new Map([...by].filter(([, o]) => o.done && (o.committed || o.gone)).map(([id, o]) => [id, o.keys]));
  }
  /** A text's size in UTF-8 bytes, as Firestore counts a string (no TextEncoder: the run controller runs without one). */
  function utf8Bytes(text) {
    const s = String(text); let n = s.length;
    for (let i = 0; i < s.length; i++) { const c = s.charCodeAt(i); if (c >= 0x80) n += c >= 0xd800 && c <= 0xdbff ? 0 : c >= 0x800 ? 2 : 1; }
    return n;
  }
  /** A short fingerprint of a text (16 hex digits), to tell two versions apart. Not for secrets. */
  function textHash(text) {
    const s = String(text); let h1 = 0xdeadbeef, h2 = 0x41c6ce57;
    for (let i = 0; i < s.length; i++) { const c = s.charCodeAt(i); h1 = Math.imul(h1 ^ c, 2654435761); h2 = Math.imul(h2 ^ c, 1597334677); }
    h1 = Math.imul(h1 ^ (h1 >>> 16), 2246822507) ^ Math.imul(h2 ^ (h2 >>> 13), 3266489909);
    h2 = Math.imul(h2 ^ (h2 >>> 16), 2246822507) ^ Math.imul(h1 ^ (h1 >>> 13), 3266489909);
    return (h2 >>> 0).toString(16).padStart(8, "0") + (h1 >>> 0).toString(16).padStart(8, "0");
  }
  /** Firestore's automatic index entries for a stored value, near enough: two per field, one per array element. */
  function indexEntries(v) {
    if (Array.isArray(v)) return v.length;
    if (v && typeof v === "object") { let n = 0; for (const k of Object.keys(v)) if (v[k] !== undefined) n += indexEntries(v[k]); return n; }
    return 2;
  }
  /** [key, line] pairs in parts of at most `limit` bytes of JSON each: [{ json, keys }]. A larger line is a part alone. */
  function archiveParts(entries, limit = RUN_RECORD.partBytes) {
    const parts = []; let cur = [], size = 2;
    const close = () => { if (cur.length) parts.push({ json: JSON.stringify(Object.fromEntries(cur)), keys: cur.map(([k]) => k) }); cur = []; size = 2; };
    for (const [k, l] of entries) {
      const n = utf8Bytes(JSON.stringify(String(k))) + utf8Bytes(JSON.stringify(l)) + 2;
      if (cur.length && size + n > limit) close();
      cur.push([String(k), l]); size += n;
    }
    close();
    return parts;
  }

  /* ═══ 9b · the sorting station's order sticker ═════════════════════════════════════════════════════════════════════
     The 1 × 1 in QR sticker is printed by QR Printer.html, the page the sorting station (sorting.html) loads in a hidden
     frame: it reads the object below from localStorage "qrPrintAll", draws the order number's QR (ECC H) with the
     dispatch date, the order number, one or two dots (earrings/studs/rings two, everything else one) and three note lines
     (Metal / Title / Match) on a 72 × 72 pt page, and opens the print dialog. This builds that object exactly as
     sorting.html's buildQrDataForCell does from the same Etsy order (its fillPreviewBoxes reads the metal and the
     keywords), so a sticker printed from the sorter is the sorting station's sticker. Nothing in it is written anywhere. */
  const SORT_GROUP_A = ["stud", "studs", "stud earrings", "ring", "rings", "earrings"];
  const SORT_GROUP_B = ["necklace", "necklaces", "huggie", "huggies", "huggie earrings", "hoop", "hoops", "hoop earrings", "bracelet", "bracelets", "extender", "extenders", "chain", "chains"];
  const SORT_METAL_NAMES = ["metal", "metal choice", "metal - engraving", "metal colour", "color", "metal choice / engraving option", "metal choice / necklace length", "number of discs / metal", "number of discs/metal", "metal/necklace length", "metal/necklace length/engrave"];
  const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  function sortPhrase(str, phrase) { return new RegExp("\\b" + phrase.trim().replace(/\s+/g, "[\\s\\W]+") + "\\b", "i").test(String(str || "").toLowerCase()); }
  /** The metal sorting.html shows under an item (its "Metal:" box), from the buyer's choices; "" when none is found. */
  function sortingMetal(variations) {
    const vars = (variations || []).map(v => ({ formatted_name: String(v.formatted_name != null ? v.formatted_name : v.name || ""), formatted_value: String(v.formatted_value != null ? v.formatted_value : v.value || "") }));
    let metalVar = vars.find(v => { const n = v.formatted_name.trim().toLowerCase(); return n.includes("metal") || n.startsWith("color") || n.startsWith("colour") || SORT_METAL_NAMES.includes(n); });
    if (!metalVar) metalVar = vars.find(v => /rose\s*gold|rosegold|rosefilled|gold\s*filled|goldfilled|\bgold\b|silv[ae]?r|sterling\s*silver|14k|white\s*gold/.test(v.formatted_value.toLowerCase()));
    if (!metalVar || !metalVar.formatted_value) return "";
    let m = metalVar.formatted_value.replace(/\+\s*engraving/gi, "").replace(/\+\s*engrave/gi, "").replace(/\b1-5\s*Characters\b/gi, "").replace(/\b6-10\s*Characters\b/gi, "").replace(/\b11\s*\+\s*Characters\b/gi, "")
      .replace(/\b1-5\s*·\s*Character(?:s)?\b/gi, "").replace(/\b6-10\s*·\s*Character(?:s)?\b/gi, "").replace(/\b11\+\s*·\s*Character(?:s)?\b/gi, "").replace(/\s{2,}/g, " ").trim();
    m = m.replace(/([a-z])([A-Z])/g, "$1 $2").replace(/\bnecklace\s*length\b[\s\/:]*/gi, "").replace(/\s*-\s*\d+(\.\d+)?\s*$/g, "").trim();
    m = m.replace(/[·•]/g, " ").trim();
    const n = m.toLowerCase();
    if (/(?:14k\s*)?(?:rose\s*gold|rosegold|rose\s*gold\s*filled|rosegold\s*filled|rosefilled)\b/.test(n)) m = "Rose Gold";
    else if (/(?:14k\s*)?(?:gold\s*filled|goldfilled)\b/.test(n)) m = "Gold Filled";
    else if (/(?:color\s*)?sterling\s*silver\b|silv[ae]?r\b/.test(n)) m = "Silver";
    else if (/\b14k\b/.test(n)) m = "14K";
    else if (/\bgold\b/.test(n)) m = "Gold Filled";
    return m.replace(/\s{2,}/g, " ").trim();
  }
  /** order: { receiptId, lines[] } as the sorter holds it; line: the line the sticker is printed from (any of them: the
   *  sticker is the whole order's). → the object QR Printer.html prints (localStorage "qrPrintAll"). */
  function sortingLabel(order, line) {
    order = order || {}; const lines = (order.lines && order.lines.length ? order.lines : [line]).filter(Boolean);
    const rid = String(order.receiptId || (line && line.receiptId) || "");
    // the transaction's expected ship date, as sorting.html reads it; a line restored from a saved run has only its
    // order's (the station takes that from the same field)
    const exp = +(lines[0] && lines[0].expectedShipDate) || +order.shipBy || 0;
    const d = exp ? new Date(exp * 1000) : null;
    const dispatchDate = d ? `${("0" + d.getDate()).slice(-2)} ${MONTHS[d.getMonth()]} ${d.getFullYear()}` : "N/A";
    const items = lines.map(l => {
      const title = String(l.title || "");
      const keywords = [];
      if (title) { SORT_GROUP_A.forEach(p => { if (sortPhrase(title, p)) keywords.push(p); }); SORT_GROUP_B.forEach(p => { if (sortPhrase(title, p)) keywords.push(p); }); }
      if (!keywords.length) keywords.push("groupB");
      const variations = (l.variations || []).map(v => ({ formatted_name: String(v.formatted_name != null ? v.formatted_name : v.name || ""), formatted_value: String(v.formatted_value != null ? v.formatted_value : v.value || "") }));
      const it = { receipt_id: /^\d+$/.test(rid) ? Number(rid) : rid, transaction_id: l.transactionId, listing_id: l.listingId, title, sku: l.sku || "", quantity: Math.max(1, +l.quantity || 1), qty: Math.max(1, +l.quantity || 1), variations, keywords, typedOrderNumber: rid };
      if (exp) it.dispatch_date = dispatchDate;
      return it;
    });
    const firstWords = s => String(s || "").replace(/\s+/g, " ").trim().split(" ").slice(0, 2).join(" ");
    const metal = items.map(it => sortingMetal(it.variations) || "No Metal").join(", ");
    const at = Math.max(0, line ? lines.findIndex(l => l === line || (l.transactionId && String(l.transactionId) === String(line.transactionId))) : 0);
    return { dispatchDate, items, userTypedOrderNum: rid || "UnknownOrder", primaryItemIndex: at, notesBlock: { metal, title4: items.map(it => firstWords(it.title)).join(", "), matrixNums: "", matrixTitle: "" } };
  }

  /** A saved working sheet is not a released set. Isolated solids stay separate even in the same run. */
  function libraryGroup(sheet, options = {}) {
    if (sheet.setId && !sheet.draft && sheet.solidIncluded !== false) return {key:"set:"+sheet.setId, name:"Set "+(sheet.setSeq || sheet.seq || ""), setId:sheet.setId, seq:sheet.setSeq || sheet.seq || null, standalone:false, working:false};
    const solid=["gold10k","gold14k"].includes(sheet.metal), scope=sheet.runId || (sheet.sources || []).map(s=>s.hash || s.name).sort().join("+") || sheet.id;
    if (solid && options.combineSolids) return {key:"solid-waiting", name:"14K / 10K Solid Waiting for Approval", setId:null, seq:null, standalone:true, working:true};
    return {key:(solid ? "standalone:"+sheet.metal+":" : "working:")+sheet.day+":"+scope, name:solid ? "Standalone "+(sheet.metal === "gold10k" ? "10K" : "14K") : "Incomplete Sheets: Waiting to be filled!", setId:null, seq:null, standalone:solid, working:true};
  }
  // Local order-number filtering: every digit narrows the same list and sheet highlights.
  const orderQuery = value => String(value || '').trim().replace(/[\s#-]/g, '');
  const orderMatches = (id, value) => { const q=orderQuery(value); return !q || (/^\d+$/.test(q) && String(id || '').includes(q)); };
  function orderGroups(pieces, value) {
    const groups=new Map();
    for(const x of pieces || []) if(!x.gone && x.rid && x.rid!=='—' && orderMatches(x.rid,value)) {
      const rid=String(x.rid);if(!groups.has(rid))groups.set(rid,[]);groups.get(rid).push(x);
    }
    return [...groups].sort(([a],[b])=>a.localeCompare(b));
  }
  return { questionGone, staleQuestionHold, orderQuery, orderMatches, orderGroups, SPECIAL, specialOf, engravingNote, sortingLabel, sortingMetal, visible, purchaseDetails, purchaseOptions, libraryGroup, METAL_TO_CARD, CARD_TO_METAL, CARD_TAG, CARD_LABEL, DEFAULT_OPTION_MAP, FORM_VALUES, SIZE_VALUES, norm, optionLookup, isNoDesign, resolveSku, variationBase, optionDesign, huggieSku, looseKey, masterSku, KNOWN_SKU_TYPOS, knownTypo, typoSku, inventorySku, tiesToOption, listingWide, interpretLine, lineKey, poolId, PIECE_RULES, pieceCountOf, piecesOf, sidesOf, kindFor, glue, pinPieces, countRead, optionCount, noteCountOf, singleSideOf, lineSignals, lineMismatched, splitSkus, pairMembers, signsOfText, signsOfLine, signValueSku, pairInfo, discsIn,
    ENGRAVING_FONTS, FONT_RULES, fontById, fontRead, fontParts, isFontOption,
    orderPlacedAt, frontOf, byQueue, rankDate, orderDay, intakePlan, completionDay, completionTime, compareCompleted, completedTitle, localDay, dateTag, dateTagOfDay, setId, setLabel, setFolder, sheetName, sheetFolder, toB36, encodeOrderList, safeChunks, evaluateOrder, planRelease, sheetRelease, kinGroups, FAST_MATERIALS, SLOW_MATERIALS, RUN_STEPS, HALF, nextStep, stepIndex, DONE_STATES,
    skuFamily, lineFamily, skuFamilyConflict, familyTwins, listingTwin, inventoryPicks, inventoryWhy, suggestCharms,
    RUN_RECORD, FINISHED_LINE, closedOrders, utf8Bytes, textHash, indexEntries, archiveParts };
});
