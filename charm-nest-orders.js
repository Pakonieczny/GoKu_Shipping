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

  /* ═══ 0 · Etsy's HTML-escaped text ═════════════════════════════════════
     Etsy hands text over HTML-escaped: the shop's SKU "ACORN'S LEAF" arrives as "ACORN&#39;S LEAF" and no master file holds that
     (Paul, 11 Oct 2026, order 4196630529: "Unknown SKU: SKU ACORN&#39;S LEAF"). decodeEntities is the ONE decoder: the standard
     entities (&#39; &apos; &amp; &quot; &lt; &gt;) and the numeric (&#39;) and hex (&#x27;) forms, as many passes as it takes (Etsy
     double-escapes some text, "&amp;#39;"; every pass that changes the text shortens it), so a text that is already decoded is never
     changed and decoding twice is the same as decoding once. cleanLine / cleanOrders decode a line's SKU, title and option names and values
     where the sorter takes the Etsy orders in (DesignLink, charm-nest-bridge.js) and where it reads a stored line back
     (rowFromRecord); cleanMaps does the same for the SKUs a person's saved answers are keyed by. upSku, norm and bySkuOf below read
     the decoded form too, so a SKU that carries no entity resolves exactly as it did, and one that does resolves as its decoded self.
     The customer's own words (personalisation, buyer message, notes) are NOT touched here. */
  const ENTITY = /&(?:#(\d{1,7})|#[xX]([0-9a-fA-F]{1,6})|(amp|apos|quot|lt|gt));/g;
  const NAMED_ENTITY = { amp: "&", apos: "'", quot: '"', lt: "<", gt: ">" };
  const decodeEntities = v => {
    let s = v == null ? "" : String(v);
    while (s.indexOf("&") >= 0) {   // (each pass that changes anything makes the text shorter, so this ends)
      const was = s;
      s = s.replace(ENTITY, (m, dec, hex, name) => {
        if (name) return NAMED_ENTITY[name];
        const cp = dec != null ? parseInt(dec, 10) : parseInt(hex, 16);
        return cp >= 32 && cp <= 0x10FFFF && !(cp >= 0xD800 && cp <= 0xDFFF) ? String.fromCodePoint(cp) : m;   // (a control code or a number that is no character stays as written)
      });
      if (s === was) break;
    }
    return s;
  };
  /** A line as the Etsy transaction gave it, with its SKU, title and the names and values of its options decoded; the line itself is changed (it is a fresh copy the station sent, or one the page built) and answered. */
  function cleanLine(line) {
    if (!line || typeof line !== "object") return line;
    for (const k of ["sku", "title"]) if (typeof line[k] === "string" && line[k].indexOf("&") >= 0) line[k] = decodeEntities(line[k]);
    if (Array.isArray(line.variations)) for (const v of line.variations) if (v && typeof v === "object") for (const k of ["name", "value", "formatted_name", "formatted_value"]) if (typeof v[k] === "string" && v[k].indexOf("&") >= 0) v[k] = decodeEntities(v[k]);
    return line;
  }
  /** What the station answers to an orders call ({ orders: [...] } or { order }): every line of every order, decoded. */
  function cleanOrders(res) {
    if (!res || typeof res !== "object") return res;
    for (const o of Array.isArray(res.orders) ? res.orders : res.order ? [res.order] : []) if (o && Array.isArray(o.lines)) o.lines.forEach(cleanLine);
    return res;
  }
  /** One of the maps a person's answers are saved in (aliases: { listingId: { bySku, pairBySku, ... } }; noDesign: { skus } ), read with its SKUs decoded: a key saved while the SKU still carried an entity is read under the decoded spelling (a
   *  decoded key already there wins); nothing is dropped. Answers the same objects when nothing in them carries an entity. */
  function cleanMaps(aliases, noDesign) {
    const keyed = m => { if (!m || typeof m !== "object" || !Object.keys(m).some(k => k.indexOf("&") >= 0)) return m; const out = {}; for (const k of Object.keys(m)) { const d = decodeEntities(k).trim().toUpperCase(); if (!(d in out) || d === k) out[d] = m[k]; } return out; };
    if (aliases && typeof aliases === "object") for (const id of Object.keys(aliases)) { const a = aliases[id]; if (!a || typeof a !== "object") continue; const b = keyed(a.bySku), p = keyed(a.pairBySku); if (b !== a.bySku || p !== a.pairBySku) aliases[id] = Object.assign({}, a, b ? { bySku: b } : {}, p ? { pairBySku: p } : {}); }
    if (noDesign && Array.isArray(noDesign.skus) && noDesign.skus.some(x => String(x).indexOf("&") >= 0)) noDesign.skus = noDesign.skus.map(x => decodeEntities(x));
    return [aliases, noDesign];
  }

  /* ═══ 1 · material: the station's metal key → the sorter's card ═══════ */
  const METAL_TO_CARD = { gold: "gold", silver: "silver", rose: "rose", "10k": "gold10k", "14k": "gold14k" };
  const CARD_TAG = { gold: "GF", silver: "SS", rose: "RG", gold10k: "10K", gold14k: "14K" };
  const CARD_LABEL = { gold: "GF 14/20", silver: "SS", rose: "RG 14/20", gold10k: "10K Gold", gold14k: "14K Gold" };

  /* ═══ 2 · option maps ══════════════════════════════════════════════════
     Charm_Option_Map/{listingId or "*"} = { optionName: { value: { field, value } } }. Names and values are matched
     after normalisation (lower-case, trimmed, single spaces). The built-in "*" map below covers the plain literal
     values the shop's listings use; anything else is a "Needs mapping" review item until a person maps it once. */
  const norm = s => decodeEntities(s).toLowerCase().replace(/\s+/g, " ").trim();   // (decoded: an option map saved under "women&#39;s" still meets "women's")
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
  /* A NECKLACE LENGTH option ("Necklace Length: 20"", "Necklace Length in inches: 14"") is the buyer's own choice of a necklace chain, and no earring has one. lineForm (HUGGIEGUARD) read it as
     "necklace" from the start; lineSignals (EARWORDS) did not, so a necklace whose TITLE also says "Earrings" was a pair of earrings (KITEPAIR, 13 Oct 2026, order 4199929230): ONE definition, read by both. */
  const isNecklaceLength = (name, value) => /\bnecklace\b/i.test(String(name || "")) && isChainOption(name) && looksLikeLength(value);
  /* BFWHY (Paul, 12 Oct 2026, order 4200828980, BIRTHFLOWER_NECKLACE_DISC-9, "Birth Flower: Marigold"): the line's other drop-down has values like Gold - 16" - 2 sides, Silver- 18"- 1 side,
     Rose- 17"- 2 sides. Whatever the shop calls it, an option whose VALUE is only a metal, a chain length and / or a number of sides (with "pair" / "single" beside them) is the shop's metal / length option, never a
     choice of charm. It is read like a metal option (named Metal / Colour / Finish), so it is no "unread option" that makes a second one ask. The station already reads the metal from such a
     value whatever the option is called (design-1.html metalKeyForTx step 2); this is the same reading for the options. A metal word alone ("Rose" is also the June flower) or a length alone is
     NOT enough: it takes a metal AND a length or a number of sides, and nothing else in the value. Never reads what the value MEANS (sides = back engraving is the engraving reading's). */
  const SPEC_LENGTH = /(^|[^\p{L}\p{N}])\d{1,2}(?:\.\d)?\s*(?:"|''|\u201d|\u2033|in\b|inch(?:es)?\b|cm\b)/giu;
  const SPEC_SIDES = /(^|[^\p{L}\p{N}])(?:[12]|one|two|single|double|both)[\s-]*sid(?:ed|es?)(?![\p{L}\p{N}])/giu;
  const SPEC_METAL_WORD = /^(?:gold|silver|silvr|sterling|rose|rosegold|yellow|solid|filled|fill|gf|karat|\d{1,2}kt?|\d{1,2}karat)$/;
  const SPEC_FILLER_WORD = /^(?:pair|pairs|single|chain|only|and|with)$/;   // (words that may stand beside a metal and a length / sides; "Gold Filled - Single" alone is not enough)
  function metalSpecValue(value) {
    let extent = 0, metal = 0;
    const t = " " + decodeEntities(value == null ? "" : value).toLowerCase() + " ";
    const rest = t.replace(SPEC_LENGTH, (m, a) => { extent++; return a + " "; }).replace(SPEC_SIDES, (m, a) => { extent++; return a + " "; });
    for (const w of rest.split(/[^\p{L}\p{N}]+/u).filter(Boolean)) {
      if (SPEC_METAL_WORD.test(w)) metal++;
      else if (!SPEC_FILLER_WORD.test(w)) return false;
    }
    return metal > 0 && extent > 0;
  }
  /** An option that carries the metal (and length, sides…): by its NAME (Metal / Colour / Finish / Plating) or by its VALUE (metalSpecValue). */
  const isMetalVariation = (name, value) => isMetalOption(name) || metalSpecValue(value);
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
  /* THE ENGRAVING FONTS (Paul, 10 Oct 2026: "find similar versions that are open source to the options we offer and then map those options"). Every font here is
     under the SIL Open Font License (the licence text is beside each file in vendor/fonts/); text becomes outlines, so the output never references a font. `files`
     are the Regular file and, when the family has a heavier cut, the Semibold file (the weight rule: Semibold under 2.2 mm cap height; a family with ONE weight,
     the four scripts below, engraves its Regular at every size). Chosen by LOOKING at the shop's own font photos (the Etsy "Font Choice" charts of listings
     1008920168 / 234758391 / 1445686334 and the Mini Tag chart of 1714117116 / 4458930445), see plans/discs-1010/images/:
       · Stylish   = a school cursive with looped capitals          -> Playwrite US Trad (Playwrite Project / TypeTogether)
       · Pristina  = a slanted pen script (Microsoft Pristina is commercial) -> Marck Script (Denis Masharov)
       · Angelina  = a light, upright handwriting                    -> Mynerve (Sarah Cadigan-Fried)
       · Typewriter= what the shop's chart draws as a humanist serif (descending J) -> Crimson Text (Sebastian Kosch); the name is the shop's, it is no typewriter face
       · Comic     = what the chart draws as a humanist sans (Myriad-like) -> Source Sans 3 (Adobe), already the app's font
       · Vibur     = the font itself is open source (Johan Kallas, Google Fonts, SIL OFL): the Mini Tag chart's lettering is Vibur, letter for letter (ENGFONT, 11 Oct: the first
                     match, Playwrite US Modern, was a look-alike; it stays installed because saved backs may name it)
       · Ace       = thin geometric capitals, NO lowercase, slashed zero (Mini Tag) -> Jost Slashed Zero (Jost by Owen Earl, Futura-like, SIL OFL; Regular + SemiBold). The
                     only change to Jost is the slash on the zero (the shop's Ace draws 0 as a slashed circle; Jost has none). `caps` = the text of such a piece is engraved in
                     CAPITALS ("w" -> "W": there is no lowercase in Ace), see capsText. Plain Jost stays installed: saved backs may name it.
       · Britannic Bold = a heavy, tall-x-height flared grotesque (Mini Tag listing 4458930445): Libre Franklin Bold (Regular file) + ExtraBold (the small-size cut) (Impallari / Libre Franklin Project, SIL OFL; static 37 KB
                     instances of the Google Fonts variable file, Latin subset). Chosen by measuring the chart's 62 letters and digits against ~170 open fonts (ENGFONT): the old match,
                     Alegreya Sans Bold, was the worst of those scored (calligraphic, light, old-style digits); Libre Franklin has the chart's weight, widths and plain grotesque forms
     To add one: put the font file(s) and its OFL.txt in vendor/fonts/, add a row here, add its words to FONT_WORDS (or its listing to FONT_LISTINGS). The Playwrite
     files are the static Regular (wght 400) instances Google Fonts serves from the variable fonts of google/fonts; the others are the files of google/fonts as they are. */
  const DEFAULT_FONT = "source-sans-3";
  const ENGRAVING_FONTS = [
    { id: "source-sans-3", name: "Source Sans 3", names: ["Source Sans 3"], files: { Regular: "vendor/fonts/SourceSans3-Regular.otf", Semibold: "vendor/fonts/SourceSans3-Semibold.otf" } },
    { id: "playwrite-us-trad", name: "Playwrite US Trad", names: ["Playwrite US Trad"], files: { Regular: "vendor/fonts/PlaywriteUSTrad-Regular.ttf" } },
    { id: "marck-script", name: "Marck Script", names: ["Marck Script"], files: { Regular: "vendor/fonts/MarckScript-Regular.ttf" } },
    { id: "mynerve", name: "Mynerve", names: ["Mynerve"], files: { Regular: "vendor/fonts/Mynerve-Regular.ttf" } },
    { id: "crimson-text", name: "Crimson Text", names: ["Crimson Text"], files: { Regular: "vendor/fonts/CrimsonText-Regular.ttf", Semibold: "vendor/fonts/CrimsonText-SemiBold.ttf" } },
    { id: "playwrite-us-modern", name: "Playwrite US Modern", names: ["Playwrite US Modern"], files: { Regular: "vendor/fonts/PlaywriteUSModern-Regular.ttf" } },
    { id: "jost", name: "Jost", names: ["Jost"], files: { Regular: "vendor/fonts/Jost-Regular.ttf", Semibold: "vendor/fonts/Jost-SemiBold.ttf" } },
    { id: "alegreya-sans", name: "Alegreya Sans", names: ["Alegreya Sans"], files: { Regular: "vendor/fonts/AlegreyaSans-Bold.ttf", Semibold: "vendor/fonts/AlegreyaSans-ExtraBold.ttf" } },
    // ENGFONT (11 Oct 2026): the three Mini Tag words matched again against the shop's own chart. Added; nothing above was removed or changed (a saved back keeps its font).
    { id: "vibur", name: "Vibur", names: ["Vibur"], files: { Regular: "vendor/fonts/Vibur-Regular.ttf" } },
    { id: "jost-ace", name: "Jost Slashed Zero", names: ["Jost Slashed Zero"], caps: true, files: { Regular: "vendor/fonts/JostSlashedZero-Regular.ttf", Semibold: "vendor/fonts/JostSlashedZero-SemiBold.ttf" } },
    { id: "libre-franklin", name: "Libre Franklin", names: ["Libre Franklin"], files: { Regular: "vendor/fonts/LibreFranklin-Bold.ttf", Semibold: "vendor/fonts/LibreFranklin-ExtraBold.ttf" } }
  ];
  /** The font words the shop's drop-downs offer, any spelling (case, spacing, punctuation, "Typewrite" for "Typewriter"), and the installed font each one is. */
  const FONT_WORDS = [
    { id: "playwrite-us-trad", words: ["Stylish"] },
    { id: "marck-script", words: ["Pristina"] },
    { id: "mynerve", words: ["Angelina"] },
    { id: "crimson-text", words: ["Typewriter", "Typewrite", "TypeWrit", "Typewritr", "TypeWrt"] },   // (seven real spellings; any other that starts "typewr" is read by FONT_STEMS)
    { id: "source-sans-3", words: ["Comic"] },
    { id: "vibur", words: ["Vibur"] },
    { id: "jost-ace", words: ["Ace", "Ace (no lowercase)"] },                // (a 3-letter English word: read only as a whole part of a font option or a length option, never inside other text; "no lowercase" is the shop's own suffix)
    { id: "libre-franklin", words: ["Britannic Bold", "Britannic"] }
  ];
  /** The USUAL font of a listing: used ONLY when the buyer names no font the shop offers (ENGFONT, 11 Oct 2026). On 10 Oct this was a rule that engraved the three Mini Tag listings in ONE
   *  font whatever the buyer picked ("ignore the Font choice"); Paul then asked for each offered font to be matched, so the buyer's own word always wins now (a font choice never holds an
   *  order, never changes silently) and this is only the fallback for a line that carries no font word at all. Matched by listing id or by SKU. */
  const FONT_LISTINGS = [
    { font: "vibur", listings: ["4458930445", "1714117116"], skus: ["TINY INITIAL TAG 1", "TINY INITIAL TAG 1-CO"], why: "the Mini Tag listings: Vibur when the buyer names no font" }
  ];
  /** Words read by their stem, case aside (Etsy cuts a value at 9 letters and buyers misspell: TYPEWRITE, TypeWrit, Typewritr, TypeWrt, "Type Writer"): [{ id, stem }], on the folded word. */
  const FONT_STEMS = [
    { id: "crimson-text", stem: /^typewr[a-z]{0,6}$/ },
    // the three Mini Tag words, on the folded word (case, spaces and punctuation are gone): Ace with the shop's own "(no lowercase)" suffix or a buyer's variant, Britannic Bold with the one-n spelling, Vibur
    { id: "jost-ace", stem: /^ace(?:(?:no|without)?lower(?:case)?|capsonly|allcaps|caps|(?:upper|capital)(?:case|s)?(?:only)?)?$/ },
    { id: "libre-franklin", stem: /^britann?ic(?:bold)?$/ },
    { id: "vibur", stem: /^vib[ue]r$/ }
  ];
  /** "No Font" / "NO FONT" is a choice in the same drop-downs, not a font: nothing is asked or held, the line engraves in the app's own font. */
  const NO_FONT = /^no(?:n|t)?fonts?$/;
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
  /* font words: the shop's drop-down words (FONT_WORDS) and the installed fonts' own names, folded (fontKey), and the installed font each is */
  const WORD_FONT = new Map(), SHOP_WORDS = new Set();
  for (const f of ENGRAVING_FONTS) for (const n of f.names) WORD_FONT.set(fontKey(n), f.id);
  for (const g of FONT_WORDS) for (const w of g.words) { WORD_FONT.set(fontKey(w), g.id); SHOP_WORDS.add(fontKey(w)); }
  /** The installed font a whole font word is ("Typewrite", "TYPEWRITER", "Pristina."), "" when it is not one the shop offers. */
  const stemFont = k => { const s = FONT_STEMS.find(x => x.stem.test(k)); return s ? s.id : ""; };
  const shopFontOf = word => { const k = fontKey(word); return SHOP_WORDS.has(k) ? WORD_FONT.get(k) : stemFont(k); };
  const fontOfWord = word => { const k = fontKey(word); return WORD_FONT.get(k) || stemFont(k); };
  /** The installed font a phrase names: the whole phrase, else the shop's font words inside it ("Typewriter font", "Pristina Script"); "" when it names none or two different ones. */
  function fontOfPhrase(phrase) {
    const whole = fontOfWord(phrase); if (whole) return whole;
    const ids = new Set(String(phrase == null ? "" : phrase).split(/[^\p{L}\p{N}]+/u).map(shopFontOf).filter(Boolean));
    return ids.size === 1 ? [...ids][0] : "";
  }
  const FONT_BY_LISTING = new Map(), FONT_BY_SKU = new Map();
  for (const x of FONT_LISTINGS) { for (const l of x.listings || []) FONT_BY_LISTING.set(String(l), x); for (const k of x.skus || []) FONT_BY_SKU.set(String(k).trim().toUpperCase(), x); }
  /** The listing exception a line falls under (FONT_LISTINGS: one font whatever the buyer picks), by the listing's id or the SKU Etsy gave it; null for every other line. */
  function fontListing(line, ...skus) {
    const hit = FONT_BY_LISTING.get(String(line && line.listingId != null ? line.listingId : ""));
    if (hit) return hit;
    for (const k of [line && line.sku, ...skus]) { const x = FONT_BY_SKU.get(String(k == null ? "" : k).trim().toUpperCase()); if (x) return x; }
    return null;
  }
  /** One font value as a reading: { asked, chain, align, id, name (the installed font; "" when the shop does not offer the word), listing (the listing's one font stands), pick }.
   *  strict: only a part that IS one of the shop's font words counts (a length or metal option that carries a font: "Gold • Typewriter", 16"·STYLISH·Center). Otherwise (an option
   *  named for a font) every part is the font the buyer asked for, a shop word inside it ("set" + "TypeWrt") being what is read when there is one. */
  function fontFrom(p, fx, strict) {
    const none = p.words.some(w => NO_FONT.test(fontKey(w))), rest = p.words.filter(w => !NO_FONT.test(fontKey(w)));
    const shop = rest.filter(w => strict ? fontOfWord(w) : fontOfPhrase(w)), words = shop.length ? shop : strict ? [] : rest;
    if (!words.length) return none && !strict ? { asked: "", chain: p.chain, align: p.align, id: "", name: "", listing: false, pick: [], none: true } : null;   // ("No Font": the buyer asks for no font; a length that carries it stays the length it was)
    // ENGFONT (11 Oct): the buyer's own word always wins; the listing's usual font (FONT_LISTINGS) stands in only when the words name no font the shop offers
    const asked = words.join(" / "), word = fontOfPhrase(asked), font = fontById(word || (fx ? fx.font : ""));
    return { asked, chain: p.chain, align: p.align, id: font ? font.id : "", name: font ? font.name : "", listing: !!fx && !word, pick: fontChoices(asked) };
  }
  /** What ONE option says about a font: null when its NAME is not a font's or its value holds no font word; else
   *  { asked (the font as the buyer's option writes it), chain, align, id, name (the installed font the word is, any spelling; "" for a word the shop does not offer), listing, pick: [{ id, name }] }.
   *  `fx`: the line's listing exception (fontListing): that listing's one font is the font whatever word the buyer picked. */
  function fontRead(name, value, fx) {
    if (!isFontOption(name)) return null;
    return fontFrom(fontParts(value), fx || null, false);
  }
  /** A font hidden in a LENGTH option ("Necklace Length /Font /Alignment": 16"·PRISTINA·Center, or plain "Necklace Length": 16"·TYPEWRITER·Right): null for any other option.
   *  Only a part that is one of the shop's font words counts (a length never turns into a font by guesswork); the buyer's other words stay the length it always was. */
  function fontHidden(name, value, fx) {
    if (isFontOption(name) || !isChainOption(name)) return null;
    return fontFrom(fontParts(value), fx || null, true);
  }
  /** A font hidden in a METAL option that is named for one too ("Metal? Font Choice?": "Gold • Typewriter", "Silver • Custom"): the shop's font word in it, else null. */
  const fontInMetal = (name, value, fx) => isMetalOption(name) && FONT_NAME.test(String(name || "")) ? fontFrom(fontParts(value), fx || null, true) : null;
  /** What a line keeps of a font reading (spec.font). `id` is the installed font ("" = the app falls back to Source Sans 3 and says "Requested font"). */
  const fontSpecOf = (fr, source) => Object.assign({ asked: fr.asked, id: fr.id || "", name: fr.name || "", source }, fr.align ? { align: fr.align } : {}, fr.listing ? { listing: true } : {});
  /** The engraving font of ONE piece of a line (a counted disc, a left ear, a single charm): { id, name, asked, mapped, source }. A piece's own font (spec.fontPieces[index],
   *  one per disc when a listing asks per disc) wins over the line's (spec.font); a font word the shop does not offer, or none, is Source Sans 3 with `asked` kept. */
  function pieceFont(spec, index, face) {
    const own = spec && Array.isArray(spec.fontPieces) && index != null ? spec.fontPieces[index] : null;
    // a FACE that names its own font word (front + back engraving: spec.engraving[face].fontWord, CONTRACT section 8) is read like a drop-down word; every other face uses the line's font (one font per purchase line)
    const fw = faceFontWord(spec, face), fid = fw ? fontOfPhrase(fw) : "";
    let f = fid ? { id: fid, asked: fw, source: "face" } : own || (spec && spec.font) || null; const askedOnly = fw && !fid ? fw : "";
    // BFFONT: a font the buyer named in the message (spec.fontText) serves a piece that no person, no face and no font option settled: it comes after the drop-down's own font and before the listing's
    // usual one. An exact word or a spelling of one is the shop's font (mapped); a font that is not ours, or only a kind of font, is engraved in the nearest installed one and stays a request to look at.
    const ft = !fid && !askedOnly && spec && spec.fontText && spec.fontText.asked && !(f && f.id && !f.listing && !(FONT_MESSAGE_OVER_OPTION && spec.fontText.id)) ? spec.fontText : null;
    if (ft) f = { id: ft.id || "", asked: ft.asked, source: "message" };
    const inst = (f && f.id && fontById(f.id)) || fontById(DEFAULT_FONT);
    const listing = !!(f && f.listing), asked = askedOnly || (f && f.asked ? String(f.asked) : "");
    // (a listing's usual font that stands in for a word the shop does not offer is still a request to look at: not "mapped")
    const out = { id: inst.id, name: inst.name, asked, mapped: !!(f && f.id && fontById(f.id)) && !(listing && asked) && !askedOnly, listing, caps: !!inst.caps, source: f && f.source ? f.source : "default" };
    if (ft) { const same = ft.how === "exact" || ft.how === "spelling" || ft.how === "usual"; Object.assign(out, { mapped: same && !!ft.id, via: "message", how: ft.how, style: ft.style || "", substituted: !!ft.id && !same }); }
    return out;
  }
  /** The font word a face of a front + back line names for itself (spec.engraving[face].fontWord), "" when it names none (the line's font then serves both faces).
   *  No face means the back, as everywhere else (CONTRACT: an absent face is "back"). */
  function faceFontWord(spec, face) {
    const e = spec && spec.engraving && spec.engraving[face === "front" ? "front" : "back"];
    return e && typeof e.fontWord === "string" ? e.fontWord.trim() : "";
  }
  /** Fonts that have no lowercase (Ace): the text of a piece in one is engraved in capitals ("w" -> "W"). Any other font keeps the text exactly as it is. */
  const fontCaps = id => { const f = fontById(id); return !!(f && f.caps); };
  const capsText = (text, id) => fontCaps(id) ? String(text == null ? "" : text).toLocaleUpperCase("en-US") : String(text == null ? "" : text);
  /** The installed font a saved back says it was engraved in: its fontKey, else its font NAME (every back saved before the other fonts says "Source Sans 3"), else Source Sans 3. */
  function fontOfRecord(rec) {
    if (rec && rec.fontKey && fontById(rec.fontKey)) return fontById(rec.fontKey).id;
    const k = fontKey(rec && rec.font), hit = k ? ENGRAVING_FONTS.find(f => f.names.some(n => fontKey(n) === k)) : null;
    return hit ? hit.id : DEFAULT_FONT;
  }
  /** The one plain line the cards show: "Font: Stylish → Playwrite US Trad", or for a font the shop does not offer "Requested font: Script (engraved in Source Sans 3)"; "" when the line has no font. */
  function fontLine(spec, index, face) {
    const f = spec && (spec.font || (spec.fontPieces && spec.fontPieces[index]) || (faceFontWord(spec, face) && {}) || (spec.fontText && spec.fontText.asked && {})); if (!f) return "";
    const p = pieceFont(spec, index, face), asked = clip(p.asked, 40), caps = p.caps ? " · capitals only" : "";
    // BFFONT: a font the buyer wrote in the message says so ("from the message"); a stand-in says which kind it is the nearest of
    if (p.via === "message") {
      if (p.how === "usual") return `Font: ${p.name} (the usual font; the buyer asked for “${asked}”, from the message)`;
      if (p.mapped) return `Font: ${asked ? asked + " → " : ""}${p.name}${caps} (from the message)`;
      const kind = fontStyleLabel(p.style);
      return p.substituted ? `Requested font: ${asked} (engraved in ${p.name}, the nearest ${kind ? kind + " " : ""}font we have${caps}; from the message)` : `Requested font: ${asked} (engraved in ${p.name}, the usual font; from the message)`;
    }
    if (p.listing) return asked ? `Requested font: ${asked} (engraved in ${p.name}, the usual font of this listing${caps})` : `Font: ${p.name} (the usual font of this listing: the buyer named none${caps})`;
    return p.mapped ? `Font: ${asked ? asked + " → " : ""}${p.name}${caps}` : asked ? `Requested font: ${asked} (engraved in ${p.name})` : "";
  }
  const fontWhy = fr => `font “${clip(fr.asked, 40)}” is not one of the engraving fonts (${fr.pick.length === 1 ? "the app engraves in " : "the app has "}${fr.pick.map(f => f.name).join(", ")}): a person picks`;

  /* ═══ 2c · a font named in the buyer's own words (BFFONT, Paul 12 Oct 2026) ═══════════════════════════════════════════════════════════════
     (Paul, order 4200828980, personalisation "Typewriter font" / "Georgia Andy Margot Jaarsma" / "October 26 2020": "Sometimes users will stipulate the font choice in the message, so the
     message has to be parsed for font choices, and that has to have some flexibility for wrongly spelled fonts or similar fonts to what we have in our repository that can be substituted in
     case of not exact match.") One reader for EVERY listing: fontReadText(text) finds the font requests in free text (the personalisation box, the buyer's message, a staff note), says which
     lines are only a request (they are not words to engrave), and fontTextOf(spec) keeps the one reading on the line as spec.fontText. A drop-down font (spec.font) is read exactly as before.
     THE RULE (fontScanLine). A word is a font request only beside a CUE, never because it looks like a font:
       · label       "Font: Pristina", "font Angelina", "Font style: Stylish", "Fuente: ..."  - a line that starts with the word font / typeface / lettering is a font line, whatever follows;
       · font cue    "Typewriter font", "the Pristina font", "font Pristina", "in the uppercase Ace font", "letra typewriter" - the name touches the word font (before or after it);
       · style cue   "Angelina script", "Pristina Style", "Cursive text - Izzy", "block letters" - a shop font word (never Ace) or a style word before script / style / text / letters / writing;
       · weak cue    "in Comic Sans please", "using Vibur", "Evie in cursive", "with Typewriter." - in / with / using / use / choose / pick / select + a font name, ONLY when the name ENDS the phrase (end of the
                     line, punctuation, please / thanks / and) and is a shop word, a two-word font name (Courier New) or cursive / script / calligraphy: "in Georgia Andy" is no request;
       · bare line   "Pristina" alone on its own line: a font ONLY when it is one of the shop's own font stems (Stylish, Pristina, Typewriter, Comic, Vibur, Britannic Bold) and other lines carry the words.
                     Angelina and Ace are first names as often as fonts: alone on a line they stay words and ONE plain line asks a person (fontText.asks), nothing is guessed.
       A single-word font name of someone else's (Georgia, Courier, Arial, Times) needs the word font / typeface or a label: "Georgia Andy Margot Jaarsma" has no cue, so it is the buyer's NAME.
       A word after no / not / don't / without / never is not asked for ("No cursive pls"); a question ("Can this be in cursive?") is put to the shop, not a choice; "font #3" names a picture on the
       listing and is asked, never guessed; the buyer's own handwriting (a photo, "in his handwriting") is no font.
     SPELLING (fontPhrase): case, spaces, punctuation, plurals and the shop's seven Typewriter spellings are set aside (fontKey); then the stems (typewr...), Comic Sans / Comic Sans MS, and an edit distance
       (Damerau-Levenshtein, the same first letter, 1 edit for 5-9 letters, 2 for 10+: Typewritter, Tipewriter, Pristine, Angelia, Britanic, Viber, Cursiv) - spelling is forgiven only beside a font / style
       cue or a label, never after a bare "in" ("in pristine condition" is no font).
     NEAREST (FONT_STYLES, ONE table of data): a font that is not ours (Courier New, Georgia, Brush Script) or only a kind of font (cursive, serif, bold, handwriting) is engraved in the first INSTALLED font
       of its style's ordered preference list; the buyer's own words stay beside it ("Requested font: Courier New (engraved in Crimson Text, the nearest typewriter-style font we have; from the message)")
       so a person sees it. It never holds an order. Fonts that are only a taste (nice, pretty, italic, custom) pick nothing: the usual font and the same plain line. To add a font to the app: install
       it (ENGRAVING_FONTS) and put its id FIRST in the lists of its styles below. jost-ace is capitals only and is never a stand-in.
     WHERE IT COUNTS (pieceFont): after a person's saved choice, a font the buyer names for that face, and the drop-down's own font (which wins when the message names another one: spec.fontText.differs says so);
       before the listing's usual font (FONT_LISTINGS). The words of a line that is only a request are taken out of the engraved words (engravingOf, the reader's result, a reading that failed). */
  /** WHO WINS when the order's font OPTION (the drop-down: what the buyer bought) and a font written in the MESSAGE name different fonts. The default (Paul, 12 Oct 2026, from the audit): the drop-down wins,
   *  and the plain lines say "Buyer's message also asks for X" - never a hold. Set this to true to let a message font that is one of ours (or the nearest of a kind) win instead: it is the only place that decides. */
  const FONT_MESSAGE_OVER_OPTION = false;
  /** The one plain sentence for a drop-down and a message that disagree ("" when they do not). */
  const fontDisagree = (spec, short) => { const ft = spec && spec.fontText; if (!ft || !ft.differs || faceFontWord(spec, "front") || faceFontWord(spec, "back")) return "";   // (a face that names its own font is a face font, as ever)
    if (short) return FONT_MESSAGE_OVER_OPTION && ft.id ? `Buyer's message asks for ${ft.asked}, not the option's ${ft.differs} (the message wins)` : `Buyer's message also asks for ${ft.asked} (the font option wins)`;
    return FONT_MESSAGE_OVER_OPTION && ft.id ? `The font option chose ${ft.differs}; the buyer's message asks for ${ft.asked} and that is what is engraved` : `Buyer's message also asks for ${ft.asked}; the font option chose ${ft.differs} and that is what is engraved`; };
  const FONT_STYLES = [
    // style (row order is the priority when a phrase holds several style words), label (in the plain line), prefer (installed font ids, best first), names (other people's fonts, whole names), words (kind-of-font words)
    { style: "typewriter", label: "typewriter-style", prefer: ["crimson-text", "source-sans-3"],
      names: ["courier", "courier new", "courier prime", "american typewriter", "special elite", "prestige elite", "lucida console", "consolas", "monaco", "menlo", "andale mono", "letter gothic", "old typewriter", "monospace", "monospaced"],
      words: ["monospace", "monospaced", "mono"] },
    { style: "brush", label: "brush-script", prefer: ["vibur", "marck-script", "playwrite-us-modern", "playwrite-us-trad"],
      names: ["brush script", "brush script mt", "brush lettering", "marker felt"], words: ["brush", "brushed", "signature", "marker"] },
    { style: "script", label: "script", prefer: ["marck-script", "playwrite-us-trad", "vibur", "playwrite-us-modern"],
      names: ["edwardian script", "edwardian script itc", "french script", "french script mt", "freestyle script", "kunstler script", "palace script", "palace script mt", "vivaldi", "zapfino", "mistral", "lucida calligraphy",
        "bickham script", "great vibes", "allura", "alex brush", "parisienne", "dancing script", "sacramento", "tangerine", "italianno", "pinyon script", "playlist script", "petit formal script"],
      words: ["script", "scripted", "scripts", "calligraphy", "calligraphic", "fancy", "elegant", "formal", "wedding", "ornate", "decorative", "flourish", "swash", "scrolly"] },
    { style: "cursive", label: "cursive", prefer: ["playwrite-us-trad", "playwrite-us-modern", "marck-script", "vibur"],
      names: ["monotype corsiva", "snell roundhand", "apple chancery", "zapf chancery", "lucida handwriting", "segoe script", "pacifico", "lobster", "yellowtail", "kaushan script", "satisfy", "courgette"],
      words: ["cursive", "joined", "looped", "loopy", "swirly", "wavy", "flowing", "curly", "connected", "longhand"] },
    { style: "handwriting", label: "handwriting", prefer: ["mynerve", "vibur", "playwrite-us-modern"],
      names: ["bradley hand", "segoe print", "kristen itc", "ink free", "chalkboard", "chalkboard se", "indie flower", "caveat", "patrick hand", "permanent marker", "shadows into light", "amatic sc", "gloria hallelujah"],
      words: ["handwriting", "handwritten", "handlettered", "casual", "childlike", "childish"] },
    { style: "blackletter", label: "old-style", prefer: ["crimson-text", "marck-script"],
      names: ["old english", "old english text", "old english text mt", "blackletter", "black letter", "fraktur", "gothic script", "textura", "chancery"], words: ["blackletter", "fraktur"] },
    { style: "slab", label: "slab-serif", prefer: ["crimson-text", "libre-franklin"],
      names: ["rockwell", "clarendon", "roboto slab", "museo slab", "slab serif", "egyptienne"], words: ["slab"] },
    { style: "serif", label: "serif", prefer: ["crimson-text"],
      names: ["times", "times new roman", "georgia", "garamond", "eb garamond", "palatino", "palatino linotype", "book antiqua", "bookman", "baskerville", "libre baskerville", "bodoni", "didot", "playfair", "playfair display",
        "cambria", "century schoolbook", "caslon", "minion", "cormorant", "merriweather", "lora", "crimson", "crimson pro", "perpetua", "goudy", "sabon", "cochin"],
      words: ["serif", "serifs", "classic", "classical", "traditional", "oldstyle"] },
    { style: "rounded", label: "rounded", prefer: ["jost", "source-sans-3"],
      names: ["arial rounded", "arial rounded mt bold", "vag rounded", "varela round", "quicksand", "nunito", "comfortaa", "fredoka"], words: ["rounded", "round", "bubble", "bubbly", "balloon", "puffy"] },
    { style: "geometric", label: "geometric sans", prefer: ["jost", "source-sans-3"],
      names: ["futura", "century gothic", "avenir", "avenir next", "montserrat", "gotham", "poppins", "josefin sans", "josefin", "raleway", "avant garde", "itc avant garde", "league spartan", "lexend", "urbanist"], words: ["geometric"] },
    { style: "comic", label: "comic", prefer: ["source-sans-3"], names: ["comic sans", "comic sans ms", "comic neue", "comic relief"], words: ["cartoon", "cartoony"] },
    { style: "bold", label: "bold sans", prefer: ["libre-franklin", "alegreya-sans", "source-sans-3"],
      names: ["impact", "arial black", "cooper black", "cooper", "bold clare", "franklin gothic", "franklin gothic heavy", "bebas neue", "anton"], words: ["bold", "bolder", "heavy", "thick", "chunky", "extrabold", "block", "blocks", "blocky"] },
    { style: "sans", label: "sans-serif", prefer: ["source-sans-3"], usual: ["plain", "simple", "clean", "normal", "standard", "default", "regular", "basic", "minimal", "minimalist", "usual", "neutral"],
      names: ["arial", "helvetica", "helvetica neue", "calibri", "verdana", "tahoma", "trebuchet", "trebuchet ms", "open sans", "roboto", "lato", "segoe ui", "gill sans", "myriad", "myriad pro", "noto sans", "ubuntu", "proxima nova", "lucida sans",
        "sans serif", "sans-serif", "san serif", "sansserif"], words: ["sans", "sanserif"] },
    // a taste, not a style: nothing is picked for these (the usual font, and the plain line says what was asked)
    { style: "vague", label: "", prefer: [], words: ["nice", "pretty", "cute", "beautiful", "lovely", "classy", "delicate", "dainty", "pleasant", "unique", "special", "different", "custom", "personal", "artistic", "creative", "fun", "funky",
      "funny", "playful", "cool", "italic", "italics", "light", "thin"] }
  ];
  /** Words that, with the word font beside them, never name a font (colours and finishes: "black font", "darkened font"). */
  const FONT_VETO = new Set("black white gold golden silver rose red blue green pink dark darker darken darkened lighter deeper deep engraved engraving size sizes smaller bigger larger small big large tiny whole same other another next previous".split(" "));
  /** Class words allowed after a bare in / with / using (a plain "in cursive please"); every other class word needs the word font or a style cue. */
  const FONT_WEAK_CLASS = new Set(["cursive", "script", "scripted", "calligraphy", "calligraphic"]);
  /** A few shop-font spellings beyond FONT_STEMS: the Comic Sans family is the shop's Comic. */
  const FONT_ALIASES = [{ id: "source-sans-3", rx: /^comic(?:sans(?:ms)?|neue|relief|book)?$/ }];
  /** Shop words that are first names as often as fonts: "ace" counts only beside the word font / typeface (strict), "angelina" beside any cue but never bare or after a bare in (strong). */
  const FONT_NAMELIKE = { ace: "strict", angelina: "strong" };
  // ── the vocabulary, folded once (fontKey: case, accents, spaces and punctuation set aside) ──
  const EXT_NAME = new Map(), CLS_WORD = new Map(), USUAL_WORD = new Set();
  FONT_STYLES.forEach(row => {
    for (const n of row.names || []) { const k = fontKey(n); if (k && !EXT_NAME.has(k)) EXT_NAME.set(k, row); }
    for (const w of row.words || []) { const k = fontKey(w); if (k && !CLS_WORD.has(k)) CLS_WORD.set(k, row); }
    for (const w of row.usual || []) { const k = fontKey(w); if (k && !CLS_WORD.has(k)) { CLS_WORD.set(k, row); USUAL_WORD.add(k); } }
  });
  /** Class words allowed beside a style cue (script, style, text, letters, writing): the unambiguous kinds only ("wedding style" is no font). */
  const FONT_CORE_CLASS = new Set("cursive script scripted scripts calligraphy calligraphic handwriting handwritten serif sans slab block blocks bold monospace monospaced brush rounded geometric blackletter fraktur".split(" "));
  const FONT_FUZZY = [...SHOP_WORDS].filter(k => k.length >= 5 && !/lowercase|capsonly|allcaps/.test(k)).map(k => ({ key: k, id: WORD_FONT.get(k) }));
  const CLS_FUZZY = [...CLS_WORD.keys()].filter(k => k.length >= 7 || (k.length >= 5 && FONT_CORE_CLASS.has(k)));   // (a long kind-of-font word may be one edit off; a short one only when it is a plain kind: "Sarif", "Serrif")
  /** The fonts of a style, best first: the first INSTALLED one that is not capitals-only. The ONE place that decides "nearest". */
  function nearestFont(style) {
    const row = FONT_STYLES.find(r => r.style === style);
    const id = row && (row.prefer || []).find(x => { const f = fontById(x); return f && !f.caps; });
    return id || DEFAULT_FONT;
  }
  const fontStyleLabel = style => { const row = FONT_STYLES.find(r => r.style === style); return row ? row.label : ""; };
  /** Optimal-string-alignment (Damerau-Levenshtein) distance of two folded words; max + 1 as soon as it is certain to exceed max. */
  function damerau(a, b, max) {
    if (a === b) return 0;
    const la = a.length, lb = b.length; if (Math.abs(la - lb) > max) return max + 1;
    let p2 = null, p1 = Array.from({ length: lb + 1 }, (_, j) => j);
    for (let i = 1; i <= la; i++) {
      const cur = [i]; let low = i;
      for (let j = 1; j <= lb; j++) {
        let v = Math.min(p1[j] + 1, cur[j - 1] + 1, p1[j - 1] + (a[i - 1] === b[j - 1] ? 0 : 1));
        if (i > 1 && j > 1 && a[i - 1] === b[j - 2] && a[i - 2] === b[j - 1]) v = Math.min(v, p2[j - 2] + 1);
        cur[j] = v; if (v < low) low = v;
      }
      if (low > max) return max + 1;
      p2 = p1; p1 = cur;
    }
    return p1[lb];
  }
  /** How many edits a word of this many letters may be off: none up to 4 letters (ace, mono), one for 5-9, two for 10 and more. */
  const fontEdits = len => (len >= 10 ? 2 : len >= 5 ? 1 : 0);
  /** The key with a plural or a possessive set aside ("scripts", "typewriters", "pristina's"): the key itself first. */
  const fontForms = k => { const v = [k]; if (k.length > 3 && /s$/.test(k)) v.push(k.slice(0, -1)); if (k.length > 4 && /es$/.test(k)) v.push(k.slice(0, -2)); return v; };
  const shopCanon = k => (/^ace/.test(k) ? "ace" : k);
  const readOf = (id, how, canon, extra) => Object.assign({ id, how, canon: canon || "", style: "", type: "shop" }, extra || {});
  const styleRead = (row, how, canon) => (row.style === "vague" ? { id: "", how: "vague", style: "vague", canon, type: "class" } : { id: nearestFont(row.style), how, style: row.style, canon, type: how === "near" ? "external" : "class" });
  /** A folded phrase as one of the SHOP's font words: exact ("pristina"), a spelling of it (the typewr... stems, Comic Sans, a plural), or - when `fuzzy` - within the edit distance of one. */
  function asShop(k, o) {
    for (const v of fontForms(k)) {
      const sid = SHOP_WORDS.has(v) ? WORD_FONT.get(v) : "";
      if (sid) return readOf(sid, v === k ? "exact" : "spelling", shopCanon(v));
      const iid = o.shopOnly ? "" : WORD_FONT.get(v);   // (the installed fonts' own names: "Jost font", "Mynerve")
      if (iid) return readOf(iid, v === k ? "exact" : "spelling", shopCanon(v));
      const st = stemFont(v) || (FONT_ALIASES.find(a => a.rx.test(v)) || {}).id || "";
      if (st) return readOf(st, "spelling", shopCanon(v));
    }
    if (o.fuzzy && k.length >= 5) {
      let best = null, tie = false; const max = fontEdits(k.length);
      for (const t of FONT_FUZZY) {
        if (t.key[0] !== k[0]) continue;
        const d = damerau(k, t.key, max); if (d > max) continue;
        if (!best || d < best.d) { best = { d, t }; tie = false; } else if (d === best.d && t.id !== best.t.id) tie = true;
      }
      if (best && !tie) return readOf(best.t.id, "spelling", shopCanon(best.t.key));
    }
    return null;
  }
  const asExternal = k => { for (const v of fontForms(k)) { const row = EXT_NAME.get(v); if (row) return styleRead(row, "near", v); } return null; };
  function asClass(k, o) {
    for (const v of fontForms(k)) { const row = CLS_WORD.get(v); if (row) return Object.assign(styleRead(row, USUAL_WORD.has(v) ? "usual" : "class", v), { core: FONT_CORE_CLASS.has(v), weak: FONT_WEAK_CLASS.has(v) }); }
    if (o && o.fuzzy && k.length >= 5) {
      let best = null, tie = false; const max = fontEdits(k.length);
      for (const t of CLS_FUZZY) { if (t[0] !== k[0]) continue; const d = damerau(k, t, max); if (d > max) continue; if (!best || d < best.d) { best = { d, t }; tie = false; } else if (d === best.d && CLS_WORD.get(t) !== CLS_WORD.get(best.t)) tie = true; }
      if (best && !tie) { const row = CLS_WORD.get(best.t); return Object.assign(styleRead(row, "class", best.t), { core: FONT_CORE_CLASS.has(best.t), weak: FONT_WEAK_CLASS.has(best.t) }); }
    }
    return null;
  }
  /** The reading of ONE phrase (keys: its folded words) beside a cue: { id, how: exact | spelling | near | class | usual | vague, style, canon, kind } or null.
   *  cue: "label" | "font" (the word font touches it) | "style" (script / style / text / letters / writing after it) | "weak" (a bare in / with / using) | "bare" (the whole line). */
  function fontName(keys, o) {
    const k = keys.join(""); if (k.length < 3 || (keys.length === 1 && FONT_VETO.has(k))) return null;   // ("black font" is a colour; "Cooper Black" is a font)
    const cue = o.cue, strong = cue === "label" || cue === "font", style = cue === "style";
    let h = asShop(k, { fuzzy: (strong || style) && (keys.length === 1 || keys.every(x => x.length >= 3)), shopOnly: cue === "weak" || cue === "bare" });   // (an initial before the name is not part of it: "A Angelina font")
    if (h) {
      const nl = FONT_NAMELIKE[h.canon];
      if (nl === "strict" && !strong) return null;
      if (nl === "strong" && !(strong || style)) return null;
      return h;
    }
    if (cue === "bare") return null;
    if (strong || keys.length >= 2) { h = asExternal(k); if (h && (strong || keys.length >= 2)) return h; }
    h = asClass(k, { fuzzy: strong });
    if (!h) return null;
    if (strong) return h;
    if (h.how === "vague" || h.how === "usual") return null;
    return cue === "weak" ? (h.weak ? h : null) : h.core ? h : null;
  }
  const FONT_CUE = new Set("font fonts typeface typefaces lettering letra letras fuente fuentes tipografia police schriftart schrift lettertype fonte carattere".split(" "));
  const FONT_STYLE_CUE = new Set("script style styles text letters letter writing".split(" "));
  const FONT_WEAK_CUE = new Set("in with using use uses choose pick select prefer".split(" "));
  const FONT_DET = new Set("the a an my your this that those some our".split(" "));
  const FONT_INTENSIFIER = new Set("really very so super extra quite too truly".split(" "));
  const FONT_NEG = new Set("no not dont don never without avoid nor except instead rather non".split(" "));
  const FONT_END = new Set("please pls plz thanks thank thx ty if for on at and but then kindly".split(" "));
  const FONT_STOPRUN = new Set("in with using use uses choose pick select prefer is are be was want wants like need would please pls and or but so then".split(" "));
  const FONT_SKIP_AFTER = new Set("is are called named of in a an the to be would like".split(" "));
  const FONT_LABEL = /^\s*(?:(?:\d{1,2}\s*[.)]|[-*•])\s*)?(?:the\s+)?(font\s*(?:choice|style|name|type|selection|option)s?|fonts?|typefaces?|lettering|letra|fuente|police|schriftart)\b\s*(?:(?:is|are|will\s+be|should\s+be|to\s+be)\b)?\s*([:=#\-–—]*)\s*(.*?)\s*$/iu;
  const FONT_TOKEN = /#?\d+|[\p{L}][\p{L}\p{N}'’]*/gu;
  const fontTokens = line => { const out = [], re = new RegExp(FONT_TOKEN.source, "gu"); for (let m; (m = re.exec(line));) out.push({ t: m[0], k: fontKey(m[0].replace(/['’]s$/i, "")), s: m.index, e: m.index + m[0].length }); return out; };
  const styleIx = h => { const i = FONT_STYLES.findIndex(r => r.style === h.style); return i < 0 ? 99 : i; };

  /** Every font request on ONE line: { hits: [{ cue, id, how, style, canon, type, ns, ne (the name), s, e (the whole request, to take out), question }], asks: [plain lines] }.
   *  o.others: the text has other non-blank lines (a bare font word counts only then). */
  function fontScanLine(line, o) {
    o = o || {}; line = String(line == null ? "" : line);
    const toks = fontTokens(line), K = toks.map(t => t.k), n = toks.length, hits = [], asks = [];
    if (!n) return { hits, asks };
    const question = /\?/.test(line), say = t => clip(String(t).trim(), 50);
    const gapOk = (a, b) => /^[\s\-–—'"“”‘’*_]*$/.test(line.slice(toks[a].e, toks[b].s));      // token a then token b, nothing but spaces (and a dash or a quote) between
    const brk = (a, b) => /[.!?;:,()\/|\\\[\]{}]/.test(line.slice(toks[a].e, toks[b].s));
    const negated = i => { for (let j = i - 1; j >= 0 && j >= i - 3; j--) { if (brk(j, j + 1)) break; if (FONT_NEG.has(K[j])) return true; } return false; };
    const joined = (a, b) => { for (let q = a; q < b; q++) if (!gapOk(q, q + 1)) return false; return true; };
    const taken = (a, b) => hits.some(h => !(b < h.i0 || a > h.i1));
    const push = (h, a, b, cueIx) => { if (taken(a, b)) return false; hits.push(Object.assign({ question, i0: a, i1: b }, h)); return true; };
    // "font #3", "font 3", "font number 3", "font no. 3": a picture on the listing, not a font name
    const numbered = c => (c + 1 < n && /^#?\d{1,2}$/.test(toks[c + 1].t)) || (c + 2 < n && /^(?:no|nr|num|number)$/.test(K[c + 1]) && /^\d{1,2}$/.test(toks[c + 2].t));

    // 1 · a label: "Font: Pristina", "font Angelina", "Font style: Stylish", "Font: As discussed in messages, Slab Serif"
    const lab = FONT_LABEL.exec(line);
    if (lab && lab[3]) {
      const sep = lab[2], rest = lab[3], at = line.lastIndexOf(rest);
      const cut = rest.search(/\s[\/|]\s|;/), value = (cut > 0 ? rest.slice(0, cut) : rest).replace(/[\s.!]+$/, "").replace(/\s*(?:please|pls|thanks?)$/i, "").trim();
      const ts = fontTokens(value), ks = ts.map(t => t.k);
      let h = null, from = 0, upto = 0;
      if (ts.length && (/^(?:#\s*|no\.?\s*|nr\.?\s*|number\s*)\d{1,2}\b/i.test(value) || (sep && /^\d{1,2}$/.test(value)) || (/#/.test(sep) && /^\d{1,2}\b/.test(value)))) asks.push(`the message names “${say(line)}”, a number on the listing's photo: a person picks the font`);
      else {
        for (let len = Math.min(5, ts.length); len >= 1 && !h; len--) { h = fontName(ks.slice(0, len), { cue: "label" }); if (h) upto = len; }
        for (let st = 1; st < ts.length && !h; st++) for (let len = Math.min(3, ts.length - st); len >= 1 && !h; len--) { const g = fontName(ks.slice(st, st + len), { cue: "label" }); if (g && g.how !== "vague") { h = g; from = st; upto = len; } }
        if (h) hits.push(Object.assign({ question, cue: "label", i0: 0, i1: n - 1, ns: at + ts[from].s, ne: at + ts[from + upto - 1].e, s: 0, e: cut > 0 ? at + cut : line.length }, h));
        else if (sep && ts.length <= 8 && !question) hits.push({ question, cue: "label", i0: 0, i1: n - 1, ns: at, ne: at + value.length, s: 0, e: cut > 0 ? at + cut : line.length, id: "", how: "none", style: "", canon: "", asked: value });
      }
    }

    // 2 · the word font beside the name: "Typewriter font", "the uppercase Ace font", "font Pristina", "a nice pretty font"
    for (let c = 0; c < n; c++) {
      if (!FONT_CUE.has(K[c]) || taken(c, c)) continue;
      let found = null;
      for (let len = Math.min(4, c); len >= 1 && !found; len--) {
        const a = c - len; if (!joined(a, c)) continue;
        const h = fontName(K.slice(a, c), { cue: "font" });
        if (h && !negated(a) && !negated(c)) found = { h, a, b: c, ns: toks[a].s, ne: toks[c - 1].e, s: toks[a].s, e: toks[c].e };
      }
      if (!found && c > 0) {   // a run of kind-of-font words: "clean all-caps block font", "in a nice pretty font"
        let a = c; while (a > 0 && c - a < 3 && gapOk(a - 1, a) && !FONT_STOPRUN.has(K[a - 1])) a--;
        let best = null, at = -1;
        for (let q = c - 1; q >= a; q--) { const w = asClass(K[q], { fuzzy: true }); if (w && !FONT_VETO.has(K[q]) && (!best || styleIx(w) < styleIx(best))) { best = w; at = q; } }
        if (best && !negated(at) && !negated(c)) found = { h: best, a, b: c, ns: toks[a].s, ne: toks[c - 1].e, s: toks[a].s, e: toks[c].e };
      }
      if (!found) {   // the name after the word: "font Pristina", "Font: Vibur", "font #3"
        if (!/^[\s:=#\-–—(\[“"']*$/.test(c + 1 < n ? line.slice(toks[c].e, toks[c + 1].s) : "")) continue;
        if (numbered(c)) { asks.push(`the message names “${say(line)}”, a number on the listing's photo: a person picks the font`); continue; }
        let q = c + 1; while (q < n && q <= c + 3 && FONT_SKIP_AFTER.has(K[q]) && joined(q - 1, q)) q++;
        for (let len = Math.min(4, n - q); len >= 1 && !found; len--) {
          if (q + len > n || !joined(q, q + len - 1)) continue;
          const h = fontName(K.slice(q, q + len), { cue: "font" });
          if (h && h.how !== "vague" && h.how !== "usual" && !negated(c)) found = { h, a: c, b: q + len - 1, ns: toks[q].s, ne: toks[q + len - 1].e, s: toks[c].s, e: toks[q + len - 1].e };
        }
      }
      if (found && found.a > 0) {   // the taste words in front of a name belong to the request: "a really nice pretty script font", "the nice Pristina font"
        let a = found.a, swallowed = false;
        while (a > 0 && found.b - a < 5 && gapOk(a - 1, a) && (FONT_INTENSIFIER.has(K[a - 1]) || ((CLS_WORD.get(K[a - 1]) || {}).style === "vague" && !FONT_VETO.has(K[a - 1])))) { a--; swallowed = true; }
        const words = a;
        if (swallowed && a > 0 && FONT_DET.has(K[a - 1]) && gapOk(a - 1, a)) a--;
        if (a < found.a && !negated(a)) { found.a = a; found.s = toks[a].s; if (found.h.how === "vague") found.ns = toks[words].s; }   // (a taste is asked in all its words: "nice pretty")
      }
      if (found && push(Object.assign({ cue: "font", ns: found.ns, ne: found.ne, s: found.s, e: found.e }, found.h), found.a, found.b)) { /* taken */ }
    }

    // 3 · the name before a style word: "Angelina script", "Pristina Style", "Cursive text", "block letters"
    for (let c = 1; c < n; c++) {
      if (!FONT_STYLE_CUE.has(K[c]) || taken(c, c)) continue;
      for (let len = Math.min(3, c + 1); len >= 2; len--) {   // (a font NAME that ends in the style word: "Brush Script", "Edwardian Script ITC" is read whole)
        const a = c - len + 1; if (a < 0 || !joined(a, c) || taken(a, c)) continue;
        const h = asExternal(K.slice(a, c + 1).join("")); if (h && !negated(a) && push(Object.assign({ cue: "style", ns: toks[a].s, ne: toks[c].e, s: toks[a].s, e: toks[c].e }, h), a, c)) break;
      }
      if (taken(c, c)) continue;
      for (let len = Math.min(3, c); len >= 1; len--) {
        const a = c - len; if (!joined(a, c) || taken(a, c)) continue;
        const h = fontName(K.slice(a, c), { cue: "style" });
        if (h && !negated(a) && push(Object.assign({ cue: "style", ns: toks[a].s, ne: toks[c - 1].e, s: toks[a].s, e: toks[c].e }, h), a, c)) break;
      }
    }

    // 4 · a bare in / with / using + a name that ENDS the phrase: "in Comic Sans please", "Evie in cursive", "using Vibur."
    for (let w = 0; w < n; w++) {
      if (!FONT_WEAK_CUE.has(K[w]) || taken(w, w)) continue;
      let q = w + 1; while (q < n && FONT_DET.has(K[q]) && joined(q - 1, q)) q++;
      if (q >= n || (!joined(w, w + 1) && q === w + 1)) continue;
      let adj = 0; while (adj < 2 && q + adj < n - 1 && (CLS_WORD.get(K[q + adj]) || {}).style === "vague" && joined(q + adj, q + adj + 1)) adj++;   // ("in a pretty cursive": the taste words before the style are part of the request)
      for (let len = Math.min(3, n - q - adj); len >= 1; len--) {
        const first = q + adj, last = first + len - 1; if (!joined(first, last) || taken(w, last)) continue;
        const h = fontName(K.slice(first, last + 1), { cue: "weak" });
        if (!h || negated(w)) continue;
        if (!(last === n - 1 || FONT_END.has(K[last + 1]) || brk(last, last + 1))) continue;
        if (push(Object.assign({ cue: "weak", ns: toks[first].s, ne: toks[last].e, s: toks[w].s, e: toks[last].e }, h), w, last)) break;
      }
    }

    // 5 · the whole line is one of the shop's font words (and other lines carry the words)
    if (!hits.length && o.others) {
      let a = 0; if (n > 1 && /^\d{1,2}$/.test(toks[0].t) && /^\s*[.)]\s*$/.test(line.slice(toks[0].e, toks[1].s))) a = 1;   // ("3. Typewriter")
      const keys = K.slice(a); const lead = line.slice(0, toks[a].s), tail = line.slice(toks[n - 1].e);
      if (keys.length && keys.length <= 3 && joined(a, n - 1) && /^[\s\-–—*•"'“”‘’(]*$/.test(lead) && /^[\s.!,\-–—*"'“”‘’)]*$/.test(tail)) {
        const h = fontName(keys, { cue: "bare" });
        if (h) hits.push(Object.assign({ question, cue: "bare", i0: a, i1: n - 1, ns: toks[a].s, ne: toks[n - 1].e, s: 0, e: line.length }, h));
        else { const sh = asShop(keys.join(""), { shopOnly: true }); if (sh && FONT_NAMELIKE[sh.canon]) asks.push(`“${say(line)}” is alone on a line: a name or the ${sh.canon === "ace" ? "Ace" : "Angelina"} font? It stays in the words; a person checks`); }
      }
    }
    hits.sort((x, y) => x.i0 - y.i0);
    return { hits, asks };
  }

  const FONT_EDGE_L = /^(?:[\s,;:.!\-–—|\/&+*"'“”‘’()\[\]]|(?:please|pls|plz|kindly|thanks|thx|and|in|with|using|use)\b)+/i;
  const FONT_EDGE_R = /(?:[\s,;:.!\-–—|\/&+*"'“”‘’()\[\]]|\b(?:please|pls|plz|kindly|thanks|thx|and|in|with|using|use|the|for|on|at|to|of)|\bthank\s*you)+$/i;
  const FONT_SENT = new Set("i i'd id i'll please pls engrave engraved engraving write written put add make want wanted like would could can you name names word words letter letters initial initials is are be will text".split(" "));
  /** A line's leftover after a request is taken out: still words to engrave ("D", "Evie", "Sarah & Tom") and not a sentence or an instruction. */
  const fontCleanRest = r => { if (r === "I") return true; /* (the initial I: "I font Stylish") */ const ts = r.toLowerCase().split(/\s+/).map(x => x.replace(/^[^\p{L}\p{N}']+|[^\p{L}\p{N}']+$/gu, "")).filter(Boolean); return r.length <= 45 && ts.length > 0 && ts.length <= 5 && !ts.some(t => FONT_SENT.has(t)) && !/\b(?:font|typeface|script|cursive|capitals?|uppercase|lowercase|caps|centre?d?|cent(?:er|re))\b/i.test(r); };
  const fontFiller = r => { const w = r.toLowerCase().replace(/[^\p{L}\p{N}' ]+/gu, " ").split(/\s+/).filter(Boolean); return w.length > 0 && w.every(x => new Set("please pls kindly can could would you i id i'd to have has put place write add engrave engraved engraving the my this that following text words word wording on at in for it and thanks thank thx want like need use using with a an be is".split(" ")).has(x)) && !(w.length === 1 && (w[0] === "i" || w[0] === "id" || w[0] === "a")); };
  /** The font requests of a free text and the text without them: { text, lines, requests: [{ asked, id, how, style, canon, cue, line, quote }], asks: [plain lines], removed: [quotes], changed }.
   *  A line that is only a request goes; a request at the edge of a short line of words ("D font Stylish", "Evie in cursive please") is cut out of it; a request inside a sentence stays where it is (the font is
   *  read, the sentence is a person's or the paid reader's to settle). o.keepFaceLines: a line that names the front or the back is left to the face reader; o.cueOnly: a bare font word on a line of its own is not read (the paid reader's own words). Text with no request is answered unchanged. */
  function fontReadText(text, o) {
    o = o || {}; const raw = String(text == null ? "" : text), src = raw.replace(/\r/g, "").split("\n");
    const others = !o.cueOnly && src.filter(l => l.trim()).length > 1, out = [], requests = [], asks = [], removed = []; let changed = false;
    src.forEach((line, ix) => {
      if (!line.trim()) { out.push(line); return; }
      const sc = fontScanLine(line.length > 1500 ? line.slice(0, 1500) : line, { others });   // (a long message is read as far as its first 1,500 letters of a line)
      for (const a of sc.asks) asks.push(a);
      const use = [];
      for (const h of sc.hits) {
        if (h.question) { asks.push(`the buyer asks about a font (“${clip(line.trim(), 50)}”): the shop answers`); continue; }
        use.push(h);
        requests.push({ asked: h.asked != null ? h.asked : line.slice(h.ns, h.ne).trim(), id: h.id, how: h.how, style: h.style || "", canon: h.canon || "", cue: h.cue, line: ix, quote: clip(line.trim(), 60) });
      }
      if (!use.length) { out.push(line); return; }
      // the request, with the in / using / please that leads into it and the please / thanks that follows
      let rest = line;
      for (const h of use.slice().sort((a, b) => b.s - a.s)) {
        let s = h.s, e = h.e;
        if (h.cue !== "label" && h.cue !== "bare") {
          const head = rest.slice(0, s).replace(/\s+$/, ""), m = /(?:^|[\s,;])((?:(?:please|pls|plz|kindly)\s+)?(?:in|with|using|use|uses|choose|pick|select|prefer)\s+(?:(?:the|a|an|my|your)\s+)?)$/i.exec(head + " ");
          if (m) s = Math.max(0, head.length + 1 - m[1].length);
          const t = /^\s*(?:,?\s*(?:please|pls|plz|kindly|thanks?|thank\s*you|thx))?[\s.!]*/i.exec(rest.slice(e)); if (t) e += t[0].length;
        }
        rest = rest.slice(0, s) + " " + rest.slice(e);
      }
      rest = rest.replace(/[ \t]+/g, " ").replace(FONT_EDGE_L, "").replace(FONT_EDGE_R, "").trim().replace(/^(?:\d{1,2}\s*[.)]?|[-*•])$/, "");   // ("3. Typewriter Font": the list number goes with it)
      const faced = o.keepFaceLines && /(?:^|[^\p{L}\p{N}_])(?:front|back)(?![\p{L}\p{N}_])/iu.test(line);
      if (/^(?:a|an|the)$/.test(rest) && use.every(h => h.type === "class")) rest = "";   // ("a nice font": the article goes with it; an initial after a font NAME stays)
      if (!rest || fontFiller(rest)) { removed.push(clip(line.trim(), 60)); changed = true; }
      else if (!faced && fontCleanRest(rest)) { out.push(rest); removed.push(clip(line.trim(), 60)); changed = true; }
      else out.push(line);
    });
    let outText = raw;
    if (changed) outText = out.join("\n").replace(/^(?:[ \t]*\n)+/, "").replace(/(?:\n[ \t]*)+$/, "").replace(/\n[ \t]*(?:\n[ \t]*){2,}/g, "\n\n");
    return { text: outText, lines: outText.split("\n").map(x => x.trim()).filter(Boolean), requests, asks, removed, changed };
  }
  /** A font phrase the paid reader or a face gave ("cursive", "Typewriter font", "Courier New", "Pristine"): { id, how, style, canon } - id "" when nothing can be chosen. Exact readings are the shop's (fontOfPhrase). */
  function fontResolve(phrase) {
    const s = String(phrase == null ? "" : phrase).trim(); if (!s) return { id: "", how: "none", style: "", canon: "" };
    const exact = fontOfPhrase(s); if (exact) return { id: exact, how: "exact", style: "", canon: "" };
    const ts = fontTokens(s).filter(t => !FONT_CUE.has(t.k)), ks = ts.map(t => t.k);
    for (let len = Math.min(5, ks.length); len >= 1; len--) for (let st = 0; st + len <= ks.length; st++) { const h = fontName(ks.slice(st, st + len), { cue: "label" }); if (h && h.how !== "vague") return h; }
    const vague = ks.map(k => asClass(k, { fuzzy: true })).find(h => h && h.how === "vague");
    return vague || { id: "", how: "none", style: "", canon: "" };
  }
  /** The font the buyer's own words ask for, as spec.fontText: { asked, id, name, how, style, from, quote, cue, also?, asks? } or null when the words name no font and raise no question.
   *  Read in this order: the staff note (the shop's own word), the personalisation box, the buyer's message; the first one that names a font decides, the others are listed in `also`. */
  function fontTextOf(spec) {
    if (!spec || spec.noDesign) return null;
    const pools = [["staffNote", spec.staffNote], ["personalization", (spec.personalization || []).join("\n")], ["buyerMessage", spec.buyerMessage]];
    let main = null; const also = [], asks = [];
    for (const [from, txt] of pools) {
      if (!txt || !String(txt).trim()) continue;
      const r = fontReadText(visible(txt));
      for (const a of r.asks) if (!asks.includes(a)) asks.push(a);
      for (const q of r.requests) {
        if (!main) { if (q.id || q.asked) main = Object.assign({}, q, { from }); }
        else if (q.asked && fontKey(q.asked) !== fontKey(main.asked) && !also.includes(q.asked)) also.push(q.asked);
      }
    }
    if (!main && !asks.length) return null;
    if (!main) return { asked: "", id: "", name: "", how: "ask", asks };
    const f = main.id ? fontById(main.id) : null;
    return Object.assign({ asked: main.asked, id: f ? f.id : "", name: f ? f.name : "", how: main.id ? main.how : main.how === "vague" ? "vague" : "none", style: main.style, from: main.from, quote: main.quote, cue: main.cue },
      also.length ? { also: also.slice(0, 3) } : {}, asks.length ? { asks: asks.slice(0, 3) } : {});
  }

  /** { field, value, source } for one option, or null when nothing deterministic applies. */
  function optionLookup(maps, listingId, name, value) {
    const n = norm(name), v = norm(value);
    const tryMap = (m, src) => { if (!m) return null; const byName = m[n] || m[Object.keys(m).find(k => norm(k) === n)]; if (!byName) return null; const hit = byName[v] || byName[Object.keys(byName).find(k => norm(k) === v)]; return hit ? Object.assign({ source: src }, hit) : null; };
    return tryMap(maps && maps[String(listingId)], "listing") || tryMap(maps && maps["*"], "shop") || tryMap(DEFAULT_OPTION_MAP["*"], "default");
  }

  /* ═══ 3 · SKUs that are not charms ═════════════════════════════════════ */
  /** noDesign = { patterns: [regex source strings], skus: [strings] } — Charm_Sku_NoDesign flattened. */
  function isNoDesign(sku, noDesign) {
    const s = upSku(sku); if (!s) return false;
    if ((noDesign && noDesign.skus || []).some(x => upSku(x) === s)) return true;
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
  const huggieSku = (raw, masterEntry) => { const s = upSku(raw).replace(/\s+/g, " "); if (!s || /\(HUGGIE\)$/.test(s)) return s;
    const base = variationBase(s), real = !!base && !!masterEntry && !!(masterEntry(s) || masterEntry(s + " (HUGGIE)")); return (base && !real ? base : s) + " (HUGGIE)"; };
  const variationBase = raw => { const m = /^(.+?)[\s_-]+CO$/i.exec(String(raw || "").trim()); return m ? m[1].trim().toUpperCase() : ""; };
  /* CHAMMAP (Paul, 12 Oct 2026, order 4199572384): the Etsy listing 4494184284 "Chameleon Charm Necklace" is a necklace that was read as an ASTRONAUT. Its charm-only products carry the shop's SHARED placeholder SKU
     "CO-GoldCharmO" (the same five, CO-GoldCharmO / CO-SilverChar / CO-RoseCharmO / CO-10kSolidCh / CO-14kSolidCh, are on the Wolf Face listing 4494182906 too; "CO" = charm only, then the metal and "Charm O(nly)",
     cut at 13 characters), and the master holds the label "CO-GOLDCHARMO" on the astronaut's drawing (also "CHARM" and "SILVER CHARM", labels on other drawings). The Etsy SKU matched that master entry
     exactly, at once, and nothing asked whether such a name could name ONE design: it cannot, it is on listings of every design. genericSku says what such a SKU looks like (the charm-only mark "CO", a
     separator, metal words, then "charm" or the start of "charm only"; or only metal words and the word "charm"); resolveSku never takes it as an automatic match (see there): the line takes the listing's
     saved answer for that SKU, or waits for a person with one plain line. Exact words only: "COBRA 1", "CO-OWL", "CO-ROSE", "ROSE CHARM", "BEADY CHARM" are designs. */
  const GENERIC_METAL = "(?:GOLD|SILVER|ROSEGOLD|ROSE|RG|YG|GF|SS|VERMEIL|STERLING|SOLID|FILLED|\\d{1,2}K)";
  const GENERIC_WITH_CO = new RegExp("^" + GENERIC_METAL + "*CH(?:A(?:R(?:M(?:S|O(?:N(?:LY?)?)?)?)?)?)?$"), GENERIC_NO_CO = /^(?:GOLD|SILVER|ROSEGOLD|RG|YG|GF|SS|VERMEIL|STERLING|SOLID|FILLED|\d{1,2}K)*(?:CHARMS?|CHARMONLY|CHARMO)$/;
  const genericSku = raw => { const s = upSku(raw), m = /^CO[\s_.-]+(.+)$/.exec(s); return !s ? false : m ? GENERIC_WITH_CO.test(m[1].replace(/[\s_.-]+/g, "")) : GENERIC_NO_CO.test(s.replace(/[\s_.-]+/g, "")); };
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
  const upSku = s => decodeEntities(s).trim().toUpperCase();   // (decoded before the capitals: "&#x27;" must not become "&#X27;")
  /** The answer a person saved under a SKU in a map keyed by SKU (aliases bySku, pairBySku): under the SKU itself, else under a key that is the same SKU saved while it still carried an entity. */
  const bySkuOf = (map, sku) => { if (!map || typeof map !== "object" || !sku) return undefined; if (map[sku] !== undefined) return map[sku]; for (const k of Object.keys(map)) if (k.indexOf("&") >= 0 && upSku(k) === sku) return map[k]; return undefined; };
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
  /* KNOWN_SKU_NAMES (MUSICSKU, 11 Oct 2026): the ONE definition of an Etsy SKU that is a design's plain SKU with words after it, which Paul named as that design.
     Order 4198591262 carries "MUSIC4 - SINGLE W/ DOUBLE LINE" (a necklace charm, Silver, 14"); the master design is the music note MUSIC4 (it was "MUSIC 4" until Paul
     asked for the Etsy spelling). An exact entry, compared as every SKU is (looseKey: case, spacing and punctuation set aside, nothing fuzzier). It is a table of exact
     SKUs and NOT a rule "<master SKU> - <words>": the master itself shows the words after " - " usually name ANOTHER design (of the 90 master SKUs written "<master SKU> - <words>",
     61 are not the size of the SKU before the dash; MUSIC3 and "MUSIC3 - DOUBLE MUSIC NOTE" are two designs), so another SKU of the family (MUSIC4 - DOUBLE NOTE) still waits for a person.
     It stands only when the master really holds the target, only for a SKU that is exactly that one, and after a person's saved answer (an alias, an option map); nothing stored is rewritten. */
  const KNOWN_SKU_NAMES = Object.freeze({ "MUSIC4 - SINGLE W/ DOUBLE LINE": "MUSIC4" });
  const NAME_IX = new Map(Object.keys(KNOWN_SKU_NAMES).map(k => [looseKey(k), upSku(KNOWN_SKU_NAMES[k])]));
  /** The master SKU a SKU is when it is exactly a named one (KNOWN_SKU_NAMES), else "" (the master is not asked). */
  const knownName = sku => NAME_IX.get(looseKey(sku)) || "";
  /** The master's SKU for a SKU that is exactly a named one ("" when it is not, or the master does not hold the design: MUSIC4, else the one master SKU that reads the same). */
  const namedSku = (sku, masterEntry, masterLoose) => { const t = knownName(sku); return t ? spelling(t, masterEntry, masterLoose) : ""; };
  /** The master's SKU a SKU stands for ("" when the master holds none): its own spelling, else the right spelling of a known typo, else the design Paul named for it. */
  const masterSku = (sku, masterEntry, masterLoose) => spelling(sku, masterEntry, masterLoose) || typoSku(sku, masterEntry, masterLoose) || namedSku(sku, masterEntry, masterLoose);
  /** Are two SKUs one drawing in the library? The same SKU, or two the master holds that share one design file (a design sold under two names: "MUSIC 4" and "MUSIC4", the 227 spellings
   *  the library already keeps twice). An Etsy update that moves a line from one name to the other is not a change of design: nothing is cut, asked or un-approved for it. */
  const designFile = e => (e ? e.aiPath || (Object.values(e.sizes || {}).find(z => z && z.aiPath) || {}).aiPath || "" : "");
  const sameDesign = (a, b, masterEntry) => { const x = upSku(a), y = upSku(b); if (x === y) return true; if (!x || !y || !masterEntry) return false; const f = designFile(masterEntry(x)); return !!f && f === designFile(masterEntry(y)); };
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
  /* ── a person's pick remembers WHICH PRODUCT of the listing it was for (REVLINK, 13 Oct 2026) ────────────────────────────────────────────────
     A pick for a variation whose SKU Etsy's inventory table gave (viaInventory) is saved under that SKU (aliases bySku[Y]); aliasPut ALSO records which product it was: byVia["P"+product id] and
     byVia["O"+the option ids] = Y. A NEW order of that product, read by a page that does not hold Etsy's table (nobody has asked for it, or the copy is older than the page keeps), is then read as if
     the table had given Y: the saved link stands over the shared SKU the transaction carries, with no Etsy call and no extra read (the alias documents are already loaded). Never a guess: a product no
     link names is not read by another product's link. A line that names no product at all (no product id, no option ids, no option but a personalisation) is a listing of ONE product: when exactly one
     link was ever saved for the listing, no listing-wide or huggie answer stands beside it, and the line has NO SKU of its own (a SKU of its own is a variation's: it is never given another's link), that link is its link. */
  const linkViaKeys = line => {
    const out = [], pid = idOf(line && (line.productId != null ? line.productId : line.product_id)), opts = line ? varIds(line).map(pairKey).sort().join("|") : "";
    if (pid) out.push("P" + pid);
    if (opts) out.push("O" + opts);
    return out;
  };
  function linkedSku(line, a) {
    if (!line || !a || typeof a !== "object") return "";
    const bySku = a.bySku && typeof a.bySku === "object" ? a.bySku : null, keys = linkViaKeys(line);
    if (keys.length) {   // the product is named: only a link that names the same product is its link
      const via = a.byVia && typeof a.byVia === "object" ? a.byVia : null; if (!via || !bySku) return "";
      for (const k of keys) { const y = upSku(via[k]); if (y && bySkuOf(bySku, y) !== undefined) return y; }
      return "";
    }
    const named = (line.variations || []).some(v => { const nm = v && (v.name || v.formatted_name); return !!nm && !isPersonalisation(nm) && String(v.value != null ? v.value : v.formatted_value || "").trim() !== ""; });
    const only = bySku ? [...new Set(Object.keys(bySku).map(upSku))] : [], tx = upSku(line.sku);
    if (named || only.length !== 1 || a.sku || a.huggie || (a.pairBySku && Object.keys(a.pairBySku).length)) return "";
    if (tx) return "";   // (a SKU of its own is a variation of its own: "Use this charm" is the variation's, another SKU of the listing is asked about, never given it)
    return only[0] || "";
  }
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
  /* ── an option's OWN SKU, told without Etsy's table (FLAGWHY, 11 Oct 2026) ───────────────────────────────────────────
     Paul, on 4197589560 ZODIAC_CONSTE_GEM-7 "Zodiac Sign: Scorpio", 4195168363 ZODIAC_EARRINGS-3 "Cancer" and 4194545985 LETTER_EARRING-19 "Letter: T", all
     "REVIEW REQUIRED: option not mapped, Etsy's SKU list is not loaded yet", with the right charm already on each row: "These 3 were chosen correctly".
     The transaction's SKU was a master design and the product's own (Etsy puts the SKU of the variation bought on the transaction), but the option was
     only ever answered by the listing's inventory table (tiesToOption), which the page does not hold until a line waits and Etsy has been asked. A master
     SKU on the transaction settles the option when it is demonstrably the OPTION's own and not the one SKU the whole listing shares:
       · index: it is `BASE-n` / `BASE_n` and the master holds the numbered family BASE-0, BASE-1, BASE-2… (zodiac 0-11, letters 0-25, runes 0-23, birth flowers,
         goddesses: the shop numbers one design per drop-down value), read from the master itself, never from a list of listings;
       · seen: the orders the page already holds for the listing carry a different SKU for each value of the option (this value's SKU is on no other value).
     Never when something says the SKU is shared: the same SKU on two values of the option in the orders the page holds, or in the listing's table
     (held by several products, or the listing's only SKU). Never for a SKU that is not a master design, never for a SKU of another family than the line
     (a necklace pendant on an earrings line), never over a person's saved pick, and only when exactly ONE option of the line is still unread (two could be
     two charms: a person says). The line's own SKU is the design either way; this only decides whether the option still has to be asked. */
  const INDEXED_SKU = /^(.+?)([-_])(\d{1,3})$/;
  /** The master family a numbered SKU belongs to ({ base: "ZODIAC_EARRINGS-", n: 3 }), when the master holds that base numbered 0, 1 and 2 (else null). */
  function skuIndexFamily(sku, masterEntry) {
    const m = INDEXED_SKU.exec(upSku(sku)); if (!m || !masterEntry || String(+m[3]) !== m[3]) return null;
    const base = m[1] + m[2];
    for (let i = 0; i < 3; i++) if (!masterEntry(base + i)) return null;
    return { base, n: +m[3] };
  }
  /* ── letters (KEYLETTER, 11 Oct 2026) ─────────────────────────────────────────────────────────────────────────────────
     Paul, on 4196617547 (listing 1099225598 "Key Necklace … Key to My Heart", SKU KEY_5598, "Initial: A" of an A to Z drop-down): "You are not correctly mapping these letter
     options to the SKU's. In this case the user chose the letter "A". so the Key with the SKU should have the letter "A" in it already." The row said "option not mapped, Etsy's
     SKU list … is not loaded yet". An option value that is ONE letter names the letter, and the shop numbers its letter designs in a master family of 26 (LETTER_EARRING-0 … -25,
     INITIAL_NECKLACE_SCRIPT-0 … -25: 0 = A, 19 = T), so the letter finds its design by its place in the alphabet, from what the line already says, never from a list of listings:
       1 · the line's own SKU is a member of such a family (the SKU Etsy keeps for the product bought): that is the letter's design when the member is the letter's own (FLAGWHY's
           rule settles the option); the SKU of ANOTHER letter ("LETTER_EARRING-3" on "T") is a clash, said in one line and never cut;
       2 · the line's SKU is the base of such a family (KEY_5598 with KEY_5598-0 … -25 in the master) or has a sibling named by the letter (KEY_5598-A, KEY_5598_A): that member is the design;
       3 · a person's earlier answer for ANOTHER letter of the same option on the listing was that letter's own member of one family (A → INITIAL_NECKLACE_SCRIPT-0): the rest of the alphabet follows;
     nothing is guessed beyond that: a letter whose design the master does not hold waits with one plain line ("no master design for letter A on this listing"), and suggestCharms offers the
     master's letter families for it first. A look-alike (a heart key that happens to show an H) is never taken for another letter. The option must be a letter option: named for one
     (Initial, Letter, Monogram, Alphabet), or on a line whose SKU or whose saved answers are in a letter family; "Size: S" and every other option an earlier rule reads are not touched. */
  const LETTER_NAME = /\b(?:initials?|letters?|monograms?|alphabet)\b/i;
  /** The one character an option value is ("A", "t", "7", '"T"', "Letter: T", "initial: S" followed by Etsy's object mark), else "". */
  const blankChar = c => { const k = c.charCodeAt(0); return k === 0xa0 || (k >= 0x200b && k <= 0x200f) || k === 0x2028 || k === 0x2029 || k === 0xfeff || k === 0xfffc || k === 0xfffd; };   // (no-break space, zero-width marks, Etsy's object mark after typed text)
  function oneChar(value) {
    let t = Array.from(optionText(value)).map(c => blankChar(c) ? " " : c).join("").trim();
    t = t.replace(/^(?:the\s+)?(?:initials?|letters?|monograms?|alphabet|number|digit)\s*[:=-]?\s*/i, "").replace(/^[\s"'“”‘’(\[]+|[\s"'“”‘’)\].,;:!]+$/g, "");
    return t.length === 1 ? t : "";
  }
  /** 0 to 25 for an option value that is exactly one letter (any case), else -1. */
  const letterIndex = value => { const t = oneChar(value); return /^[A-Za-z]$/.test(t) ? t.toUpperCase().charCodeAt(0) - 65 : -1; };
  const letterOfIndex = i => String.fromCharCode(65 + i);
  /** The master's 26-letter family a SKU is a member of or the base of: { base, n, own } (n: its own place, own: it is a member), when the master holds at least 25 of BASE0 … BASE25 and nothing past 25
   *  (zodiac 12, runes 24 and birth flowers 30 are not letter families). */
  function letterFamily(sku, me) {
    const s = upSku(sku); if (!s || !me) return null;
    const full = base => { let held = 0; for (let i = 0; i < 26; i++) if (me(base + i)) held++; return held >= 25 && !me(base + 26) && !me(base + 27); };
    const m = INDEXED_SKU.exec(s);
    if (m && String(+m[3]) === m[3] && full(m[1] + m[2])) return { base: m[1] + m[2], n: +m[3], own: true };
    for (const sep of ["-", "_"]) if (full(s + sep)) return { base: s + sep, n: -1, own: false };
    return null;
  }
  /** What the line says about a letter option: null (not a letter option, or the line's SKU is already that letter's own member: nothing to add), { sku, how, letter } (that member is the design:
   *  "base" | "named" | "said"), or { sku: "", letter, why, clash?: { has, want } } (no design: one plain line). sku: the line's design SKU (master spelling, "" when none); listingMap: the
   *  option maps saved for the listing ({ option: { value: { field, value } } }). */
  function letterRead(name, value, sku, me, listingMap) {
    const idx = letterIndex(value); if (idx < 0 || !me) return null;
    const L = letterOfIndex(idx), s = upSku(sku), fam = letterFamily(s, me);
    // a person's answers for other letters of this option: each the right member of one family
    const n = norm(name), byName = listingMap && (listingMap[n] || listingMap[Object.keys(listingMap).find(k => norm(k) === n)]), said = new Set();
    for (const [k, hit] of Object.entries(byName || {})) { const j = letterIndex(k); if (j < 0 || !hit || hit.field !== "design" || !hit.value) continue; const f = letterFamily(hit.value, me); if (f && f.own && f.n === j) said.add(f.base); }
    if (!LETTER_NAME.test(String(name || "")) && !fam && !said.size) return null;
    const none = why => ({ sku: "", letter: L, why });
    if (fam && fam.own) {
      if (!me(s)) return none(`no master design for letter ${L} on this listing (${s} is not in the master)`);
      if (fam.n === idx) return null;                                                    // (the SKU is this letter's own member)
      const want = fam.base + idx;
      return Object.assign(none(`SKU ${s} is the letter ${letterOfIndex(fam.n)} but the buyer chose ${L}: a person says`), { clash: { has: s, want: me(want) ? want : "" } });
    }
    if (fam) { const t = fam.base + idx; return me(t) ? { sku: t, how: "base", letter: L } : none(`no master design for letter ${L} on this listing (${t} is not in the master)`); }
    const named = s ? ["-", "_", " "].map(sep => s + sep + L).filter(k => me(k)) : [];
    if (named.length === 1) return { sku: named[0], how: "named", letter: L };
    if (said.size === 1) { const t = [...said][0] + idx; return me(t) ? { sku: t, how: "said", letter: L } : none(`no master design for letter ${L} on this listing (${t} is not in the master)`); }
    // a line whose SKU is a master design that is no letter's (KEY_5598) has nothing more to wait for; one with no SKU or a SKU no master holds may still be answered by Etsy's SKU list: the old wording stays
    return s && me(s) ? none(`no master design for letter ${L} on this listing`) : null;
  }
  /** What the page's own lines say about each listing's SKUs: { listingId: { rows: [{ sku, v: { option name: value } }], sig } } (names and values as optionLookup reads them; lines with no SKU left out). */
  function seenSkus(lines) {
    const by = {};
    for (const l of lines || []) {
      const sku = l && l.listingId ? upSku(l.sku) : ""; if (!sku) continue;
      const v = {}; for (const x of l.variations || []) { const n = norm(lvName(x)), val = norm(lvValue(x)); if (n && val) v[n] = val; }
      const id = String(l.listingId), e = by[id] || (by[id] = { rows: [], keys: new Set() }), k = sku + "\u0001" + JSON.stringify(v);
      if (!e.keys.has(k)) { e.keys.add(k); e.rows.push({ sku, v }); }
    }
    for (const id of Object.keys(by)) { by[id].sig = [...by[id].keys].sort().join("\u0002"); delete by[id].keys; }
    return by;
  }
  /** What the page's lines of this listing say about a SKU against one option's value: "shared" (the SKU is on another value of the option), "follows" (every line with
   *  this value has this SKU, no other value has it, and another value has another SKU), or "" (nothing to say). */
  function seenVerdict(seen, listingId, name, value, sku) {
    const e = seen && seen[String(listingId)], s = upSku(sku); if (!e || !s) return "";
    const n = norm(name), x = norm(value); let shared = false, varies = false, other = false;
    for (const r of e.rows) {
      const rx = r.v[n]; if (rx == null) continue;
      if (rx !== x && r.sku === s) shared = true;
      else if (rx === x && r.sku !== s) varies = true;
      else if (rx !== x && r.sku !== s) other = true;
    }
    return shared ? "shared" : varies ? "" : other ? "follows" : "";
  }
  /** The listing's table says this SKU is the listing's own: its only SKU, or on several products. */
  const tableShares = (table, sku) => !!table && typeof table === "object" && !table.gone && (Array.isArray(table.products) ? listingWide(table, upSku(sku)) : !!table.uni && upSku(table.uni) === upSku(sku));
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
    // (KEYLETTER: a value that is ONE character, "A" or "7", is a real value: it was dropped by the three-character filter, so a letter could never be matched. It matches a master SKU named exactly that character
    // and nothing looser, and a letter also finds its place in the master's 26-letter families, below)
    const wordsOf = t => upSku(t).split(/[^A-Z0-9]+/).filter(Boolean), one = oneChar(o.value).toUpperCase(), keep = w => (w.length >= 3 && !/^\d+$/.test(w)) || (one && w === one);
    const value = optionText(o.value), vw = one ? [one] : wordsOf(norm(value)).filter(keep), lead = one ? "" : wordsOf(norm(value).split(/\s+[-–—:]\s+|\s*[:(]\s*/)[0]).filter(keep)[0] || "";
    const found = new Map(), put = (sku, rank, why) => { const k = upSku(sku); if (!k) return; const was = found.get(k); if (!was || rank < was.rank) found.set(k, { sku: k, rank, why }); };
    for (const x of o.picks || []) put(x.sku, x.conflict ? 3 : 0, x.why);
    const list = [...(o.skus || [])];
    if (vw.length) for (const k of list) {
      const w = wordsOf(k), plain = w.filter(x => x !== "HUGGIE"), set = new Set(w);
      if (plain.length === vw.length && vw.every(x => plain.includes(x))) put(k, 1, "named exactly like the option");
      else if (one) continue;
      else if (vw.every(x => set.has(x))) put(k, 2, "its name holds the option's words");
      else if (lead && lead.length >= 4 && set.has(lead) && vw.length > 1 && o.lead !== false) put(k, 4, `its name holds “${lead.toLowerCase()}”`);   // (o.lead false: the SKU-like list of REVSUGGEST, charm-nest-siblings.js similar, never a one-word guess)
    }
    // a letter: its member of every 26-letter family the master holds that is named for letters (INITIAL_*, LETTER_*), best fitting the line first (below)
    const li = letterIndex(o.value);
    if (li >= 0) {
      const have = new Set(list.map(upSku)), me = k => (have.has(k) ? { sku: k } : null), seenBase = new Set();
      for (const k of have) {
        const m = INDEXED_SKU.exec(k); if (!m || !/INITIAL|LETTER|ALPHA|MONOGRAM/.test(m[1])) continue;
        const base = m[1] + m[2]; if (seenBase.has(base)) continue; seenBase.add(base);
        const f = letterFamily(base + "0", me); if (f && have.has(base + li)) put(base + li, 1, `the letter ${letterOfIndex(li)} of the ${base}0 to ${base}25 designs`);
      }
    }
    const bad = k => (skuFamilyConflict(k, o.line) ? 2 : 0) + (/\(HUGGIE\)\s*$/i.test(k) && !huggieLine ? 1 : 0);
    return [...found.values()].sort((a, b) => a.rank - b.rank || (a.rank ? bad(a.sku) - bad(b.sku) : 0) || a.sku.length - b.sku.length || a.sku.localeCompare(b.sku)).slice(0, max).map(x => ({ sku: x.sku, why: x.why }));
  }
  /* ═══ 3b · THE FORM GUARD (HUGGIEGUARD, 12 Oct 2026) ═════════════════════════════════════════════════════════════════════
     Paul, on order 4198409468 (Etsy SKU "SCHRODINGER CAT", Jewellery: Necklace, Silver, 18"): "This order is a dual piece/charm necklace and the SKU you currently have associated with is
     for the (Huggie) version of this charm. Since this is a necklace piece how did this get past your safeguards? This should be an obvious one."
     WHY IT GOT PAST: resolveSku takes a transaction SKU that is a master SKU as it stands (source "transaction"), and nothing else looked at WHAT that master entry is. The only
     form check there was (skuFamilyConflict) reads the WORDS NECKLACE / EARRING / STUD / HUGGIE / HOOP in the SKU text, and only on the paths that come from Etsy's inventory table; the
     master's plain "SCHRODINGER CAT" has none of those words. It is the same drawing as "SCHRODINGER CAT 2 (HUGGIE)" (one design file, charmnest/master/SCHRODINGER_CAT.ai, 21.2 x 24.3 pt:
     the library keeps one drawing under several SKU names), while the necklace charms are the numbered "SCHRODINGER CAT 1" and "SCHRODINGER CAT 2" (29 to 31 pt). The line read as that huggie drawing,
     with no question, and its vector picture showed the huggie skull cat.
     THE GUARD: every master entry and every line gets a FORM, "huggie" | "earring" | "necklace" | "unknown", from signals that already exist:
       a master entry: its SKU text ("(HUGGIE)", "HUGGIE HOOPS- X", "HOOPS- X", an EARRING or STUD word, a NECKLACE / CHOKER / PENDANT word; two families in one SKU say nothing); else the design FILE it
         shares with other SKUs (ctx.masterShared: the library keeps one drawing under several names): a plain-named SKU on a drawing that a (HUGGIE)-named SKU also holds is the huggie drawing when
         the huggie names are at least as many as the plain ones (SCHRODINGER CAT and "SCHRODINGER CAT 2 (HUGGIE)": one each), and the drawing's file is named for a huggie (".._HUGGIE_.ai",
         "HUGGIE_HOOPS-_X.ai") counts as one more huggie name; a drawing that six plain SKUs hold and one (HUGGIE) SKU is not a huggie drawing; else a plain X whose "X (HUGGIE)" (or "HUGGIE HOOPS- X")
         is a DIFFERENT drawing is the necklace-size charm of that design (1,330 of the master's SKUs: the shop draws the huggie version apart, "soft": it only keeps a huggie line off it);
       a line: a Huggie CHARM SET option; an option that says the form (Type / Style / Necklace or Earrings, a person's saved form answer first); a necklace length option; the earring words
         (lineSignals: earring, stud, huggies, hoop words, the shop's own earring SKUs) and a person's answer to "is this a pair of earrings?"; the title's family when it says necklace; the -CO charm-only mark.
     THE RULE: an automatic match (the transaction SKU as it stands, its master spelling, -CO, a known typo or name, Etsy's inventory SKU, a letter-family member, two designs named in words) never
     crosses these forms: a NECKLACE line never takes a huggie design, a HUGGIE line never takes a necklace design, an EARRING (stud) line never takes a huggie design. An unknown form on either
     side never clashes, and the SKU-word checks that already existed (a necklace SKU on an earrings listing and the reverse) are left exactly as they were. A clash is not cut: the line waits for a
     person with ONE plain line, and the numbered siblings of the same name that fit the line are offered (spec.siblings).
     A person's saved answer for the listing and SKU (alias, option map, saved pair) is never touched: it wins, also over an exact master SKU (resolveSku trusts the exact SKU; the answer is read here). */
  const FORMS = Object.freeze(["huggie", "earring", "necklace", "unknown"]);
  const FORM_TEXT = { huggie: [/\(\s*HUGGIES?\s*\)/, /(?:^|[^A-Z0-9_])HUGGIES?(?![A-Z0-9_])/, /HUGGIE[\s_-]*HOOPS?(?![A-Z])/, /^\s*HOOPS?\s*-/], earring: [/(?:^|[^A-Z])(?:EARRINGS?|STUDS?)(?=[^A-Z]|$)/], necklace: [/(?:^|[^A-Z])(?:NECKLACES?|CHOKERS?|CHOCKERS?|PENDANTS?)(?=[^A-Z]|$)/] };
  /** The form a SKU's TEXT says: "huggie" | "earring" | "necklace", or "" when it says none or two families ("HUGGIE_3722", a numbered design named Huggie, says nothing). */
  function formOfText(sku) {
    const s = upSku(sku), h = FORM_TEXT.huggie.some(r => r.test(s)), e = FORM_TEXT.earring.some(r => r.test(s)), n = FORM_TEXT.necklace.some(r => r.test(s));
    if (n && (h || e)) return "";
    return h ? "huggie" : e ? "earring" : n ? "necklace" : "";
  }
  const HUGGIE_FILE = /^HUGGIE_?HOOPS?|[A-Z0-9]_HUGGIE(?:_|\.)/i;   // (a design file named for a huggie: "HUGGIE_HOOPS-_UNICORN.ai", "CROWN_2_HUGGIE_.ai"; never "HUGGIE_3722.ai")
  const fileOf = e => { const p = e && e.aiPath ? String(e.aiPath) : ""; return p.slice(p.lastIndexOf("/") + 1); };
  /** The form of a master design, from its SKU and what the library says of its drawing. ctx: { masterEntry, masterShared? (sku → the master SKUs that share its design file; none: SKU text only) }.
   *  → { form: "huggie" | "earring" | "necklace" | "unknown", by: "text" | "shared" | "twin" | "none", soft? (a necklace read only from a distinct huggie twin) } */
  function masterForm(sku, ctx) {
    ctx = ctx || {}; const s = upSku(sku); if (!s) return { form: "unknown", by: "none" };
    const t = formOfText(s); if (t) return { form: t, by: "text" };
    const me = ctx.masterEntry, e = me ? me(s) : null; if (!e) return { form: "unknown", by: "none" };
    let H = HUGGIE_FILE.test(fileOf(e)) ? 1 : 0, P = 1, X = 0; const plain = [s];
    if (typeof ctx.masterShared === "function") for (const k of ctx.masterShared(s) || []) { const u = upSku(k); if (!u || u === s) continue; const f = formOfText(u); if (f === "huggie") H++; else if (f) X++; else { P++; plain.push(u); } }
    if (H >= 1 && !X && H >= P) return { form: "huggie", by: "shared" };
    // (a design is one form whatever name it is asked under: "MUSIC 4" and "MUSIC4" are one drawing, so any of its plain names with a distinct huggie twin makes it the necklace-size charm)
    const mine = designFile(e);
    for (const n of plain) for (const twin of [n + " (HUGGIE)", "HUGGIE HOOPS- " + n]) { const te = me(twin); if (te && mine && designFile(te) && designFile(te) !== mine) return { form: "necklace", by: "twin", soft: true }; }
    return { form: "unknown", by: "none" };
  }
  const HUGG_WORD = /\bhugg(?:ie|y|ies)\b/i;
  /** The form a LINE is, by the words and options it already carries. o: { optionMaps } (a person's saved form answers are read first). → { form, by } (by: the plain words for what said it). */
  function lineForm(line, o) {
    line = line || {}; o = o || {};
    if (huggieSet(line)) return { form: "huggie", by: "its Huggie CHARM SET option" };
    const vars = (line.variations || []).map(v => ({ name: lvName(v), value: lvValue(v) })).filter(v => v.name && v.value && !isPersonalisation(v.name)), opts = vars.filter(v => !isMetalOption(v.name));
    const hug = HUGG_WORD.test([String(line.title || "")].concat(opts.map(v => v.value)).join(" ")) || FORM_TEXT.huggie.some(r => r.test(upSku(line.sku)));
    const f = formOfLine(line, o.optionMaps);
    if (f === "necklace") return { form: "necklace", by: "its option says necklace" };
    if (f === "huggie") return { form: "huggie", by: "its option says huggie" };
    if (f === "earrings" || f === "earring-single") return { form: hug ? "huggie" : "earring", by: "its option says earrings" };
    if (opts.some(v => isNecklaceLength(v.name, v.value))) return { form: "necklace", by: "it has a necklace length option" };
    if (f === "charm" && opts.some(v => /^\s*charm\s*[+&]\s*engraving\s*$/i.test(v.value))) return { form: "necklace", by: "its option is the necklace charm" };
    const sig = lineSignals(line); let ear = sig.soldAs ? (hug ? "huggie" : "earring") : "";
    if (!ear && sig.ask && o.optionMaps) { const ans = optionLookup(o.optionMaps, line.listingId, EARS_OPT, sig.ask.value); if (ans && ans.field === "form" && (ans.value === "earrings" || ans.value === "huggie")) ear = ans.value === "huggie" ? "huggie" : "earring"; }
    if (ear) return { form: ear, by: "its earring words" };
    if (lineFamily(line) === "necklace") return { form: "necklace", by: "its title says necklace" };
    if (variationBase(upSku(line.sku))) return { form: "necklace", by: "its SKU is the charm-only mark" };
    return { form: "unknown", by: "none" };
  }
  /** Does a line's form clash with a design's form? Exactly the three crossings GUARD-API.md names: a NECKLACE line on a huggie design, a HUGGIE line on a necklace design, an EARRING (stud) line on a huggie design.
   *  Everything else (`unknown` on either side, a huggie line on an earring design, a necklace SKU on an earrings listing and the reverse) is NOT this guard's: the SKU-word family check
   *  (skuFamilyConflict, on Etsy's inventory SKUs) keeps deciding those exactly as it did. */
  function formClash(lf, df) {
    if (lf && typeof lf === "object") lf = lf.form;
    if (df && typeof df === "object") df = df.form;
    return (lf === "necklace" && df === "huggie") || (lf === "huggie" && df === "necklace") || (lf === "earring" && df === "huggie");
  }
  // (what a person may be OFFERED for a line: any design that is not of another known form; a necklace read only from a distinct huggie twin (soft) never stops an earring line)
  const formFits = (want, f) => !want || want === "unknown" || !f || f.form === "unknown" || f.form === want || (want === "huggie" && f.form === "earring") || (want === "earring" && f.form === "necklace" && !!f.soft);
  /** The numbered siblings of a SKU the master holds ("SCHRODINGER CAT" → "SCHRODINGER CAT 1", "SCHRODINGER CAT 2"), the ones whose form fits a line of form o.form, numbers only (never the -CO or (HUGGIE) twin of the
   *  plain name unless the line is a huggie: then "X 1 (HUGGIE)" and the huggie twins of X itself). → [{ sku, form, why, n, widthPt?, heightPt? }] by number, at most 12. ctx: { masterEntry, masterLoose?, masterShared? }. */
  function siblingsOf(etsySku, ctx, o) {
    ctx = ctx || {}; o = o || {}; const me = ctx.masterEntry, own = upSku(etsySku); if (!me || !own) return [];
    const base = own.replace(/\s*\(\s*HUGGIES?\s*\)\s*$/, "").trim(), want = o.form || "unknown", out = [], seen = new Set([own]);
    const add = (cand, n, why) => {
      const k = masterSku(cand, me, ctx.masterLoose); if (!k || seen.has(k)) return;
      const mf = masterForm(k, ctx); if (!formFits(want, mf)) return;
      seen.add(k); const e = me(k) || {}; out.push(Object.assign({ sku: k, form: mf.form, why, n }, Number.isFinite(e.widthPt) ? { widthPt: e.widthPt } : {}, Number.isFinite(e.heightPt) ? { heightPt: e.heightPt } : {}));
    };
    if (want === "huggie") { add(base + " (HUGGIE)", 0, `the huggie version of ${base}`); add("HUGGIE HOOPS- " + base, 0, `the huggie version of ${base}`); }
    // the same name written for the line's family ("GREEKGODDESS_NECKLACE_4" on an earrings line: "GREEKGODDESS_EARRING_4"), when the master holds it
    const fam = want === "necklace" ? "necklace" : want === "earring" || want === "huggie" ? "earring" : "";
    if (fam) for (const t of familyTwins(own, fam, me, ctx.masterLoose)) add(t, 0, `the ${fam} version of ${own} (same number)`);
    for (let n = 0; n <= 12; n++) for (const sep of [" ", "-", "_", ""]) add(base + sep + n + (want === "huggie" ? " (HUGGIE)" : ""), n, `numbered version ${n} of ${base}`);
    return out.sort((a, b) => a.n - b.n || a.sku.localeCompare(b.sku)).slice(0, 12);
  }
  /* ═══ 3b · LOOKALIKE NAMES (LOOKALIKE, 12 Oct 2026) ══════════════════════════════════════════════════════════════════════
     Four times on 12 Oct an exact SKU match put the wrong drawing on an order and Paul found it by eye. The one that this rule is for: the master holds TWO drawings whose names read alike once spaces, dashes,
     case and punctuation are dropped (CELESTIAL3-COMET and "CELESTIAL 3 - COMET": 138 such name groups), one a stud-size design with NO hanging hole (holes 0) and one the necklace charm with its ring. The Etsy SKU
     "Celestial3-Comet" equals the loop-less entry exactly, so the exact name won, silently, on a NECKLACE listing (the Running Shoe, Playing Cards, Mouse, Megaphone and Sheep necklaces of the sandbox pool were
     the same). A necklace hangs from a chain: a drawing with no hole cannot.
     THE RULE (measured on every stored / fixture line first: plans/lookalike-1012/LOOKALIKE-findings.md): a line whose FORM is "necklace" (the form guard above: necklace words, a chain length option, the
     listing's title family) that matched a master drawing with 0 holes by the exact name (or its -CO base, or Etsy's inventory SKU), while another master entry of the same name once spacing and punctuation are
     set aside is a DIFFERENT drawing (another design file and charm) with at least one hole, a box at least 1.2 times as big (NECKLACE_SIZE: the ring is part of it) and is not blocked, is NOT pooled. It waits with ONE plain line naming the drawings and, in the Review row, one tap for
     each; the tap is the person's saved link for this listing and this SKU (the same "Use this charm" answer, aliasPut bySku), which beats every automatic match, so the rule never asks that listing and SKU again.
     It is silent, and reads exactly as before, when: a person's answer exists for the listing and SKU (a link, a saved pair, a listing-wide answer, an option's pick); the two names are one drawing file;
     either entry is blocked; either entry has no hole count or size; the ringed entry is not clearly the bigger one (Playing Cards and Megaphone: see below); the line is not a necklace; the matched drawing already has a hole (a necklace line on the ringed entry is right). Two names that differ in no way the
     form can judge (both loop-less, both ringed) are NOT stopped: the master keeps many such pairs on purpose (a necklace listing and an add-on listing writing the SKU two ways for two sizes), so an
     exact name that is not demonstrably wrong still wins. When the form AGREES with the exact name and a lookalike of the other kind exists (a necklace line on the ringed entry, a pair of earrings on the loop-less
     one), the reading says so in one plain line (spec.lookalike.state "agrees") and nothing waits. Needs ctx.masterLike (sku → the master SKUs that read alike); without it nothing changes. */
  const LOOK_AUTO = new Set(["transaction", "variation", "inventory"]);   // (an exact master name picked by the SKU as written; a person's answer, a spelling, a typo or a named design is never in this list)
  const holesOf = e => (e && !(e.sizes && Object.keys(e.sizes).length) && e.holes != null && e.holes !== "" && Number.isFinite(+e.holes) ? Math.max(0, Math.floor(+e.holes)) : null);
  const mmWords = e => (e && Number.isFinite(+e.widthPt) && Number.isFinite(+e.heightPt) ? `${(+e.widthPt * 0.352778).toFixed(1)} × ${(+e.heightPt * 0.352778).toFixed(1)} mm` : "");
  const boxOf = e => (e && Number.isFinite(+e.widthPt) && Number.isFinite(+e.heightPt) ? +e.widthPt * +e.heightPt : 0);
  const looseDrawing = (x, y) => { const a = designFile(x), b = designFile(y); return !!a && a === b; };
  // The shop's pairs are a STUD-size drawing (no hanging hole) and the NECKLACE-size drawing of the same design (a ring, so a bigger box): in the master 150 of its 156 such pairs have the ringed drawing at least 1.2 times
  // the box of the loop-less one. A pair where the ringed one is NOT clearly the bigger (4 pairs are smaller, 2 within 1.2: the master draws or names them the other way round, a hole count the picture does not
  // bear out) is no stud / necklace pair: nothing here can say which is right, so the exact name stands as it always did and the findings list them for Paul.
  const NECKLACE_SIZE = 1.2;
  /** The lookalike reading of a design a line matched by its exact name. x: the matched master SKU; lf: the line's form ({ form } from lineForm); ctx: { masterEntry, masterLike (sku → the master SKUs that read the same
   *  once spacing and punctuation are set aside), guardCtx? }. → null (nothing to say) | { state: "held" | "agrees", design, others: [{ sku, holes, mm, ring }], candidates (held: the matched design first), reason | note } */
  function lookalikeRead(x, lf, ctx) {
    ctx = ctx || {}; const me = ctx.masterEntry, like = ctx.masterLike, form = lf && typeof lf === "object" ? lf.form : lf;
    if (typeof me !== "function" || typeof like !== "function" || !x) return null;
    const xe = me(x); if (!xe || xe.blocked) return null;
    const xh = holesOf(xe), xb = boxOf(xe); if (xh == null || !xb) return null;
    const row = (k, e) => ({ sku: k, holes: holesOf(e), mm: mmWords(e), ring: holesOf(e) > 0 });
    const big = [], small = [];   // big: ringed drawings clearly bigger than a loop-less x; small: loop-less drawings clearly smaller than a ringed x
    for (const k0 of like(x) || []) {
      const k = upSku(k0), e = k && k !== x ? me(k) : null; if (!e || e.blocked) continue;
      const h = holesOf(e), b = boxOf(e); if (h == null || !b) continue;
      if (looseDrawing(xe, e) || (xe.charmHash && e.charmHash && String(xe.charmHash) === String(e.charmHash))) continue;   // (one drawing under two names: nothing to choose)
      if (formClash(form, masterForm(k, ctx.guardCtx || { masterEntry: me }))) continue;   // (a huggie design is the form guard's, not this rule's)
      if (xh === 0 && h > 0 && b >= NECKLACE_SIZE * xb) big.push(row(k, e));
      else if (xh > 0 && h === 0 && xb >= NECKLACE_SIZE * b) small.push(row(k, e));
    }
    const mine = row(x, xe), name = o => `${o.sku} (${o.ring ? "with ring" : "no loop"}${o.mm ? ", " + o.mm : ""})`;
    if (form === "necklace" && big.length) {
      const cands = [mine].concat(big).slice(0, 4);
      const list = cands.map(name), words = list.length === 2 ? list.join(" and ") : list.slice(0, -1).join(", ") + " and " + list[list.length - 1];
      return { state: "held", design: x, others: big, candidates: cands, reason: `This SKU matches ${cands.length === 2 ? "two" : cands.length} different drawings: ${words}, and this listing is a necklace, which a drawing with no loop cannot hang on. Pick the right one.` };
    }
    if (form === "necklace" && small.length) return { state: "agrees", design: x, others: small, note: `“${x}” has a look-alike, ${name(small[0])}, which is a different drawing; this listing is a necklace, so it keeps ${x} (with ring)` };
    if (form === "earring" && big.length) return { state: "agrees", design: x, others: big, note: `“${x}” has a look-alike, ${name(big[0])}, which is a different drawing; this listing is a pair of earrings, so it keeps ${x} (no loop)` };
    return null;
  }
  const ZODIAC_AUTO = new Set(["transaction", "spelling", "variation", "typo", "named", "inventory"]);   // (ZODSIGN: the automatic sources a shared zodiac SKU may be read again from)
  const FORM_AUTO = new Set(["transaction", "spelling", "variation", "typo", "named", "inventory", "letter"]);   // (a person's answer, "alias" / "option" / a saved pair, is never in this list)
  const FORM_THIS = { huggie: "a huggie design", earring: "an earring design", necklace: "a necklace design" }, FORM_LINE = { huggie: "huggie hoops", earring: "a pair of earrings", necklace: "a necklace" };
  /** The ONE plain line for a held line: what the design is and what the listing is. */
  const formReason = (etsy, design, df, lf) => (etsy === design ? `SKU “${etsy}” is ${FORM_THIS[df.form]} in the library` : `SKU “${etsy}” reads as “${design}”, which is ${FORM_THIS[df.form]} in the library`) + `, but this listing is ${FORM_LINE[lf.form]}`;   // (starts "SKU ": QUESTION_REASON reads a stored hold that begins so as a question)
  function resolveSku(line, aliases, masterEntry, noDesign, masterLoose) {
    const raw = upSku(line.sku);
    const a = aliases && aliases[String(line.listingId)], up = s => s ? String(s).trim().toUpperCase() : "";
    // (the listing's answer is its necklace charm's: never a huggie set's, which has its own for a line with no SKU)
    const own = raw && a && a.bySku ? up(bySkuOf(a.bySku, raw)) : "", whole = !a ? "" : huggieSet(line) ? (!raw ? up(a.huggie) : "") : a.sku && (!raw || !a.v) && !/\(HUGGIE\)$/.test(raw) ? up(a.sku) : "";
    // a blocked SKU's card offers "Use this charm" too: the answer for that SKU is its design (the blocked entry beat it)
    const held = raw && masterEntry ? masterEntry(raw) : null;
    // DACHMAP (12 Oct 2026): a person's saved link for THIS listing and THIS transaction SKU (bySku, "Use this charm") beats an exact master match of that SKU when the saved design is in the master and
    // is not blocked. The Etsy SKU "DACHSHUND" is also a master entry (a cat disc filed under that name); Paul linked the listing to DACHSHUND_36943 and the exact match kept winning. Never the reverse:
    // no saved link (or one naming a design the master does not hold, or the same SKU) reads exactly as before. Only the SKU's own answer (own), never the listing-wide one (whole).
    const ownEntry = held && own && own !== raw && masterEntry ? masterEntry(own) : null, ownWins = !!(ownEntry && !ownEntry.blocked);
    // CHAMMAP (12 Oct 2026): a master entry filed under the shop's SHARED charm-only placeholder ("CO-GOLDCHARMO", a label on the astronaut; see genericSku) is no automatic match for any listing: the listing's
    // own saved answer for the SKU stands (ownWins above, or its listing-wide one), else the line waits (source "generic", held in interpretLine). A SKU on the no-design list is still that.
    if (held && !held.blocked && !ownWins && !isNoDesign(raw, noDesign) && genericSku(raw)) return own ? { sku: own, source: "alias" } : whole ? { sku: whole, source: "alias" } : { sku: raw, source: "generic" };
    if (raw && (!masterEntry || (held && !(own && (held.blocked || ownWins))) || isNoDesign(raw, noDesign))) return { sku: raw, source: "transaction" };
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
    // a SKU that is exactly one Paul named as a master design (KNOWN_SKU_NAMES): the same place and the same conditions as a known typo
    const named = raw && namedSku(raw, masterEntry, masterLoose);
    if (named) return { sku: named, source: "named" };
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

  /* ═══ 3a · Mini Tag "Charm Type: Tag3 (front + back)" (TAGSKU, 11 Oct 2026) ══════════════════════════════════════════════
     Paul, on listing 4458930445 (Mini Tag Initial Charm), order 4195966039 "Charm Type: Tag1 (front+back)" held "not mapped", and the older 4171852053 "Tag1 (front
     engrave)": "the tag designs and font options of this listing should be mapped to SKUs in our repository". The listing's Charm Type drop-down has eight values: the tag
     SHAPE (the listing's picture shows four, numbered 1 to 4) and, in brackets, the ENGRAVING: "front engrave" (the front face only) or "front+back" / "front + back" (both faces).
     Etsy cannot say which design each value is: its inventory table for the listing holds ONE SKU (Mini_0445: "Mini" and the last four digits of the listing id) on all 32
     products. So the shape comes from the tag number and the master design of that shape, and the engraving mode from the words in the brackets (told to the line: spec.tagOption).
     The four shapes are the four designs of the master's OBJECTS file named by their size in millimetres: the outline of every master thumbnail was compared with the four tags
     of the listing's own picture (outline width at 60 heights, root-mean-square difference; the 6,856 master SKUs were first cut to the portrait mini tags):
       Tag 1  plain rounded rectangle        5X8.5    (0.020; the next closest, the chamfered tags, 0.039)
       Tag 2  pointed top, rounded tip       5X8.8    (0.011; TINY INITIAL TAG 2 and 3 are the same outline to 0.002 and 1.5 % smaller: the picture cannot tell them apart, 5X8.8 is of the same set)
       Tag 3  pointed top, flat tip          4.6X8.8  (0.010; every other design 0.073 and more)
       Tag 4  chamfered rectangle            5X9      (0.019; MINI_0445, the listing's one shared SKU, is the same outline to 0.004, 1.5 % smaller, and has no engraving area)
     Confidence: Tag 1 and Tag 3 high (the only plain portrait design, the only flat-tipped one); Tag 2 and Tag 4 medium-high (each has a look-alike of the same outline and 1.5 % other size: TINY INITIAL
     TAG 2 / 3, MINI_0445). The master's own "TINY INITIAL TAG n" are no map: TAG 4 is a byte copy of TAG 1 (charm hash b5023f11), TAG 1 lies on its side (45 x 16.7 pt), TAG 3 is the 5X8.8 drawing. Paul noted
     MINI_0445 "chosen correctly" on a Tag1 order; MINI_0445 is the octagon of the picture's Tag 4: if the shop's Tag 1 is in fact that octagon, TAG_SHAPES is the one place to change. A tag whose design is only a
     COPY of a lower tag's drawing (the same charm hash) is no shape of its own and waits like a missing one: two shapes never end on one design.
     The tag number is the shape, whatever SKU the transaction carries: MINI_0445 is the listing's shared SKU, not "Tag 1". Who decides first (never overridden): a person's saved
     answer for the value (optionMapPut), then the option's own SKU (Etsy's table ties one to the value, or an indexed master family: FLAGWHY), then this rule. A shape the master
     has no design for is never replaced by a look-alike: the value waits with ONE plain line that names the shape. */
  const TAG_SHAPES = {
    1: { sku: "5X8.5", shape: "plain rounded rectangle" },
    2: { sku: "5X8.8", shape: "pointed top with a rounded tip" },
    3: { sku: "4.6X8.8", shape: "pointed top with a flat tip" },
    4: { sku: "5X9", shape: "chamfered rectangle" }
  };
  const TAG_BOTH = /\bfront\s*(?:\+|&|and|\/|plus|,)\s*back\b|\b(?:both|two|2)[\s-]*sides?\b|\bdouble[\s-]*sided\b/i;
  const TAG_FRONT = /^front(?:\s*(?:only|engrav\w*))?$|^engrav\w*\s*(?:on\s*)?front(?:\s*only)?$/i, TAG_BACK = /^back(?:\s*(?:only|engrav\w*))?$|^engrav\w*\s*(?:on\s*)?back(?:\s*only)?$/i;
  /** The tag and the engraving of a drop-down value like "Tag3 (front + back)": { tag: 3, mode: "front" | "both" | "back" | null, words } (mode null: no engraving words in it), or null when the value is no tag choice. */
  function tagOptionOf(value) {
    const s = String(value == null ? "" : value).replace(/&quot;/g, "\"").replace(/&amp;/g, "&").replace(/\s+/g, " ").trim();
    const m = /^tag\s*#?\s*(\d{1,2})(?![\d.])\s*(.*)$/i.exec(s); if (!m) return null;
    const words = m[2].replace(/^[\s(\[:,\-–—]+/, "").replace(/[\s)\]]+$/, "").toLowerCase().trim();
    const mode = !words ? null : TAG_BOTH.test(words) ? "both" : TAG_FRONT.test(words) ? "front" : TAG_BACK.test(words) ? "back" : null;
    return { tag: +m[1], mode, words };
  }
  /** What a line's options say about a Mini Tag choice: { tag, mode, name, value, variation, shape, sku, state, why } or null. state: "answered" (a person's saved answer stands), "found" (the shape's design is in the
   *  master), "missing" (it is not: why says which shape), "unchecked" (no master to ask). Only a value with engraving words in its brackets is a tag choice ("Tag 1: J" in a note is not). */
  function tagOptionRead(line, maps, me) {
    for (const v of (line && line.variations) || []) {
      const name = lvName(v), value = lvValue(v).trim(); if (!name || !value || isMetalOption(name) || isPersonalisation(name)) continue;
      const t = tagOptionOf(value); if (!t || !t.mode) continue;
      const sh = TAG_SHAPES[t.tag], out = { tag: t.tag, mode: t.mode, name, value, variation: v, shape: sh ? sh.shape : "", sku: sh ? sh.sku : "", state: "", why: "" };
      const said = optionLookup(maps, line.listingId, name, value);
      if (said) { out.state = "answered"; if (/^(?:front|both|back)$/.test(String(said.engrave || ""))) out.mode = said.engrave; }   // (a saved answer may say the engraving too: engrave "front" | "both")
      else if (!sh) { out.state = "missing"; out.why = `Tag${t.tag} is not one of the listing's four tag shapes (1 to 4), so no master design is known for it`; }
      else if (!me) out.state = "unchecked";
      else if (!me(sh.sku)) { out.state = "missing"; out.why = `the master has no design for the Tag${t.tag} shape (${sh.shape}, ${sh.sku}): add it to the master and this line reads on its own`; }
      else {
        // never two shapes on one drawing: a tag whose master design is the very same drawing (the same charm hash) as a LOWER tag's is a copy, not its own shape (the master's TINY INITIAL TAG 4 is a copy of TAG 1)
        const mine = me(sh.sku), h = String(mine && mine.charmHash || ""), twin = h ? [1, 2, 3, 4].find(u => u < t.tag && TAG_SHAPES[u] && String((me(TAG_SHAPES[u].sku) || {}).charmHash || "") === h) : 0;
        if (twin) { out.state = "missing"; out.why = `the master has no design of its own for the Tag${t.tag} shape (${sh.shape}): ${sh.sku} is the same drawing as Tag${twin}'s ${TAG_SHAPES[twin].sku}`; }
        else out.state = "found";
      }
      return out;
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
      // (the person's own words for the listing, when they left some ("pre-made piece, not laser cut"), stand in the reason: the one line that says why)
      if (KIND_WORDS_[lk.kind]) { const note = String(lk.note || "").replace(/\s+/g, " ").trim().slice(0, 90); return { kind: lk.kind, why: `listing ${lid}: ${lk.by || "a person"} said every order of it is ${KIND_WORDS_[lk.kind]}${note ? ` (${note})` : ""}` }; }
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
    const raw = upSku(line.sku), sku = upSku(opts.sku || raw);
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

  specialOf.addOn = { of: addOnOf, listings: ADDON_LISTINGS, titles: ADDON_TITLE, wait: "custom listing: no master design, waits for the order's own designs (Send to Sheet) or Complete Order" };   // (for the tests and for a reader: what says a listing is an add-on listing)
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
  /* ONE charm of a two-body design on a line that is not an earring pair (Paul, 11 Oct 2026: "This listing is not an earring so it should NOT have duplicate charms"). The design's
     two bodies being the SAME outline (two keys, two mittens) is a duplicate: the line makes one body, the left one (CharmNestPair.soloOf says which, and why). Two clearly different charms of
     one design (BEE + HIVE, WOLF + MOON) keep today's one piece holding both. The intake has no geometry, so it asks the shared module about the master entry alone (alikeDesign: the four SKUs
     measured, or an entry that says `pair.alike`); the bridge asks again with the traced design when it makes the piece and calls solo() below when the geometry says one body. */
  const pairModule = () => { try { const g = typeof self !== "undefined" ? self : typeof globalThis !== "undefined" ? globalThis : null; if (g && g.CharmNestPair) return g.CharmNestPair; if (typeof require === "function") return require("./charm-nest-pair.js"); } catch (_) { /* the page without the module: the old words */ } return null; };
  const alikeKnown = o => { try { const M = pairModule(); return !!(M && typeof M.alikeDesign === "function" && M.alikeDesign(Object.assign({ sku: o && o.sku }, (o && o.entry) || {})) === true); } catch (_) { return false; } };
  const ONE_WORDS = (index, count, why) => `the design draws ${count || 2} bodies (${why || "a left and a right charm"}) but the line is not an earring pair: made as ONE charm, the ${index === 1 ? "right" : "left"} body only, no Left or Right, nothing duplicated or mirrored`;
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
   *  soldBy ("option" | "title" | "words" | null), side ("L" | "R" | null: a single earring that names its ear), discs, necklace (the buyer chose a necklace length) }.
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
    /* NECKLACE LENGTH BOUGHT (KITEPAIR, 13 Oct 2026, order 4199929230: "Kite Necklace ... Kite Shape Earrings ...", SKU KITE, "Necklace Length: 20"" was read as a pair of earrings, Left and Right, the Right mirrored): the
       buyer chose a necklace chain, so the line is a necklace, and an ear word in the LISTING's TITLE (the title says what the listing offers, the option what this buyer bought) never makes it earrings
       or a single earring. Everything else is read exactly as before: the words in the buyer's own options (a value, an option name), the shop's own earring SKU, and what a person is asked (nothing new is
       asked: a line the title's ear words used to settle still asks nothing). The same fact lineForm reads (isNecklaceLength). */
    const neckBought = vars.some(v => !isPersonalisation(v.name) && !isMetalOption(v.name) && String(v.value).trim() && isNecklaceLength(v.name, v.value));
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
    const lineSku = String(line.sku || line.__sku || ""), skuEar = !charmOnly && SKU_EAR.test(lineSku), earWords = EAR.test(text), earText = neckBought ? EAR.test(opts.join(" ")) : earWords, skuUnderOther = skuEar && otherInTitle && !earWords && !nameEar;
    const earOutside = nameEar || (skuEar && !skuUnderOther), earAll = earText || earOutside;
    const SINGLE_TXT = /\bsingle\s+(?:stud\s+|huggie\s+|hoop\s+)?(?:earring|stud|huggie|charm)\b|\b(?:1|one)\s+(?:single\s+)?(?:earring|stud|huggie)\b(?!s)/i, SINGLE_END = /\bsingle(?:\s+(?:earring|stud|huggie|hoop|charm))?\s*$/i;
    // a TITLE that says Single: next to the earring word, or a few words before it ("Custom Single Replacement Silver Cat Huggie Earring Left Ear"), or at its end
    // ("Huggie Earring, Single", "Poodle Earring Single"). "Single Pearl Stud Earrings" is a pair of earrings with one pearl each, not a single earring: the plural word after it settles that.
    const SING_WORD = "(?:single|individual|solo)", SING_EAR = /\b(?:earring|stud|huggie|hoop)\b(?!s)(?!\s+(?:earrings|studs|huggies|hoops)\b)/i, PLURAL_OR_PAIR = /\b(?:earrings|studs|huggies|hoops|pairs?|sets?|both)\b/i;
    const TITLE_SINGLE = t => (!neckBought && SINGLE_TXT.test(t)) || (((!neckBought && EAR.test(t)) || earOutside) && (new RegExp("\\b" + SING_WORD + "\\b(?:\\s+[\\w'’&.-]+){0,6}?\\s+(?:earring|stud|huggie|hoop)\\b(?!s|\\s+(?:earrings|studs|huggies|hoops)\\b)", "i").test(t) || new RegExp("(?:^|[,;|(\\-–—]\\s*)" + SING_WORD + "\\s*\\)?\\s*$", "i").test(t)
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
    if (!soldAs && !charmOnly && !earWords) {   // (a title's ear words that a bought necklace length set aside asked nothing before, and ask nothing now)
      const t0 = notEar(title), other = (OTHER_PRODUCT.exec(title) || [])[0] || "", key = norm(title).slice(0, 190) || norm(opts.join(" ")).slice(0, 190);
      const pairPhrase = (PAIR_ASK_TITLE.exec(t0) || [])[0] || "";   // (a TITLE only: an option that says "Pair" is an unmapped option, already asked about)
      if (key && skuUnderOther) ask = { by: "sku", value: key, why: `the SKU “${clip(lineSku, 30)}” is an earring SKU, but the title names a ${other.toLowerCase()}: is this a pair of earrings (a Left and a Right)?` };
      else if (key && HUGG.test(t0) && otherInTitle && !earAll) ask = { by: "title", value: key, why: `the title says “huggie” and also “${other.toLowerCase()}”: is this a pair of earrings (a Left and a Right)?` };
      else if (key && pairPhrase && !earAll && !otherInTitle) ask = { by: "pair", value: key, why: `“${clip(pairPhrase, 40)}” says a pair but names no earring: is this a pair of earrings (a Left and a Right)?` };
    }
    return { says: signals.length > 0, signals: [...new Set(signals)], soldAs, soldBy, side, sideBy, discs: discsIn(line), ask, other: OTHER_PRODUCT.test(title), necklace: neckBought };
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
    // no option names a sign, but a drop-down value says "2 symbols" (the Zodiac REVAMP listing's 13th Zodiac Sign value is "2 symbols-leave note": the buyer writes BOTH signs in the note):
    // the signs are the note's, the first named the Left; that value is the option the signs are looked up under
    let byNote = null;
    if (!list.length) {
      const v = ((line && line.variations) || []).find(x => { const nm = lvName(x), val = lvValue(x).trim(); return !!nm && !!val && !isPersonalisation(nm) && !isMetalOption(nm) && TWO_DESIGNS.test(val); });
      if (!v) return null;
      byNote = v; optionName = lvName(v);
    }
    const notes = [].concat(((line && line.variations) || []).filter(v => isPersonalisation(lvName(v))).map(lvValue), (line && line.personalization) || [], (line && (line.buyerMessage || line.message_from_buyer)) || []);
    const said = [];   // (every sign any note names, each note's own words: a sign the drop-down already has is the same sign again, not a second one)
    for (const t of notes) for (const sg of signsOfText(t)) { said.push(sg); add(sg, { from: "note", name: optionName, value: sg, text: visible(t).trim() }); }
    const own = byNote || (list.find(x => x.from === "option" && x.variation) || {}).variation, pid = own ? idOf(own.propertyId != null ? own.propertyId : own.property_id) : "";
    const text = [...new Set(notes.map(t => visible(t).trim()).filter(Boolean))].join(" · ");
    return Object.assign({ signs: list, optionName, propertyId: pid, said, noteText: text }, byNote ? { byNote: true, optionValue: lvValue(byNote).trim() } : {});
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

  /* ═══ 3c · a SHARED zodiac SKU and the sign the buyer named (ZODSIGN, Paul, 12 Oct 2026, order 4200256896) ═════════════════════════════════════
     Paul, on the zodiac huggie hoops (Etsy SKU HUGGIE HOOPS-ZODIAC, drop-downs METAL CHOICE and HOOP SIZE, the buyer's sign typed in the personalisation box): "the buyer chose the Capricorn zodiac
     option … however you chose the Libra vector file".
     THE CAUSE, read with the real code: the SKU HUGGIE HOOPS-ZODIAC is not one sign's: the listing sells all twelve under it. But the master carries that SKU on ONE drawing, the Libra huggie
     (HUGGIE HOOPS- ZODIAC, HUGGIE HOOPS- ZODIAC AVATAR and ZODIAC ICONS LIBRA (HUGGIE) are one design file). resolveSku trusts a SKU the master knows (source "spelling": the master writes it with a
     space), and nothing read the personalisation box for a sign: the only sign readers were the "2 symbols" pair (signsOfLine) and an option that has a SKU of its own. So every sign of that listing was
     Libra, in silence, with no hold, and the word in the box fell to the engraving reader.
     THE RULE (read by interpretLine after the SKU and a person's answers; nothing here runs without ctx.masterSkus, the page's master list):
       · a SHARED zodiac SKU is a master SKU that says ZODIAC, names no sign, is no numbered family member (ZODIAC_EARRINGS-6 is its own sign) and sits on the drawing of a master SKU that names ONE sign
         and says ZODIAC (the master keeps it as a second name of that sign's drawing). Exactly this evidence: a SKU that merely holds the word (ZODIAC_39104, "ZODIAC") is left as it was;
       · the sign is what the BUYER named: an option value, the personalisation box, the buyer's note, the staff note (English names and the shop languages the 2-symbols reader knows, typo tolerance
         for the English names in the box and in long words: Capricon, Sagitarius), never the title. Two different signs, or none, wait ("Which zodiac sign?"): never guessed;
       · its charm is the master design that NAMES the sign and says ZODIAC, of the FORM of the line (section 3b reads the forms: huggie hoops take the huggie one, never the necklace-size twin); else, for
         a family whose drawing is a numbered one (ZODIAC DISC is ZODIAC_NECKLACE_DISC-7) the member at the sign's place, only when the master's own sign names agree with that numbering;
       · a sign with no such design in the repo (today Capricorn, Aquarius, Gemini, Cancer and Sagittarius, for huggies) waits with ONE plain line, "The Capricorn charm is not in the repo yet", and is
         never another sign's file; the held line is read again when the library grows, so it clears the day a master for the sign exists;
       · a person's saved answer wins (an alias, an option map for the value, a saved pair): this reading replaces only an automatic match, as the form guard does; and a line the form guard holds
         (a necklace on a huggie SKU) stays the guard's.
     The sign word is NOT engraving text: spec.zodiacSign says the buyer chose it (textOnly: the box held nothing else), and the order's reading says so in one line. */
  const SIGN_ORDER = Object.keys(SIGN_WORDS);   // Aries … Pisces: the shop's own numbering of its twelve-sign families (ZODIAC_EARRINGS-0 is Aries, -11 Pisces)
  const skuTokens = sku => upSku(sku).split(/[^A-Z]+/).filter(Boolean);
  /** The one sign a master SKU's text names, by its English name as a whole word ("ZODIAC ICONS LIBRA (HUGGIE)" → Libra; SCORPION and LEOPARD name none: whole words only), else "". */
  function skuSign(sku) { const t = skuTokens(sku), f = SIGN_ORDER.filter(s => t.includes(s.toUpperCase())); return f.length === 1 ? f[0] : ""; }
  const skuZodiac = sku => skuTokens(sku).some(w => w === "ZODIAC" || w === "ZODIACS");
  /** The sign a master SKU is the design of: it names one sign and says ZODIAC itself or through a name its drawing carries (ARIES, with ZODIAC ICONS ARIES on the same file). "" when it is not. */
  function zodiacSignOf(sku, ctx) {
    const sign = skuSign(sku); if (!sign) return "";
    if (skuZodiac(sku)) return sign;
    const share = ctx && typeof ctx.masterShared === "function" ? ctx.masterShared(upSku(sku)) || [] : [];
    return share.some(k => skuZodiac(k) && skuSign(k) === sign) ? sign : "";
  }
  /** { sku, named } when this SKU is a shared zodiac SKU (see the rule above), else null. */
  function sharedZodiac(sku, ctx) {
    const me = ctx && ctx.masterEntry, s = upSku(sku);
    if (!me || !s || !me(s) || typeof ctx.masterShared !== "function" || skuSign(s) || !skuZodiac(s) || skuIndexFamily(s, me)) return null;
    const named = (ctx.masterShared(s) || []).map(upSku).filter(k => k !== s && skuSign(k) && skuZodiac(k));
    return named.length ? { sku: s, named } : null;
  }
  /* Misspelt sign names. English names only (the shop-language words are read exactly, as the 2-symbols reader reads them), one edit (two for 10 letters and more), a word of 5 letters or more, and
     a word of 5 or 6 letters only in a short text (3 words at most: the box). Words that are one edit from a sign and are ordinary words are never a sign. */
  const SIGN_STOP = new Set(["pieces", "piece", "cancel", "cancels", "cancelled", "canceled", "dancer", "dancers", "lancer", "aquarium", "aquariums", "libre", "arise", "arises", "aires", "ares", "gemin", "virgin"]);
  const SIGN_FUZZ = SIGN_ORDER.map(s => s.toLowerCase()).filter(w => w.length >= 5);
  function fuzzySign(word, words) {
    const w = plainText(word).replace(/[^a-z]/g, ""); if (w.length < 5 || SIGN_STOP.has(w) || SIGN_OF.has(w) || (w.length < 7 && words > 3)) return "";
    const max = w.length >= 10 ? 2 : 1; let best = max + 1, hit = new Set();
    for (const t of SIGN_FUZZ) { const d = damerau(w, t, max); if (d < best) { best = d; hit = new Set([SIGN_OF.get(t)]); } else if (d === best && d <= max) hit.add(SIGN_OF.get(t)); }
    return best <= max && hit.size === 1 ? [...hit][0] : "";
  }
  /** The signs a text names, in order, each once: [{ sign, word, typo }] (typo: a misspelt English name, when fuzzy). */
  function signsInText(text, fuzzy) {
    const toks = plainText(visible(text)).split(/[^a-z]+/).filter(Boolean), out = [];
    for (const w of toks) { let s = SIGN_OF.get(w), typo = false; if (!s && fuzzy) { s = fuzzySign(w, toks.length); typo = !!s; } if (s && !out.some(x => x.sign === s)) out.push({ sign: s, word: w, typo }); }
    return out;
  }
  const SIGN_FILLER = new Set(["zodiac", "sign", "signs", "star", "symbol", "charm", "astrology", "horoscope", "my", "is", "the", "a", "an", "please", "thanks", "thank", "you", "one", "and", "for", "of", "i", "it", "me", "with"]);
  const SIGN_FROM = { option: "the option", box: "the personalisation box", note: "the buyer's note", staff: "the staff note" };
  /** What the buyer named, from the sources in the order they are trusted (the sign options, the personalisation box, the buyer's note, the staff note): { signs: [{ sign, from, word, typo, text, name?, variation? }], box: [texts], textOnly }. */
  function signsNamed(line, order) {
    line = line || {}; const found = [], add = (sign, rec) => { if (!found.some(x => x.sign === sign)) found.push(Object.assign({ sign }, rec)); }, boxVals = [];
    for (const v of line.variations || []) {
      const name = lvName(v), value = lvValue(v).trim(); if (!name || !value) continue;
      if (isPersonalisation(name)) { boxVals.push(value); continue; }
      if (isMetalVariation(name, value) || isSizeOption(name) || isChainOption(name) || isPriceOption(name)) continue;
      for (const r of signsInText(value, false)) add(r.sign, { from: "option", word: r.word, typo: false, text: value, name, variation: v });
    }
    const box = uniqStr([].concat(line.personalization || [], boxVals).map(t => visible(t).trim()).filter(Boolean));
    const groups = [["box", box], ["note", [line.buyerMessage || (order && order.buyerMessage) || ""]], ["staff", [line.staffNote || (order && order.staffNote) || ""]]];
    for (const [from, texts] of groups) for (const t of texts) { const tt = visible(t).trim(); if (tt) for (const r of signsInText(tt, true)) add(r.sign, { from, word: r.word, typo: r.typo, text: tt }); }
    // the box held nothing but sign words (and filler): the buyer's word is the sign, there is no text to engrave
    const textOnly = box.length > 0 && found.some(x => x.from === "box") && box.every(t => plainText(t).split(/[^a-z]+/).filter(Boolean).every(w => SIGN_OF.has(w) || SIGN_FILLER.has(w) || !!fuzzySign(w, 1)));
    return { signs: found, box, textOnly };
  }
  const signFormOk = (want, f) => !want || (want === "necklace" ? f === "necklace" || f === "unknown" : f === want);
  /** The master design that is sign S's own, of the form `want` ("" = any): { sku } | { ambiguous: [sku, sku] } (two drawings) | { other: [{ sku, form }] } (the sign has designs, none of this form) | {} (none). */
  function signDesign(sign, want, ctx) {
    const me = ctx.masterEntry, guard = { masterEntry: me, masterLoose: ctx.masterLoose, masterShared: ctx.masterShared }, seen = new Set(), by = new Map(), other = [];
    for (const raw of ctx.masterSkus() || []) {
      const k = upSku(raw); if (!k || seen.has(k) || skuSign(k) !== sign) continue; seen.add(k);
      if (!me(k) || !zodiacSignOf(k, ctx)) continue;
      const f = masterForm(k, guard).form;
      if (!signFormOk(want, f)) { other.push({ sku: k, form: f }); continue; }
      const file = designFile(me(k)) || k; if (!by.has(file)) by.set(file, []); by.get(file).push(k);
    }
    // (one drawing under several names: the one that names the sign and the kind, the shortest)
    const best = list => list.slice().sort((a, b) => (want === "huggie" ? (/\(HUGGIE\)$/.test(b) ? 1 : 0) - (/\(HUGGIE\)$/.test(a) ? 1 : 0) : 0) || a.length - b.length || a.localeCompare(b))[0];
    if (by.size === 1) return { sku: best([...by.values()][0]) };
    if (by.size > 1) return { ambiguous: [...by.values()].map(best).sort() };
    return other.length ? { other } : {};
  }
  /** The member of a NUMBERED family the shared SKU's drawing belongs to, at the sign's place (Aries 0 … Pisces 11): only when the family holds all twelve and the master's own sign names agree with the
   *  numbering (every named zodiac design sharing a drawing with a member sits at its sign's place, at least two do). null otherwise. */
  function signIndexed(sign, fam, want, ctx) {
    const me = ctx.masterEntry, at = SIGN_ORDER.indexOf(sign), guard = { masterEntry: me, masterLoose: ctx.masterLoose, masterShared: ctx.masterShared };
    for (const k of ctx.masterShared(fam.sku) || []) {
      const F = skuIndexFamily(k, me); if (!F) continue;
      for (let i = 0; i < 12; i++) if (!me(F.base + i)) return null;
      if (at < 0 || !signFormOk(want, masterForm(F.base + at, guard).form)) return null;
      let agree = 0;
      for (let i = 0; i < 12; i++) for (const n of ctx.masterShared(F.base + i) || []) { const t = zodiacSignOf(n, ctx); if (!t) continue; if (SIGN_ORDER.indexOf(t) !== i) return null; agree++; }
      return agree >= 2 ? { sku: upSku(F.base + at), base: F.base } : null;
    }
    return null;
  }
  const joinSigns = signs => signs.length > 1 ? signs.slice(0, -1).join(", ") + " and " + signs[signs.length - 1] : signs[0] || "";
  /** What a line with a shared zodiac SKU reads as: null (not one, no master list, or the form guard's line) | { family, sign?, from?, word?, typo?, text?, textOnly, variation?, pick?: { sku, how }, hold?: { kind, reason }, note }.
   *  lf / df: the line's form and the shared SKU's own design form (section 3b). */
  function zodiacRead(line, order, sku, lf, df, ctx) {
    if (!ctx || typeof ctx.masterSkus !== "function") return null;
    const fam = sharedZodiac(sku, ctx); if (!fam || formClash(lf, df)) return null;
    const etsy = upSku(line && line.sku) || fam.sku, nm = signsNamed(line, order), base = { family: fam.sku, textOnly: nm.textOnly };
    const shared = `SKU “${etsy}” is shared by every sign of this listing`;
    if (!nm.signs.length) return Object.assign(base, { hold: { kind: "which", reason: `Which zodiac sign? ${shared} and the buyer named none (${nm.box.length ? `the personalisation box says “${clip(nm.box.join(" · "), 40)}”` : "the personalisation box is empty"})` } });
    if (nm.signs.length > 1) return Object.assign(base, { hold: { kind: "which", reason: `Which zodiac sign? The buyer names ${joinSigns(nm.signs.map(x => x.sign))} (${shared})` } });
    const one = nm.signs[0], want = lf && lf.form && lf.form !== "unknown" ? lf.form : df && df.form && df.form !== "unknown" ? df.form : "";
    const note = `Zodiac sign: ${one.sign}, from ${one.from === "option" ? `the “${clip(one.name, 30)}” option` : SIGN_FROM[one.from]}${one.typo ? ` (typed “${clip(one.text, 30)}”)` : ""}`;
    const out = Object.assign(base, { sign: one.sign, from: one.from, word: one.word, typo: one.typo, text: one.text, note }, one.variation ? { variation: one.variation } : {});
    let r = signDesign(one.sign, want, ctx), how = "name";
    if (!r.sku && !r.ambiguous) { const ix = signIndexed(one.sign, fam, want, ctx); if (ix) { r = { sku: ix.sku }; how = "numbered"; } }
    if (r.sku) return Object.assign(out, { pick: { sku: r.sku, how } });
    if (r.ambiguous) return Object.assign(out, { hold: { kind: "many", reason: `The ${one.sign} charm has more than one design of this kind in the repo (${r.ambiguous.slice(0, 3).join(", ")}): a person picks` } });
    const o = (r.other || [])[0], kind = !o ? "" : FORM_THIS[o.form === "unknown" ? "necklace" : o.form] || "";
    return Object.assign(out, { hold: { kind: "missing", reason: `The ${one.sign} charm is not in the repo yet${o ? ` (the repo has it only as ${kind}: ${o.sku})` : ""}` } });
  }

  /** The pair of designs a person saved for this line's listing and SKU ({ L, R }, aliasPut pair), or null. The listing's own record and the SKU as the line carries it: no other listing, no other SKU, no title. */
  function pairLinkOf(line, aliases) {
    const a = aliases && line && aliases[String(line.listingId)], raw = upSku(line && line.sku), k = a && a.pairBySku && raw && bySkuOf(a.pairBySku, raw);
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
      rep.link = { L: link.L, R: link.R, miss };   // (SCHROLINK: a line that is not an earring pair is held on this, never made as one piece of the SKU's own design)
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
        const first = sl.signs[0] || {}, note = noteSays(sl.noteText);
        if (!sl.signs.length) rep.why = `the line says two different designs (${sig.signals[0] || "an option"}) but no sign is named: ${note}. Write the two symbols in the order's note, name the two designs, or say it is the same on both ears`;
        else if (sl.signs.length === 1) {
          // the note names the SAME sign as the drop-down (or only repeats it): both ears are that sign, nothing to ask (a Left and a Right, the Right the mirror of the Left)
          if (first.from === "option" && sl.said.length && sl.said.every(x => x === first.sign)) rep.same = { sign: first.sign, text: sl.noteText, why: `the buyer's note repeats ${first.sign}: the same sign on both ears` };
          else rep.why = `the line says two different designs (${sig.signals[0] || "an option"}) but names one (${first.sign}): ${note}. Write the second symbol in the order's note, name the second design, or say it is the same on both ears`;
        } else if (sl.signs.length > 2) rep.why = `the line says two different designs but names ${sl.signs.length} signs (${sl.signs.map(x => x.sign).join(", ")}): ${note}. Name the right two, or say it is the same on both ears`;
        else {
          rep.lack = rep.lack || {};
          const designs = sl.signs.map(x => { const virt = x.from === "option" ? x.variation : { name: sl.optionName, value: x.sign, propertyId: sl.propertyId }; return { x, sku: has(o.designOf(virt, x.from === "option") || "") }; });
          if (designs.every(d => d.sku) && designs[0].sku !== designs[1].sku) return Object.assign({ members: [{ side: "L", sku: designs[0].sku }, { side: "R", sku: designs[1].sku }], source: "signs", signs: rep.signs, note: sl.noteText }, sl.byNote ? { byNote: true, optionName: sl.optionName, optionValue: sl.optionValue } : {});
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
    info.earring = soldAs === "pair" || (info.mismatched && info.source === "name" && soldAs == null && !form && !sig.other && !sig.necklace);
    if (info.mismatched && !info.earring && soldAs !== "single") {
      const named = info.source !== "design" && info.source !== "name";   // (two designs the line names, not one design file that draws two bodies)
      info.mismatched = false; info.source = null; info.members = null; if (named) info.twoNamed = true; else info.twoBodies = true;
    }
    /* A SET OF TWO DESIGNS (SIBLINGS, Paul, 12 Oct 2026: a necklace whose listing wears two different charms, "SCHRODINGER CAT 1" and "SCHRODINGER CAT 2"): the pair of designs a PERSON
       saved for this listing and SKU (the saved pair link, source "alias", charm-nest-siblings.js suggests it) on a line that is not an earring pair is two pieces of ONE order line, each
       its own design, no Left and Right, nothing mirrored, counted like any other multi-piece line (units times two). Never read from words: a SKU or title that names two designs
       on a necklace is still one piece of the first design (twoNamed above), and an earring line keeps the Left and Right it always had. */
    const aliasSet = o.members && o.members.source === "alias" && Array.isArray(o.members.members) && o.members.members.length === 2 ? o.members.members.map(m => upSku(m && m.sku)) : null;
    if (aliasSet && aliasSet[0] && aliasSet[1] && aliasSet[0] !== aliasSet[1] && !info.earring && soldAs !== "single") { delete info.twoNamed; delete info.twoBodies; info.set = { designs: aliasSet, source: "alias" }; }
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
    if (info.set) info.perUnit = info.set.designs.length;   // (a set of two designs: two pieces for each unit bought, whatever a count option says)
    // what a person should see, plainly
    if (info.asks.some(a => a.rule === "ear:words")) info.notes.push("it waits until a person says whether this is a pair of earrings (" + askWords.why + ")");
    else if (earForm) info.notes.push("a person said this listing is " + (earForm === "huggie" ? "huggie hoops" : "a pair of earrings"));
    if (info.single && !info.sideSaid) info.notes.push("single earring: the line does not say left or right");
    else if (info.single && info.sideBy === "note" && +line.quantity > 1) info.notes.push(`${Math.round(+line.quantity)} single earrings and the buyer's note names one ear: which of them is which ear is not guessed`);
    if (info.second) info.notes.push(info.second.answered === "same" ? (info.second.by === "note" ? `the line says two different designs: ${info.second.why}` : "the line says two different designs: a person said the same design on both ears") : info.second.viaOption ? `waits for the second symbol's charm: ${info.second.why}` : "the line says two different designs but names one: it waits until a person names the second design or says it is the same on both ears");
    else if (sig.says && !info.mismatched) info.notes.push("the line says two different designs but is not an earring pair: nothing is changed");
    if (info.mismatched && info.source === "alias") info.notes.push("the Left and the Right are the two designs a person saved for this listing and SKU");
    if (info.set) info.notes.push(`two designs a person saved for this listing and SKU, ${info.set.designs.join(" and ")}: two pieces of one order line, no Left or Right, nothing mirrored`);
    // (the same outline twice, two keys or two mittens, on a line that is not an earring pair: ONE charm. A single earring that names no ear is such a line too.)
    if ((info.twoBodies || (info.single && info.mismatched && !info.sideSaid)) && alikeKnown(o)) info.oneBody = { index: 0, count: 2, rule: "the left body" };
    if (info.oneBody) info.notes.push(ONE_WORDS(info.oneBody.index, info.oneBody.count, "the same outline twice"));
    else if (info.twoBodies) info.notes.push("the design draws two bodies (a left and a right charm) but the line is not an earring pair: made as one piece with both bodies, no Left or Right, nothing mirrored");
    else if (info.twoNamed) info.notes.push("the line names two designs but is not an earring pair: made as one piece of the first design, no Left or Right");
    if (cr.note && !cr.answered && !(cr.certain && cr.n === cr.note.n) && !info.asks.length && !info.earring) info.notes.push(`the buyer's note says “${clip(cr.note.text, 30)}” but no option gives that count: made as ${info.perUnit}`);
    return info;
  }
  const unitsOf = x => { const s = x && x.spec, l = x && x.line; return Math.max(1, Math.round(+(s && s.quantity) || +(x && x.quantity) || +(l && l.quantity) || 1)); };
  const perUnitOf = p => Math.max(1, Math.floor(+(p && p.perUnit)) || 1);
  /** The form a RAW line's options choose (necklace, earrings, huggie, charm...), by the deterministic rules interpretLine applies to a line with no
   *  learned answers: a Huggie CHARM SET value, the default option map, a form word in a Type / Style / chain option. null when none. */
  function formOfLine(line, maps) {   // (maps: a person's saved option maps, read first: HUGGIEGUARD; none = the default rules only, as before)
    for (const v of (line && line.variations) || []) {
      const name = lvName(v), value = lvValue(v).trim(); if (!name || !value || isMetalOption(name) || isPersonalisation(name)) continue;
      if (HUGGIE_SET.test(value)) return "huggie";
      const hit = optionLookup(maps || null, line.listingId, name, value); if (hit && hit.field === "form") return hit.value;
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
  /** A COUNTED line ("ROSEGOLD - 2 Disc", "3 discs": n separate pieces of ONE necklace, more than the Etsy quantity says): { total, qty, noun } with total = pieceCountOf (the count the
   *  pipeline cuts, engraves and puts on sheets), qty = the Etsy quantity, noun = "disc" when the option named discs, else "piece". null for every other line: an earring pair, a single,
   *  a plain quantity-N line (pieces = quantity), an old glued record. Every place that SAYS how many pieces such a line is reads this (DISCREAD 3), so none says "1 piece" for two discs. */
  function countedPieces(x) {
    try {
      const sp = (x && x.spec) || {}, pr = sp.pair, pc = Math.floor(+sp.pieceCount), q = unitsOf(x);
      if (!pr || pr.kind !== "multi" || pr.earring || pr.glued || !(pc >= 2) || pc <= q) return null;
      return { total: pc, qty: q, noun: pr.discs > 0 && pr.discs * q === pc ? "disc" : "piece" };
    } catch (_) { return null; }
  }
  /** What a counted line adds to its Etsy quantity in words: " · 2 discs" (Quantity 1 · 2 discs); "" for every other line. */
  const countedFact = x => { const c = countedPieces(x); return c ? ` · ${c.total} ${c.noun}s` : ""; };
  /** The pieces of a line in order: [{ n, of, unit, side, bodyIndex }] (flat). An earring pair is a Left then a Right for every unit (L R L R),
   *  matching or mismatched (bodyIndex 0 then 1 for a mismatched design's own bodies); a single earring has the side its line names, or null;
   *  a glued mismatched copy, discs, letters and charms: no side. A piece an old line already has carries no side. */
  function piecesOf(x) {
    const p = pairOf(x), total = pieceCountOf(x), units = unitsOf(x), per = Math.max(1, Math.round(total / units)), natural = total === units * perUnitOf(p), out = [], set = natural ? setDesignsOf(p) : null;
    for (let i = 0; i < total; i++) {
      let side = null, bodyIndex = 0;
      if (natural && p.earring && !p.glued && per % 2 === 0) { side = i % 2 === 0 ? "L" : "R"; if (p.mismatched) bodyIndex = i % 2; }
      else if (natural && p.single && (units === 1 || p.sideBy === "listing")) side = p.sideSaid || null;
      // (a set of designs a person saved for a line that is not earrings, SCHROLINK: piece 1 of each unit is the first design, piece 2 the second; no side, nothing mirrored)
      out.push(set ? { n: i + 1, of: total, unit: Math.floor(i / per) + 1, side, bodyIndex, sku: set[i % set.length] } : { n: i + 1, of: total, unit: Math.floor(i / per) + 1, side, bodyIndex });
    }
    return out;
  }
  /** The designs of a SET a person saved for a line that is not an earring pair (spec.pair.set, pairInfo; [first, second]), as flat SKUs, else null: the line is not one, an earring pair, glued or an old line.
   *  Accepts the spec, its pair facts, or a row. */
  function setDesignsOf(x) {
    const p = x && x.spec && x.spec.pair ? x.spec.pair : x && x.pair && typeof x.pair === "object" ? x.pair : x;
    const d = p && !p.earring && !p.glued && !p.legacy && p.set && Array.isArray(p.set.designs) ? p.set.designs.map(v => String(v || "")) : null;
    return d && d.length >= 2 && d.every(Boolean) && new Set(d).size === d.length ? d : null;
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
  /** The pool read the design's geometry and found that a line that is not an earring pair draws the same outline twice (CharmNestPair.soloOf: { index, count, rule }): the line makes ONE charm, that body.
   *  Records it in spec.pair.oneBody and says so in spec.pair.notes in place of the words that said both bodies. The count, the sides and the kind do not change (one piece per unit, no side). */
  function solo(spec, one) {
    if (!spec || !spec.pair) return spec;
    // (no `one`: the line is NOT cut as one body after all (the geometry says its bodies are different charms, or it was pooled before with both): the intake's words about one body are taken back)
    if (!one) {
      const pr = spec.pair; if (!pr.oneBody) return spec;
      delete pr.oneBody; pr.notes = (Array.isArray(pr.notes) ? pr.notes : []).filter(n => !/made as ONE charm, the (?:left|right) body only/.test(String(n)));
      if (pr.twoBodies) pr.notes.push("the design draws two bodies (a left and a right charm) but the line is not an earring pair: made as one piece with both bodies, no Left or Right, nothing mirrored");
      return spec;
    }
    if (!Number.isInteger(one.index)) return spec;
    const pr = spec.pair, was = pr.oneBody;
    pr.oneBody = { index: one.index, count: +one.count || 2, rule: String(one.rule || "the left body") };
    if (was && was.index === pr.oneBody.index && was.rule === pr.oneBody.rule) return spec;
    pr.notes = (Array.isArray(pr.notes) ? pr.notes : []).filter(n => !/made as one piece with both bodies|made as ONE charm, the (?:left|right) body only|made as one glued piece/.test(String(n)));
    pr.notes.push(ONE_WORDS(one.index, one.count, "the same outline twice"));
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

  /* ═══ 3d · NO EARRING IS EVER ENGRAVED (ZODNOBACK, Paul 12 Oct 2026, order 4200256896) ═══════════════════════════════════
     (Paul: "for some reason that was interpreted as an engraving. No earrings are ever allowed to have back engraving, make that a rule forever." The listing was the zodiac
     huggie hoops; the buyer's sign, "Capricorn", sat in the personalisation box because the drop-down has no zodiac list; the engraving reader took it for words for the back, "45% sure".)
     THE RULE, in one place: a line whose FORM is "huggie" or "earring" (the form guard of section 3b: studs, hoops, huggies, zodiac / letter earrings, pairs, a huggie charm set; never a
     necklace, a bracelet, a keychain, a loose charm or a line of unknown form) has NO engraving, on either face, whatever it says: spec.engraveCandidate is false, spec.engraving is never
     written (so no "2 sides", "front and back", labelled words or option can make one), and spec.engraveNever says why. Every later reader asks this one fact, never the words again: the faces
     model (CharmNestEngraveFaces.setupOf, so a person's later pick cannot add one back: the pick is dropped quietly, nothing asks), Engrave (jobs, cards, seals, readiness), the order window,
     the Orders rows and the timeline. The buyer's words are NOT lost: they stay in spec.personalization (the sign word is the sign resolver's to read, interpretLine) and show as what the
     customer wrote; here they are only never an engraving and never a question.
     Not touched: an engraving an earring line already has APPROVED or WRITTEN (a seal is the permanent record; Engrave leaves a decided job as it is). A line whose form is not
     known to be earrings keeps its engraving exactly as before: when the title also names a necklace, a pendant, a bracelet or the like and only earring WORDS say earrings, the form is not
     sure and the rule stays out of it (never a necklace's back engraving lost to a guess). */
  const EAR_NEVER_WHY = "Earrings are never engraved (the shop's rule for every huggie, hoop and stud)";
  const EAR_OTHER_PRODUCT = /\b(?:necklaces?|pendants?|bracelets?|anklets?|key\s*(?:chains?|rings?)|keychains?|chokers?)\b/i;
  /** Does this line (a raw Etsy line, an interpretLine spec, or a row carrying one) never engrave? → { rule: "earring", form: "huggie" | "earring", by: plain words, why } or null.
   *  Takes `{ lineForm, designForm, form, pair, line }` too (what interpretLine knows when it asks). A spec that already carries its verdict (spec.engraveNever) answers with it. */
  function earringNeverBackEngraves(x) {
    if (!x || typeof x !== "object") return null;
    const row = x.parentRow || x, sp = row.spec && typeof row.spec === "object" ? row.spec : row;
    if (sp.engraveNever && typeof sp.engraveNever === "object" && sp.engraveNever.rule) return sp.engraveNever;
    const line = row.line && typeof row.line === "object" ? row.line : x.line && typeof x.line === "object" ? x.line : Array.isArray(row.variations) || row.title != null ? row : null;
    let lf = sp.lineForm, by = "";
    if (lf && typeof lf === "object") { by = String(lf.by || ""); lf = lf.form; }
    if ((lf === undefined || lf === null) && line) { try { const r = lineForm(line, {}); lf = r.form; by = String(r.by || ""); } catch (_) { lf = "unknown"; } }
    const df = sp.designForm && typeof sp.designForm === "object" ? sp.designForm.form : sp.designForm, unsure = !lf || lf === "unknown";
    let form = "";
    if (lf === "huggie" || lf === "earring") form = lf;
    else if (unsure && (df === "huggie" || df === "earring")) form = df;                                  // (the design drawn is a huggie / earring design, and nothing says otherwise)
    else if (unsure && (sp.form === "huggie" || sp.form === "earrings" || sp.form === "earring-single")) form = sp.form === "huggie" ? "huggie" : "earring";
    else if (unsure && sp.pair && sp.pair.earring) form = "earring";
    if (!form) return null;
    // earring WORDS alone, under a title that also names a necklace / pendant / bracelet: not sure (a bundle): the rule stays out of it
    if (line && by === "its earring words" && EAR_OTHER_PRODUCT.test(String(line.title || ""))) return null;
    return { rule: "earring", form, by: by || (unsure && (df === "huggie" || df === "earring") ? "its design" : "its form"), why: EAR_NEVER_WHY };
  }

  /* ═══ 3e · front and back engraving (ENGINTAKE, Paul 11 Oct 2026) ═══════════════════════════════════════════════════════
     (Paul, order 4195966039, Mini Tag listing 4458930445, Charm Type "Tag1 (front+back)", font Ace, personalisation "front - W" / "back - 1. 27. 24":
     "there must be an actual check on every single listing that is being processed to ensure that if there is back and front engraving, it's
     correctly identified and then interpreted: what is supposed to go on the front, what on the back, in terms of what the buyer chose.")
     Until now every line was read as a BACK engraving. engravingOf(line, spec) reads, for EVERY line whatever its listing, what the buyer chose and wrote,
     and interpretLine keeps it as spec.engraving. It answers null for a line that has no front / back word anywhere (the option values and names, the
     title, the personalisation, the notes): such a line is read exactly as before (the back, by the engraving reader). Otherwise:
       { mode: "none" | "front" | "back" | "both",
         front: { text, fontWord, fontId, fontFrom } | null,   back: the same,
         pieces?: { L: { front, back }, R: ... } | { D1: ... }  only when the buyer gave different words per ear / disc (an earring pair, a counted-disc line),
         slots?: ["L","R"] | ["D1".."Dn"],
         source: "option" | "title" | "note" | "person",       where the MODE came from (option = a drop-down value or name; note = labels or a phrase the buyer typed)
         modeFrom: the quoted words, textFrom: "personalization" | "staffNote" | "buyerMessage" | "messages" | "",
         confidence: 0..1, why: one plain line, raw: the buyer's words when a person must read them,
         review: null | { kind: "texts" | "mode" | "conflict" | "nowords", reason: one plain line } }   a person must look (never guessed, never blocks other lines)
     MODE (the buyer's choice): the option value, else the option name with a yes, else the title (front / both words only, and always to be confirmed), else the
       labels typed in the box (both labels = both), else a phrase typed in a note. Words that mean both: "front+back", "front + back", "front & back", "Front&Back",
       "front and back", "front/back", "both sides", "two sided", "engraved on both"; front: "front engrave", "front only", "engraved on front", "Writing-FRONT";
       back: "back engraving", "back only", "engraved on the back". "Truck Front Charm" (a design's name) is no engraving word: a face word counts only beside an
       engraving word, a joiner (+ & / and), "only", or as the whole value of an option named for an engraving / a side.
     TEXTS (what goes on each face), labelled forms only: "front - W / back - 1. 27. 24", "Front: A, Back: B", "front = A" "back = B", "front / A" "back / B", one label
       per line, either order, "on the front: A", "A on the front and B on the back", "Front of tag in font Britannic Bold: T", "Front: m - Vibur". Never guessed: mode
       both (or front) with unlabeled, mixed, doubled, or instruction-bearing words is marked for a person with the buyer's words kept in `raw`.
     FONT: each face has the font word the line's single font choice gives, unless the face names its own ("Front: m - Vibur", "Front in font Ace: W", a line "font: Ace").
     A PERSON's saved edit (o.person) always wins; the detector's own reading is kept beside it (`detected`) and never replaces it. */
  const E_ENG = "(?:engrav\\w*|engr\\b|etch\\w*|inscri\\w*|writing|handwrit\\w*|personali[sz]\\w*|monogram\\w*)";
  const E_SIDE = "(?:[\\s-]*side)?", E_JOIN = "(?:\\+|&|\\/|,|\\band\\b|\\bplus\\b)";
  const E_RX = s => new RegExp(s, "i");
  const FB_SIDES_2 = /\b(?:two|2|double|dual|dbl|dble)(?:\s*x)?[\s-]*(?:sided|sides?|faces?)\b|\bboth[\s-]*(?:sides?|faces?)\b/i, FB_SIDES_1 = /\b(?:one|1|single|sgl)(?:\s*x)?[\s-]*(?:sided|sides?|faces?)\b|\b(?:flower|bloom|design|art|artwork|picture|image|symbol|motif)s?[\s-]*only\b/i;   // (the number of engraved sides a value names: "Gold - 16\" - 2 sides", "single sided", "2x sided", "1side", "one side only", "flower only")
  const FB_BOTH = [E_RX(`\\bfront${E_SIDE}\\s*${E_JOIN}\\s*(?:the\\s+)?back\\b`), E_RX(`\\bback${E_SIDE}\\s*${E_JOIN}\\s*(?:the\\s+)?front\\b`), /\bboth[\s-]*(?:sides?|faces?)\b/i, /\b(?:two|2|double|dual|dbl|dble)(?:\s*x)?[\s-]*(?:sided|sides?|faces?)\b/i];
  const FB_BOTH_ENG = [E_RX(`\\b${E_ENG}\\s*[-–:()]*\\s*(?:on\\s+|at\\s+)?both\\b`)];
  const FB_FRONT = [E_RX(`\\bfront${E_SIDE}\\s*[-–:()]*\\s*(?:only\\b|${E_ENG})`), E_RX(`\\b${E_ENG}\\s*[-–:()]*\\s*(?:on\\s+|at\\s+|to\\s+|for\\s+|in\\s+)?(?:the\\s+)?front\\b`)];
  const FB_BACK = [E_RX(`\\bback${E_SIDE}\\s*[-–:()]*\\s*(?:only\\b|${E_ENG})`), E_RX(`\\b${E_ENG}\\s*[-–:()]*\\s*(?:on\\s+|at\\s+|to\\s+|for\\s+|in\\s+)?(?:the\\s+)?back\\b`)];
  const FB_NONE = /\bno[\s-]*engrav\w*|\bwithout\s+(?:any\s+)?engrav\w*|\bengrav\w*\s*[:\-]?\s*(?:none|no)\b|\bnot\s+engraved\b|\bno\s+personali[sz]ation\b/i;
  const FB_NEAR = new RegExp(E_ENG, "i");
  const FB_QUICK = /front|back|both|sided|faces?\b|sides?\b|only\b|blank/i;
  const FB_BLANK = /(?:^|[\s·|\-–+\/,])blank(?:$|[\s·|\-–+\/,])/i;   // a bar's "GOLD · Blank" / "Blank- SILVER": the buyer chose no engraving
  const FB_VALUE = /^\s*(?:the\s+)?(front|back|both)(?:[\s-]*sides?)?(?:\s+only)?\s*$/i, FB_NAME = /engrav|side|face|where|position|placement|location|inscri|writing|monogram|etch|personali[sz]/i;
  const FB_YES = /^\s*(?:yes|y|true|included?|add(?:ed)?|\+|1)\s*$/i, FB_NO = /^\s*(?:no|none|n\/a|false|0|-)\s*$/i;
  const FB_WORD = /(?:^|[^\p{L}\p{N}_])(?:front|back)(?![\p{L}\p{N}_])/iu;
  /** The mode the words of ONE text mean ("" for none): "front" | "back" | "both". `strict`: a joiner or "both sides" counts only beside an engraving word (a title, a message). */
  function engravingModeIn(text, strict, opt) {
    const t = decodeEntities(text == null ? "" : text);
    for (const r of FB_BOTH) { const m = r.exec(t); if (m && (!strict || FB_NEAR.test(t.slice(Math.max(0, m.index - 40), m.index + m[0].length + 40)))) return "both"; }
    if (opt && FB_BOTH_ENG.some(r => r.test(t))) return "both";   // ("engrave on both" in a note may mean two items, never two faces: only an option's own value says it)
    const f = FB_FRONT.some(r => r.test(t)), k = FB_BACK.some(r => r.test(t));
    return f && k ? "both" : f ? "front" : k ? "back" : "";
  }
  /** The mode one OPTION chose: its value ("Tag1 (front+back)"), else the value alone as a side ("Front" under an option named for a side or an engraving), else its name when the value says yes. */
  function engravingModeOfOption(name, value) {
    const n = decodeEntities(name || ""), v = decodeEntities(value || "").trim();
    let m = engravingModeIn(v, false, true);
    if (!m && FB_NAME.test(n)) for (const seg of v.split(/\s*[·|]\s*/)) { const f = FB_VALUE.exec(seg); if (f) { m = f[1].toLowerCase(); break; } }   // ("GOLD·Front" under "Metal/Both Sides Engraved? (Front/Back)")
    if (!m && FB_YES.test(v)) m = engravingModeIn(n, false);
    return m;
  }
  /** SIDES (BFSIDES, Paul 12 Oct 2026, order 4200828980: Birth Flower necklace "Gold - 16\" - 2 sides"): the number of engraved sides an option VALUE names, 2 / 1 / 0 (none named). Any option, any case, with
   *  metal and length words around it ("Silver- 16\"- 2 sides", "GOLD·DOUBLE SIDED", "one-sided", "single side", "both sides"). A value that also names a face ("Tag1 (front+back) 2 sided") says its faces itself, so it is no side count. */
  function sidesOfValue(value) {
    const v = decodeEntities(value == null ? "" : value).replace(/&nbsp;|\u00a0/gi, " ").trim(); if (!v) return 0;
    const two = FB_SIDES_2.test(v), one = FB_SIDES_1.test(v);
    if (two === one) return 0;   // (none, or both at once ("1 side / 2 sides"): the value does not say, nothing is guessed)
    const rest = v.replace(new RegExp(FB_SIDES_2.source, "ig"), " ").replace(new RegExp(FB_SIDES_1.source, "ig"), " ");
    return engravingModeIn(rest, false, true) || FB_NONE.test(rest) ? 0 : two ? 2 : 1;
  }
  /** THE POLICY for the two readings a person might want the other way round (BFSIDES, 12 Oct 2026; each is read when a line is read, so one line here flips it for every order, nothing else changes):
   *    oneSideWithWords  "skip" (default): "1 side" on a design with its own front art engraves nothing of the buyer's even when words were written: ONE calm line "Buyer chose 1 side; words not engraved: ...", the order is never held.
   *                      "ask": the same, and a person is asked (reviewKind "conflict"). "back": the words are engraved on the back anyway, as for 2 sides.
   *    twoSidesUnlabelled "back" (default): 2 sides on such a design puts the buyer's unlabelled words on the BACK, no question. "ask": the words are suggested for the back and a person confirms.
   *  Labelled words ("back: ...") are the back's in both; words the buyer labelled "front:" / "back:" against the side count are always a question for a person. */
  const SIDES_POLICY = { oneSideWithWords: "skip", twoSidesUnlabelled: "back" };
  /** A listing whose pictures or handwriting come from the buyer (a photo, a drawing, a signature): its design is only a blank carrier and its sides are the buyer's, whatever the master draws. */
  const SIDES_PICT_TITLE = /\b(?:hand[\s-]?writ\w*|photos?|photographs?|pictures?|drawings?|sketch\w*|logos?|fingerprints?)\b/i;
  /** Does a master design carry its own engraved front art (a flower, a sign, a picture) that the buyer's words only sit beside? { front: true | false | null, art, long, how }.
   *  An entry counts its members: the outline, the hanging holes (entry.holes), the BACK KEEP-OUT layers (entry.backKeepOut), and what is left is the design's artwork. A LONG design (a bar, a plate, a tag:
   *  its long side is LONG_ASPECT = 1.4 times its short side, as charm-nest-geom lies it flat) is a carrier of words whatever drawing it holds, so it never counts as art-first. (BFSIDES checked it on the 1,907 designs of the library
   *  index of 9 Oct 2026 against their real drawings: with the long-design guard it says "has art" wrongly for 6, and misses 27 that have some; a miss only leaves the line read as before.) An explicit entry.frontArt (a person's patch,
   *  a later index) wins over the count. null = the entry is not known or counts nothing: the line is read as before. */
  function frontArtOf(entry, title) {
    if (!entry || typeof entry !== "object") return { front: null, art: 0, long: false, how: "" };
    const w = +entry.widthPt, h = +entry.heightPt, long = w > 0 && h > 0 && Math.max(w, h) / Math.min(w, h) >= 1.4;
    if (typeof entry.frontArt === "boolean") return { front: entry.frontArt, art: entry.frontArt ? 1 : 0, long, how: "flag" };
    if (title && SIDES_PICT_TITLE.test(decodeEntities(String(title)))) return { front: false, art: 0, long, how: "kind" };   // (a handwriting / photo / drawing listing: the buyer's picture is the art, read as before)
    const m = Math.floor(+entry.members), n = Math.floor(+entry.holes);
    if (!(m >= 1) || !(n >= 0)) return { front: null, art: 0, long, how: "" };
    const art = m - 1 - n - (Array.isArray(entry.backKeepOut) ? entry.backKeepOut.length : 0);
    return { front: art > 0 && !long, art: Math.max(0, art), long, how: "index" };
  }

  /* ── the words of each face ─────────────────────────────────────────────────────────────────────────────────────────── */
  const FB_TAIL_OF = /^\s+of\s+(?:the\s+|my\s+|this\s+|each\s+)?(?:tag|charm|disc|disk|pendant|bar|piece|necklace|earring|it)s?(?![\p{L}\p{N}_])/iu;
  const FB_NOUN = /^\s+(?:engrav\w*|text|words?|wording|initials?|letters?|inscription|numbers?|date|says?|reads?|design)(?![\p{L}\p{N}_])/iu;
  const FB_FONT = /^\s+(?:in|with|using)\s+(?:the\s+)?(?:font\s+)?([^:=\-–—,;/\n]{2,30}?)(?=\s*(?:[:=\-–—,;/\n]|$))/i;
  const FB_INSTR = /\b(?:please|pls|thanks?|thank\s*you|capital|capitals|uppercase|upper\s*case|lowercase|lower\s*case|all\s*caps|cent(?:er|re)d?|font|typeface|size|bold|italics?|cursive|script|engrav\w*|hello|hi|hey|dear|would\s+like|i\s+want|i'd\s+like|make\s+it|leave\s+it|same\s+as|if\s+possible)\b/i;
  const isWordCh = c => !!c && /[\p{L}\p{N}_]/u.test(c);
  const trimFace = s => String(s == null ? "" : s).split("\n").map(x => x.replace(/[ \t]+/g, " ").trim()).filter(Boolean).join("\n")
    .replace(/^[\s,;/|&+:=\-–—]+/, "").replace(/[\s,;/|&+:\-–—]+$/, "").replace(/^and\s+/i, "").replace(/\s+and$/i, "").replace(/^(["'“‘])([\s\S]*)(["'”’])$/, "$2").trim();
  /** The labels of a body of text: [{ face, kind: "L" label first | "W" label and a space only | "D" text first ("A on the front") , start, end, own }] and the face words that are no label. */
  function faceLabels(body) {
    const labels = [], strays = [], re = /(front|back)(?![\p{L}\p{N}_])/giu;
    for (let m; (m = re.exec(body));) {
      if (isWordCh(body[m.index - 1])) continue;
      const face = m[1].toLowerCase(); let start = m.index, end = start + m[0].length, own = "", t, prep = false;
      const pre = /(?:^|[\s,;/|&+(\[])((?:on|at|in|for)\s+(?:the\s+)?)$/i.exec(body.slice(0, start));
      if (pre) { start -= pre[1].length; prep = true; }
      if ((t = /^[\s-]*side(?![\p{L}\p{N}_])/iu.exec(body.slice(end)))) end += t[0].length;
      if ((t = /^\s+only(?![\p{L}\p{N}_])/iu.exec(body.slice(end)))) end += t[0].length;
      if ((t = FB_TAIL_OF.exec(body.slice(end)))) end += t[0].length;
      if ((t = FB_FONT.exec(body.slice(end)))) { const w = t[1].replace(/\s+font$/i, "").trim(); if (fontOfPhrase(w)) { own = w; end += t[0].length; } }
      if ((t = FB_NOUN.exec(body.slice(end)))) end += t[0].length;
      const after = body.slice(end), before = body.slice(0, start);
      let sep = "", len = 0;
      if ((t = /^[ \t]*[:=][ \t]*/.exec(after))) { sep = "colon"; len = t[0].length; }
      else if ((t = /^[ \t]*[-–—]+[ \t]*/.exec(after))) { sep = "dash"; len = t[0].length; }
      else if ((t = /^[ \t]*\/[ \t]*/.exec(after))) { sep = "slash"; len = t[0].length; }
      else if ((t = /^[ \t]*\n[ \t]*/.exec(after))) { sep = "newline"; len = t[0].length; }
      else if (/^[ \t]*[.!?]*[ \t]*$/.test(after)) sep = "end";
      else if ((t = /^[ \t]*[,;|&+][ \t]*/.exec(after))) { sep = "list"; len = t[0].length; }
      else if ((t = /^[ \t]+and(?![\p{L}\p{N}_])[ \t]*/iu.exec(after))) { sep = "and"; len = t[0].length; }
      else if ((t = /^[ \t]+(?=\S)/.exec(after))) { sep = "space"; len = t[0].length; }
      const prev = /(?:^|\n)[ \t]*$/.test(before) ? "line" : /(?:[,;|&+/(\[]|\band\b)[ \t]*$/i.test(before) ? "list" : "text";
      let kind = "";
      if (sep === "colon") kind = "L";
      else if (prev !== "text" && ["dash", "slash", "newline", "end"].includes(sep)) kind = "L";
      else if (prev !== "text" && sep === "space") kind = "W";
      else if (prev === "text" && prep && ["end", "newline", "list", "and", "slash"].includes(sep)) kind = "D";
      if (kind === "D") len = 0;
      else if (!kind && prev === "text" && !prep && sep === "space") kind = "X";   // "front A back B": the second face word follows the first one's words
      if (kind) labels.push({ face, kind, start, end: end + len, own }); else strays.push(face);
    }
    // a face word that follows words with a space only is a label when it is the one other face word of the text ("front A back B", "front: A back B"); else it is part of the words
    const xs = labels.filter(l => l.kind === "X"), clear = labels.filter(l => l.kind !== "X");
    if (xs.length && !(xs.length === 1 && !strays.length && clear.length === 1 && clear[0].face !== xs[0].face)) { for (const l of xs) strays.push(l.face); labels.splice(0, labels.length, ...clear); }
    else for (const l of xs) l.kind = "L";
    // a label with a space only ("front W / back 1.27") is a label when a clear one stands beside it, or when both faces are named; alone it may be part of the words ("Front Row Seats")
    const strong = labels.filter(l => l.kind === "L"), weak = labels.filter(l => l.kind === "W");
    const promote = weak.length && (strong.length || new Set(weak.map(l => l.face)).size === 2);
    return { labels: labels.filter(l => l.kind !== "W" || promote).map(l => (l.kind === "W" ? Object.assign({}, l, { kind: "L" }) : l)), strays: strays.concat(promote ? [] : weak.map(l => l.face)) };
  }
  /** A font named after the words ("m - Vibur", "W (Vibur)", "A in Vibur font"): { text, word }. "Ace" is a name as often as a font: it counts only with the word font beside it. */
  function faceFont(text) {
    const m = /^([\s\S]*?\S)\s*(?:[-–—]|[(\[]|\b(?:in|with|using)\b)\s*(?:the\s+)?(font\s+)?([A-Za-z][A-Za-z ]{1,20}?)(\s+font)?\s*[)\]]?\s*$/i.exec(text);
    if (!m) return { text, word: "" };
    const word = m[3].trim(), hasFont = !!(m[2] || m[4]);
    if (!fontOfPhrase(word) || (/^ace$/i.test(word) && !hasFont)) return { text, word: "" };
    return { text: m[1].trim(), word };
  }
  /** Words before the first label that only greet or ask ("Hello!", "please engrave on the front: W"): no inscription in them, so they are no reason to stop. */
  const FB_FILL = new Set("please pls kindly can could would you i id i'd to have has put place write add engrave engraved engraving inscribe the my this that following text words word wording on at in for it and thanks thank thx hello hi hey dear want like need".split(" "));
  const askOnly = t => { const w = String(t || "").toLowerCase().replace(/[^\p{L}\p{N}' ]+/gu, " ").split(/\s+/).filter(Boolean); return w.length > 0 && w.every(x => FB_FILL.has(x)) && !(w.length === 1 && (w[0] === "i" || w[0] === "id")); };
  /** A line that asks for something ("both center", "WORD upside down if possible", "darkened font", "THANK YOU <3") and is no inscription. */
  const FB_REQ = /\b(?:upside\s*down|dark(?:en\w*)?|deeper?|sideways|vertical(?:ly)?|horizontal(?:ly)?|proof|possibl\w*|poss\b|also|thanks)\b|<3|^[\s\-•*]*poss/i;
  const askLine = l => FB_INSTR.test(l) || FB_REQ.test(l);
  /** One face's block of words: a blank line ends it when everything after it is a request ("front: go birds / back: WORD1 [blank] both center / upside down / THANK YOU"). */
  function cutBlock(raw) {
    const m = /^([\s\S]*?\S)[ \t]*\n[ \t]*\n([\s\S]*)$/.exec(String(raw));
    if (!m) return { text: raw, gap: false };
    const rest = m[2].split("\n").map(x => x.trim()).filter(Boolean);
    return !rest.length || rest.every(askLine) ? { text: m[1], gap: false } : { text: raw, gap: true };
  }
  /** "T (capital letter)" / "mine (lowercase)": a closing note in brackets about HOW to write the words is no part of them. */
  function stripNote(t) { const m = /^([\s\S]*?\S)\s*\(([^()]*)\)\s*$/.exec(String(t)); return m && askLine(m[2]) ? m[1] : t; }
  const plainInscription = t => { const ls = String(t || "").split("\n"); return ls.length <= 3 && ls.every(l => l.length <= 40) && !FB_INSTR.test(t) && !/[()\[\]]/.test(t); };
  /** The faces a text labels: { n (labels taken), front, back (each { text, own } or null), problems: [codes], strays, lead }.
   *  problems: lead (words before the first label), empty (a label with no words), dup (a face labelled twice), mixed (two label styles), tail (words after the last "on the back"). */
  function faceParse(body) {
    const out = { n: 0, front: null, back: null, problems: [], strays: [], lead: "", named: [] };
    const { labels, strays } = faceLabels(body); out.strays = strays; out.n = labels.length; out.named = [...new Set(labels.map(l => l.face))];
    if (!labels.length) return out;
    const L = labels.filter(l => l.kind === "L"), D = labels.filter(l => l.kind === "D");
    if (L.length && D.length) { out.problems.push("mixed"); return out; }
    if (D.length && /\?/.test(body)) { out.problems.push("ask"); return out; }   // "Can the initial be on the front and the date on the back?": a question to the shop, not the words
    const segs = [];
    if (L.length) {
      out.lead = trimFace(body.slice(0, L[0].start)); if (out.lead && !askOnly(out.lead)) out.problems.push("lead");
      L.forEach((l, i) => segs.push({ l, raw: body.slice(l.end, i + 1 < L.length ? L[i + 1].start : body.length) }));
    } else {
      let at = 0;
      D.forEach(l => { segs.push({ l, raw: body.slice(at, l.start) }); at = l.end; });
      const tail = trimFace(body.slice(at)).replace(/^[.!]+$/, "");
      if (tail && !/^(?:please|pls|thanks?|thank you|thx)[.!]*$/i.test(tail)) out.problems.push("tail");
    }
    for (const s of segs) {
      const cb = L.length ? cutBlock(s.raw) : { text: s.raw, gap: false};
      if (cb.gap && !out.problems.includes("gap")) out.problems.push("gap");
      const t = stripNote(trimFace(cb.text)), f = faceFont(t), face = s.l.face;
      if (out[face]) { if (!out.problems.includes("dup")) out.problems.push("dup"); continue; }
      if (!f.text) { out.problems.push("empty"); out[face] = { text: "", own: s.l.own || f.word }; continue; }
      out[face] = { text: f.text, own: s.l.own || f.word };
    }
    return out;
  }
  const FB_PIECE_EAR = /(left|right)(?:\s*ear(?:ring)?s?)?(?![\p{L}\p{N}_])/giu;
  const FB_ORD = { first: 1, second: 2, third: 3, fourth: 4, fifth: 5, sixth: 6, "1st": 1, "2nd": 2, "3rd": 3, "4th": 4, "5th": 5, "6th": 6 };
  const FB_PIECE_DISC = /(?:(?:disc|disk|tag|charm|initial|letter|pendant)\s*#?\s*(\d{1,2})|(first|second|third|fourth|fifth|sixth|1st|2nd|3rd|4th|5th|6th)\s+(?:disc|disk|tag|charm|initial|letter|pendant))(?![\p{L}\p{N}_])/giu;
  /** A text cut at the pieces it names ("Left: front A back B / Right: front C back D", "Disc 1: ..."): [{ slot, raw }] and the words before the first, or null when it names none. */
  function pieceSegments(body, slots) {
    const ears = slots.includes("L") || slots.includes("R"), discs = slots.some(s => /^D\d/.test(s));
    if (!ears && !discs) return null;
    const hits = [], re = new RegExp((ears ? FB_PIECE_EAR : FB_PIECE_DISC).source, "giu");
    for (let m; (m = re.exec(body));) {
      if (isWordCh(body[m.index - 1])) continue;
      const slot = ears ? (m[1].toLowerCase() === "left" ? "L" : "R") : "D" + (+m[1] || FB_ORD[String(m[2]).toLowerCase()] || 0);
      const after = body.slice(m.index + m[0].length), before = body.slice(0, m.index);
      const prev = /(?:^|\n)[ \t]*$/.test(before) ? "line" : /(?:[,;|&+/(\[]|\band\b)[ \t]*$/i.test(before) ? "list" : "text";
      const sep = /^[ \t]*[:=]/.test(after) ? "colon" : /^[ \t]*(?:[-–—]|\/|\n)/.test(after) ? "dash" : /^[ \t]+\S/.test(after) ? "space" : "";
      if (sep === "colon" || (prev !== "text" && sep)) hits.push({ slot, start: m.index, end: m.index + m[0].length + /^[ \t]*(?:[:=]|[-–—]+|\/)?[ \t]*/.exec(after)[0].length });
    }
    if (!hits.length) return null;
    const segs = hits.map((h, i) => ({ slot: h.slot, raw: body.slice(h.end, i + 1 < hits.length ? hits[i + 1].start : body.length) }));
    return { lead: body.slice(0, hits[0].start), segs };
  }
  const faceNames = { front: "front", back: "back" };
  /** Words that say the writing comes as a picture, not as text. */
  const PICT = /\b(?:pictures?|photos?|photographs?|images?|handwrit\w*|hand[\s-]?written|drawings?|sketch\w*|logos?|artwork)\b/i;
  /** The one word or phrase in double quotes ("JFC" is the engraving ...), or "" when there is none or more than one. */
  const quotedOne = t => { const q = String(t || ""); if ((q.match(/[“”"«»]/g) || []).length !== 2) return ""; const m = /[“"«]([^“”"«»\n]{1,40})[”"»]/.exec(q); return m ? m[1].trim() : ""; };
  const REASON = {
    lead: "words come before the first front / back label",
    empty: "a front / back label has no words",
    dup: "the same face is labelled twice",
    mixed: "the front / back labels are written two different ways",
    ask: "the note is a question or a request, not the words to engrave",
    gap: "more words follow a blank line, and they are not a request",
    tail: "words are left over after the last label",
    dupPiece: "the same ear or disc is named twice",
    missingPiece: "not every ear or disc is named",
    pieceWords: "the words for an ear or disc do not say which is the front and which the back"
  };
  const sayFace = f => (f && f.text ? `“${clip(f.text, 30)}”` : "nothing");
  const uniqStr = arr => [...new Set(arr)];
  const pieceName = s => (s === "L" ? "the left ear" : s === "R" ? "the right ear" : "disc " + String(s).slice(1));
  /** CUSTOMER INSTRUCTIONS IN A MESSAGE (ENGAUTO, Paul 12 Oct 2026, point 5: the defaults follow "customer instructions"). A buyer's message (never "front desk" or "back at work") that ASKS for engraving on a face with words between the verb
   *  and the face ("please put rr on the front", "can you engrave my initials on the front?"): the faces it names, [] when none. engravingModeIn needs the engraving word right beside the face, so these were read as nothing
   *  and the line was engraved on the back as if the message had never been written. Asking only: the caller never takes words from a message. */
  const MSG_FACE_ASK = /\b(?:engrav\w*|inscri\w*|etch\w*|put|add|write|place|want|need|like)\b[^.!?\n]{0,60}?\b(?:on|at)\s+(?:the\s+|its\s+|my\s+)?(front|back)\b(?!\s+(?:side\s+)?of\s+(?:the\s+)?(?:box|package|packaging|card|envelope|bag|order|receipt))/gi;
  function messageFaceAsk(texts) {
    const out = new Set();
    for (const t of texts) { const body = decodeEntities(String(t || "")); MSG_FACE_ASK.lastIndex = 0; for (let m; (m = MSG_FACE_ASK.exec(body));) out.add(m[1].toLowerCase()); }
    return ["front", "back"].filter(f => out.has(f));
  }
  /** What the buyer chose and wrote about the faces of ONE line (the CONTRACT's spec.engraving, plans/front-back-1011/CONTRACT.md section 2). line: the Etsy line (variations, title,
   *  personalization); spec: its reading (personalization, buyerMessage, staffNote, messages, font, pair, pieceCount). null when the line has no face word anywhere: such a line has no
   *  `spec.engraving`, which the faces model reads as today's default (the back, as the engraving reader always read it). Never a person's word: that is row.engravingOverride. */
  function engravingOf(line, spec, art) {
    line = line || {}; spec = spec || {}; art = art || { front: null };
    // a rework / modification fee line ("Re-work: $5 back engraving fee") and any line with no design of its own is never an engraving of its own
    if (spec.noDesign || /\b(?:re-?work|modifications?)\b/i.test(String(line.title || ""))) return null;
    // ZODNOBACK (section 3d): an earring is never engraved, on either face: no mode, no side count, no label, no option or title can make one, so nothing below is read at all
    if (spec.engraveNever) return null;
    const opts = (line.variations || []).map(v => { const id = idOf(v && (v.valueId != null ? v.valueId : v.value_id)); return { name: decodeEntities(lvName(v)).trim(), value: decodeEntities(lvValue(v)).trim(), drop: !!id && id !== "0" }; }).filter(v => v.name || v.value);
    const asText = x => visible(x).replace(/\r/g, "").trim();
    // (every line passes here: a line that mentions no face at all is answered at once)
    if (!FB_QUICK.test([line.title, spec.buyerMessage, line.buyerMessage, spec.staffNote, line.staffNote].concat(spec.personalization || [], line.personalization || [], (spec.messages || line.messages || []).map(m => m && m.text), opts.map(v => v.name + " " + v.value)).join("\n"))) return null;
    // a drop-down Etsy happens to call "Personalization" has a value id: it is an option (its value is no words); the text box has none
    const free = v => isPersonalisation(v.name) && !v.drop;
    const given = (spec.personalization && spec.personalization.length ? spec.personalization : line.personalization) || [];
    const dropVals = new Set(opts.filter(v => v.drop && isPersonalisation(v.name)).map(v => asText(v.value)));   // (main still reads these as buyer text; here they only say the mode)
    const pers = uniqStr([].concat(given, opts.filter(v => v.value && free(v)).map(v => v.value)).map(asText).filter(t => t && !dropVals.has(t)));
    const buyer = asText(spec.buyerMessage || line.buyerMessage || ""), staff = asText(spec.staffNote || line.staffNote || "");
    const msgs = (spec.messages || line.messages || []).map(m => asText(m && m.text)).filter(t => t && engravingNote(t));
    const title = decodeEntities(line.title || "");
    // the pieces that can carry their own words: an earring pair (L, R) or a counted-disc line (D1..Dn); a single charm has none
    const slots = (() => {
      const pr = spec.pair, pc = Math.floor(+spec.pieceCount) || 0;
      if (pr && pr.earring && !pr.glued && pc >= 2) return ["L", "R"];
      const per = pr && !pr.earring && !pr.glued && pr.kind === "multi" && pc >= 2 ? Math.round(pc / Math.max(1, Math.round(+spec.quantity) || 1)) : 0;
      return per >= 2 && per <= 24 ? Array.from({ length: per }, (_, i) => "D" + (i + 1)) : [];
    })();

    // ── what the buyer chose: the mode ──
    // an option the line reader already settled says the engraving itself (TAGSKU / CONTRACT section 13: spec.options[].mapped.engrave, from a person's saved answer for the value or the Mini Tag rule):
    // that exact word is used instead of reading the option's text again, and a person's saved answer stays above the text
    const said = new Map();
    for (const o of spec.options || []) if (o && o.mapped && /^(?:front|both|back)$/.test(String(o.mapped.engrave || ""))) said.set(decodeEntities(String(o.name || "")).trim() + "\n" + decodeEntities(String(o.value == null ? "" : o.value)).trim(), o.mapped.engrave);
    const hits = [];
    for (const v of opts) {
      if (free(v) || !v.value) continue;
      const asked = said.get(v.name + "\n" + v.value); let mode = asked || engravingModeOfOption(v.name, v.value), side = 0;
      // SIDES (BFSIDES): a value that only counts sides ("Gold - 16\" - 2 sides") on a design that already carries its own front art (a birth flower, a zodiac sign, a disc with a picture) engraves the BUYER'S words on the
      // back for 2 sides (the front is the design's art, untouched) and nothing of the buyer's for 1 side. A design with no art of its own (a tag, a bar, a plain disc) keeps both faces as buyer text, as before;
      // a person's saved answer for the value (`said`) and a value that names its faces stay above this.
      if (!asked && art.front === true) { const n = sidesOfValue(v.value); if (n) { side = n; mode = n === 2 ? "back" : "none"; } }
      if (mode) hits.push({ mode, name: v.name, value: v.value, from: `${v.name}: ${v.value}`, side });
    }
    const noneOpt = opts.find(v => !free(v) && v.value && (FB_NONE.test(v.value) || (/engrav/i.test(v.name) && FB_NO.test(v.value))));
    const blankOpt = !noneOpt && !hits.length && pers.length ? opts.find(v => !free(v) && v.value && FB_BLANK.test(v.value)) : null;
    const titleMode = engravingModeIn(title, true);
    // font lines ("font: Ace", "Ace font") are not words to engrave: set aside and read as the buyer's font
    let noteFont = "";
    // BFFONT: and the lines / phrases the tolerant font reader knows (a misspelling, "Pristine font", "in cursive please", "Courier New font", "3. Typewriter Font"): their font is spec.fontText, they are not words to engrave
    const stripFont = txt => fontReadText(String(txt || "").split("\n").filter(l => {
      const f = /^\s*(?:(?:the\s+)?font(?:\s+choice)?|typeface|lettering)\s*(?:is|[:=\-–—])\s*(.+?)\s*$/i.exec(l) || /^\s*(.+?)\s+font\s*$/i.exec(l);
      if (f && fontOfPhrase(f[1])) {
        // (a line that holds WORDS beside the font - "D+R in the Ace font", "please engrave “MC” in the uppercase Ace font" - is never dropped whole: the reader below takes the font phrase out and leaves the
        //  words; a sentence stays whole for the front / back checks that hold a non-plain inscription. Only a line that is nothing but the font goes, as it always did.)
        const rd = fontReadText(l, { keepFaceLines: true });
        if (rd.requests.length && (rd.text.trim() || !rd.changed)) return true;
        noteFont = noteFont || f[1].trim(); return false;
      }
      return true;
    }).join("\n"), { keepFaceLines: true }).text;
    const persText = stripFont(pers.join("\n"));
    const pools = [["staffNote", staff], ["personalization", persText], ["buyerMessage", buyer], ["messages", msgs.join("\n")]].map(([k, t]) => ({ k, text: k === "personalization" ? t : stripFont(t) })).filter(p => p.text.trim());
    const readPool = p => {
      const cut = slots.length > 1 && p.k === "personalization" ? pieceSegments(p.text, slots) : null;
      if (!cut) return Object.assign({ k: p.k, text: p.text, pieces: null }, faceParse(p.text));
      const lead = faceParse(cut.lead), out = { k: p.k, text: p.text, n: lead.n, front: lead.front, back: lead.back, problems: lead.problems.slice(), strays: lead.strays.slice(), lead: "", pieces: {} };
      for (const s of cut.segs) {
        if (out.pieces[s.slot]) { if (!out.problems.includes("dupPiece")) out.problems.push("dupPiece"); continue; }
        const r = faceParse(s.raw); out.n += r.n; out.strays.push(...r.strays); out.problems.push(...r.problems.filter(x => !out.problems.includes(x)));
        out.pieces[s.slot] = Object.assign(r, { words: trimFace(s.raw) });
      }
      if (slots.some(x => !out.pieces[x]) && !out.problems.includes("missingPiece")) out.problems.push("missingPiece");
      return out;
    };
    // the buyer's message and the shop's conversations are free talk: their face words are labels only on a line whose option or title already says front / back (else "front desk ... back at work" is no engraving)
    const noteLabels = hits.length > 0 || !!titleMode;
    const parsed = pools.map(p => (p.k === "buyerMessage" || p.k === "messages") && !noteLabels ? { k: p.k, text: p.text, pieces: null, n: 0, front: null, back: null, problems: [], strays: [], lead: "" } : readPool(p));
    const labelled = parsed.filter(p => p.n > 0 || p.pieces);
    const noneHit = noneOpt ? (pools.some(p => p.k === "personalization" && FB_WORD.test(p.text)) ? noneOpt : null) : blankOpt;
    const anyFace = hits.length || titleMode || labelled.length || pools.some(p => engravingModeIn(p.text, p.k !== "personalization")) || noneHit;
    if (!anyFace) {
      // a message that asks for the FRONT (or both faces) while nothing else says which face: the line is not read "as before" in silence, a person says what goes on each face (only a back asked for in a message is the default anyway)
      const ask = messageFaceAsk([buyer].concat(msgs));
      if (!ask.includes("front")) return null;
      const raw0 = [pers.join("\n"), buyer || msgs.join("\n")].filter(Boolean).join("\n").slice(0, 300);
      return { v: 1, mode: "both", source: "note", front: null, back: null, textSource: "unlabelled", needsPerson: true, confidence: 0.3, why: clip(`the buyer's message asks for engraving on the ${ask.join(" and the ")}, but nothing else says which words go where: a person says what goes on each face`, 170), option: null, reviewKind: "texts", raw: raw0 };
    }
    const lf = noteFont ? { word: noteFont } : spec.font && spec.font.asked ? { word: String(spec.font.asked), line: true } : { word: "" };
    const faceOut = f => {
      if (!f || !f.text) return null;
      const ls = String(f.text).split("\n").map(x => x.replace(/[ \t]+/g, " ").trim()).filter(Boolean);
      return ls.length ? { text: ls.join("\n"), lines: ls, fontWord: f.own || noteFont || null } : null;
    };

    // ── the mode and where it came from ──
    let mode = "", source = "", modeFrom = "", conflict = "";
    const modes = uniqStr(hits.map(h => h.mode));
    if (modes.length) { mode = hits[0].mode; source = "option"; modeFrom = hits[0].from; if (modes.length > 1) conflict = `two options disagree: “${clip(hits[0].from, 40)}” and “${clip(hits.find(h => h.mode !== mode).from, 40)}”`; }
    else if (noneHit) { mode = "none"; source = "option"; modeFrom = `${noneHit.name}: ${noneHit.value}`; }   // (the buyer chose no engraving: that stays, and words that came with it are a conflict)
    else if (titleMode) { mode = titleMode; source = "title"; modeFrom = clip(title, 60); }
    const pick = labelled.find(p => !p.problems.length) || labelled[0] || null;   // the first source that labels the faces (the staff note first: it overrides the buyer's field)
    const namedFaces = p => { const set = new Set(); for (const q of [p].concat(Object.values(p.pieces || {}))) for (const f of q.named || []) set.add(f); return ["front", "back"].filter(f => set.has(f)); };
    if (!mode) for (const p of pools) { const m = engravingModeIn(p.text, p.k !== "personalization"); if (m) { mode = m; source = "note"; modeFrom = `what the buyer wrote in ${p.k === "personalization" ? "the personalisation" : p.k === "staffNote" ? "the staff note" : "a note"}`; break; } }
    if (!mode && pick) {
      const named = namedFaces(pick);
      mode = named.length === 2 ? "both" : named[0] || "";
      if (mode) { source = "note"; modeFrom = `the ${named.join(" and ")} labels in ${pick.k === "personalization" ? "the personalisation" : pick.k === "staffNote" ? "the staff note" : "a note"}`; }
    }
    if (!mode) return null;
    const optHit = source === "option" ? (hits.length ? { name: hits[0].name, value: hits[0].value } : noneHit ? { name: noneHit.name, value: noneHit.value } : null) : null;
    const sideN = source === "option" && hits.length && hits[0].side && !conflict ? hits[0].side : 0;   // (a side count read on a design with its own front art: the buyer's words on the back, or none)
    const sideWords = sideN === 1 ? persText.trim() : "", oneWay = sideWords ? String(SIDES_POLICY.oneSideWithWords || "skip") : "";   // (1 side and words anyway: see SIDES_POLICY)
    const sideBack = sideN === 2 || (sideN === 1 && oneWay === "back"), sideNone = sideN === 1 && !sideBack;
    if (sideN === 1 && sideBack) mode = "back";

    // ── the texts: what goes on each face ──
    const eng = { v: 1, mode, source, front: null, back: null, textSource: "none", needsPerson: false, confidence: 0.5, why: "", option: optHit };
    let review = null;
    const flag = (kind, reason, partial) => { if (!review) review = { kind, reason: clip(reason, 170), partial: !!partial }; };
    const raw = (pers.length ? pers.join("\n") : [staff, buyer, msgs.join("\n")].filter(Boolean).join("\n")).slice(0, 300);
    const modeWords = { front: "front", back: "back", both: "front and back", none: "no engraving" }[mode];
    const fromWhat = source === "option" ? `the option “${clip(sideN ? hits[0].value : modeFrom, 50)}”` : source === "title" ? "the listing title" : modeFrom;
    const hand = source === "option" && /hand\s*writ|writing/i.test(modeFrom);
    const picture = PICT.test(raw) || hand;
    let single = false, quoted = false;
    if (conflict) flag("conflict", conflict);
    const backOnly = mode === "back" && !(pick && (pick.front || Object.values(pick.pieces || {}).some(q => q.front)));   // (back only: the engraving reader reads the words as it always did; only a front label would be a conflict)
    if (pick && !backOnly && pick.k !== "personalization" && pick.k !== "staffNote") flag("texts", `${modeWords} engraving, but the words are only in a note, not in the personalisation box: a person confirms what goes on each face`);
    if (pick && backOnly) { /* nothing to read here */ }
    else if (pick && pick.problems.length) flag("texts", `${modeWords} engraving, but ${REASON[pick.problems[0]] || "the words are not clear"}: a person says what goes on each face`);
    else if (pick) {
      const shared = { front: pick.front, back: pick.back };
      const fsets = pick.pieces ? slots.map(sl => { const r = pick.pieces[sl]; return { slot: sl, front: r && r.front || shared.front, back: r && r.back || shared.back, words: r && r.words, labelled: !!(r && (r.front || r.back)) }; }) : null;
      // a named piece with plain words and no label takes them on the one face a front-only (or back-only) line has
      if (fsets) for (const x of fsets) if (!x.labelled && x.words) { if (mode === "front") x.front = { text: x.words }; else if (mode === "back") x.back = { text: x.words }; }
      const has = f => (fsets ? fsets.some(x => x[f]) : !!shared[f]);
      const allHave = f => (fsets ? fsets.every(x => x[f]) : !!shared[f]);
      const every = fsets ? fsets : [shared];
      const odd = every.flatMap(x => [x.front, x.back]).filter(x => x && !plainInscription(x.text) || x && slots.length > 1 && /\b(?:left|right)\b/i.test(x.text));
      if (mode === "both" && !(allHave("front") && allHave("back"))) {
        const gap = fsets && fsets.find(x => !x.front || !x.back);
        flag("texts", gap ? (!gap.labelled && gap.words ? `front and back engraving, but the words for ${pieceName(gap.slot)} do not say which is the front and which the back: a person decides` : `front and back engraving, but ${pieceName(gap.slot)} is missing its ${gap.front ? "back" : "front"}: a person says what goes on it`)
          : `front and back engraving, but only the ${shared.front ? "front" : "back"} is written: a person says what goes on the ${shared.front ? "back" : "front"}`, !fsets);
      }
      else if (mode === "front" && has("back")) flag("conflict", `front engraving chosen but the words are labelled back${has("front") ? " too" : ""}: a person says which face`);
      else if (mode === "back" && has("front")) flag("conflict", `back engraving chosen but the words are labelled front${has("back") ? " too" : ""}: a person says which face`);
      else if (mode === "none") flag("conflict", sideNone ? "1 side was chosen (the design's own art only), but the buyer labelled front / back words: a person decides" : "the option says no engraving, but the buyer labelled front / back words: a person decides");
      else if (mode === "front" && !allHave("front")) flag("texts", `front engraving, but ${pieceName((fsets.find(x => !x.front) || {}).slot)} has no words: a person says what goes on it`);
      if (odd.length) flag("texts", `the words include instructions or notes (${odd.slice(0, 2).map(x => `“${clip(x.text, 30)}”`).join(", ")}): a person writes exactly what is engraved`);
      if (!review || review.partial) {
        if (fsets) {
          const sig = x => JSON.stringify([x.front && x.front.text, x.back && x.back.text, x.front && x.front.own, x.back && x.back.own]);
          const face = x => ({ front: mode === "back" ? null : faceOut(x.front), back: mode === "front" ? null : faceOut(x.back) });
          if (fsets.every(x => sig(x) === sig(fsets[0]))) Object.assign(eng, face(fsets[0]));
          else {
            eng.pieces = {}; for (const x of fsets) eng.pieces[x.slot] = face(x);
            flag("pieces", `each ${slots[0] === "L" ? "ear" : "disc"} has its own words (${clip(slots.map(sl => `${pieceName(sl)}: ${[eng.pieces[sl].front && `front ${sayFace(eng.pieces[sl].front)}`, eng.pieces[sl].back && `back ${sayFace(eng.pieces[sl].back)}`].filter(Boolean).join(", ")}`).join("; "), 110)}): a person enters them for each`);
          }
        } else { eng.front = mode === "back" ? null : faceOut(shared.front); eng.back = mode === "front" ? null : faceOut(shared.back); }
      }
    } else if (hand) {
      flag("texts", "handwriting engraving on the " + (mode === "both" ? "front and back" : mode) + ": the writing comes as a picture, a person handles it");
    } else if (mode === "front") {
      const own = persText.trim();
      if (!pers.length) flag(buyer || staff || msgs.length ? "texts" : "nowords", buyer || staff || msgs.length ? "front engraving chosen; the words are only in a note, not in the personalisation box: a person says exactly what is engraved" : "front engraving was chosen but no words were written");
      else if (parsed.some(p => p.strays.length) && /^\s*(?:the\s+)?(?:front|back)(?![\p{L}\p{N}_])/iu.test(own)) flag("texts", `front engraving, and the words start with “${/^\s*(?:the\s+)?(front|back)/i.exec(own)[1]}”: a person checks it is not a label`);
      else if (!plainInscription(own)) {
        const q = quotedOne(own);
        if (q && plainInscription(q) && source === "option") { eng.front = faceOut({ text: q }); single = quoted = true; }
        else flag("texts", picture ? "front engraving, but the writing comes as a picture or handwriting: a person handles it" : `front engraving, but the words are not a plain inscription (“${clip(own, 40)}”): a person writes exactly what is engraved`);
      }
      else if (own.split("\n").length > 1 || /\s[\/|]\s/.test(own)) flag("texts", "front engraving with more than one item of words (“" + clip(own.replace(/\n/g, " / "), 40) + "”): a person checks all of it goes on the front");
      else if (source === "title") flag("mode", "the listing title says front engraving but no option says which the buyer chose: a person confirms");
      else { eng.front = faceOut({ text: own }); single = true; }
    } else if (mode === "both") {
      if (!pers.length && !buyer && !staff && !msgs.length) flag("nowords", "front and back engraving was chosen but no words were written");
      else if (picture && !labelled.length) flag("texts", "front and back engraving, but the writing comes as pictures or handwriting: a person handles it");
      else if (!pers.length && !staff) flag("texts", "front and back engraving; the words are only in a note, not in the personalisation box: a person says what goes on each face");
      else flag("texts", "front and back engraving, but the words do not say which go on the front and which on the back: a person decides");
    } else if (mode === "none" && pers.length && !sideNone) flag("conflict", "the option says blank (no engraving), but the buyer wrote words: a person decides");
    else if (sideNone && sideWords && oneWay === "ask") flag("conflict", `1 side was chosen (the design's own art only), but the buyer wrote words (“${clip(sideWords.replace(/\s*\n+\s*/g, " / "), 40)}”): a person says whether they go on the back`);
    // SIDES: 2 sides on a design with its own front: the words are the BACK's (labelled "back: ..." or one plain block), no guess is needed because the front is the art. Words that are no plain inscription
    // (a request, a long note) are left to the engraving reader, as every back line is; no words at all is asked of a person.
    if (sideBack && !review) {
      const lab = pick && !pick.problems.length && pick.back && ["personalization", "staffNote"].includes(pick.k) ? pick.back : null, own = lab ? lab.text : persText.trim();
      if (!own) { if (!buyer && !staff && !msgs.length) flag("nowords", "2 sides were chosen (the design's own art on the front, the buyer's words on the back), but no words were written"); }
      else if (plainInscription(own)) { eng.back = faceOut({ text: own, own: lab && lab.own }); single = !lab; if (!lab && sideN === 2 && SIDES_POLICY.twoSidesUnlabelled === "ask") flag("texts", `2 sides were chosen (the design's own art on the front), and the words are not labelled: a person confirms they go on the back`, true); }
      else { const q = quotedOne(own); if (q && plainInscription(q)) { eng.back = faceOut({ text: q }); single = quoted = true; if (sideN === 2 && SIDES_POLICY.twoSidesUnlabelled === "ask") flag("texts", `2 sides were chosen (the design's own art on the front), and the words are not labelled: a person confirms they go on the back`, true); } }
    }
    if (mode === "none" && !review && !sideNone) return null;
    if (mode === "back" && !sideBack) { eng.front = null; eng.back = null; }   // (back only: the engraving reader reads the words as it always did)
    // ── the plain line, the confidence and what a person is asked ──
    const fw = eng.front && eng.front.fontWord, bw = eng.back && eng.back.fontWord, lineFont = !fw && !bw && spec.font && spec.font.asked ? String(spec.font.asked) : "";
    // (the message's font, when no face and no option names one: "· font Courier New → Crimson Text (from the message)")
    const msgFont = !fw && !bw && !lineFont && spec.fontText && spec.fontText.asked ? (() => { const p = pieceFont(spec, null); return ` · font ${clip(spec.fontText.asked, 30)}${p.name && spec.fontText.id && p.name.toLowerCase() !== String(spec.fontText.asked).toLowerCase() ? " → " + p.name : ""} (from the message)`; })() : "";
    const fontSay = (fw && bw && fw !== bw ? ` · fonts ${fw} / ${bw}` : fw || bw ? ` · font ${fw || bw}` : lineFont ? ` · font ${lineFont}` : msgFont) + (!fw && !bw && fontDisagree(spec) ? ` · ${fontDisagree(spec, true)}` : "");
    if (review) {
      eng.needsPerson = true; eng.reviewKind = review.kind; eng.why = review.reason; eng.raw = raw;
      eng.textSource = mode === "none" || review.kind === "nowords" || picture ? "none" : pick && !pick.problems.length ? "labelled" : "unlabelled";
      eng.confidence = review.kind === "nowords" ? 0.3 : eng.front || eng.back ? 0.5 : 0.3;
      if (!(eng.front || eng.back)) { eng.front = null; eng.back = null; }
    } else {
      eng.textSource = !(eng.front || eng.back) ? "none" : single ? "single" : "labelled";
      const sure = source === "option" ? 0.95 : source === "title" ? 0.7 : 0.6;
      if (mode === "both") { eng.why = `front and back from ${fromWhat}: front ${sayFace(eng.front)}, back ${sayFace(eng.back)}${fontSay}`; eng.confidence = sure; }
      else if (mode === "front") { eng.why = `front only from ${fromWhat}: ${sayFace(eng.front)} goes on the front${fontSay}`; eng.confidence = quoted ? Math.min(sure, 0.8) : Math.min(sure, 0.9); }
      else if (sideBack) { eng.why = clip(`${sideN} side${sideN === 1 ? "" : "s"} from ${fromWhat}: the design's own art is the front, the words go on the back${eng.back ? `: ${sayFace(eng.back)}` : " (read from the note)"}${fontSay}`, 200); eng.confidence = Math.min(art.how === "flag" ? 0.95 : 0.9, quoted ? 0.8 : 1); }
      else if (sideNone) { eng.why = sideWords ? clip(`Buyer chose 1 side; words not engraved: “${clip(sideWords.replace(/\s*\n+\s*/g, " / "), 90)}”`, 200) : clip(`1 side from ${fromWhat}: only the design's own art is engraved`, 200); eng.confidence = art.how === "flag" ? 0.95 : 0.9; }
      else { eng.why = `back only from ${fromWhat}: read as before`; eng.confidence = source === "option" ? 0.95 : 0.8; }
      if (source !== "option" && (mode === "front" || mode === "both")) eng.why = clip(`${eng.why} · the listing has no such option: a person confirms`, 200);
    }
    if (sideN) eng.sides = { n: sideN, art: art.how };   // (the side count that was read, and how the design's own front art was known: "index" counted from the library entry, "flag" a person's / the index's word)
    return eng;
  }
  engravingOf.faceParse = faceParse; engravingOf.modeIn = engravingModeIn; engravingOf.modeOfOption = engravingModeOfOption;

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
    const me = ctx.masterEntry, loose = ctx.masterLoose, txSku = upSku(line.sku);   // (txSku: the SKU as the transaction carries it, before any inventory SKU stands in for it)
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
          return !!nm && !!val && !isMetalVariation(nm, val) && !isPersonalisation(nm) && !optionLookup(ctx.optionMaps, line.listingId, nm, val) && tiesToOption(table, v, listing.sku); });
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
    // REVLINK (13 Oct 2026): the table gave this line nothing (it is not loaded, or names no product of the line) but a person's pick was saved for this very product (byVia): the line is read under the SKU
    // that pick is kept under, as the table would have given it; no Etsy call (linkedSku above)
    if (!viaInventory && !heldFamily && !(listing && listing.sku)) {
      const a1 = ctx.aliases && ctx.aliases[String(line.listingId)], y1 = a1 ? linkedSku(line, a1) : "";
      if (y1 && y1 !== upSku(line.sku)) { viaInventory = { tx: upSku(line.sku), sku: y1, by: "link" }; line = Object.assign({}, line, { sku: y1 }); }
    }
    // the SKU as bought: a charm-only listing's Huggie CHARM SET is its SKU's huggie design
    const set = huggieSet(line), bought = set ? huggieSku(line.sku, me) : upSku(line.sku);
    let { sku, source: skuSource } = resolveSku(set ? Object.assign({}, line, { sku: bought }) : line, ctx.aliases, me, ctx.noDesign, loose);
    if (viaInventory && skuSource !== "alias" && skuSource !== "generic") skuSource = "inventory";   // (CHAMMAP: a placeholder SKU Etsy's table gives the product is as generic as one on the transaction)
    // HUGGIEGUARD (stage A): what the line IS, and a person's saved answer for this listing and SKU. resolveSku trusts an exact master SKU before any saved answer, so when the exact design's form clashes
    // with the line (below) the saved answer is read here: another design wins (source "alias"), the same design is the person's word that it is right (formSaid: never held again)
    const guardCtx = { masterEntry: me, masterLoose: loose, masterShared: ctx.masterShared };
    let lf = { form: "unknown", by: "none" }, formSaid = false;
    try {
      lf = lineForm(line, { optionMaps: ctx.optionMaps });
      if (me && sku && !set && FORM_AUTO.has(skuSource)) {
        const a0 = ctx.aliases && ctx.aliases[String(line.listingId)], r0 = upSku(line.sku);
        const saved = a0 ? upSku((r0 && a0.bySku ? bySkuOf(a0.bySku, r0) : "") || (a0.sku && !a0.v && !/\(HUGGIE\)$/.test(r0) ? a0.sku : "")) : "";
        if (saved && formClash(lf, masterForm(sku, guardCtx))) { if (saved !== sku) { sku = saved; skuSource = "alias"; } else formSaid = true; }
      }
    } catch (_) { /* the guard never stops a line from being read */ }
    // the SKU of the variation bought, when it is a design's own and the inventory ties it to an option's value, is that
    // option's answer: the option is not asked, and no charm a person once picked for it stands over the SKU
    const sold = viaInventory && viaInventory.etsy || upSku(line.sku), named = !!(sku && me && me(sku)),
      // REVLINK: a line read by a person's link for its product (viaInventory.by "link") has every option the product carries decided by that link: those options are not asked, as the table would have said
      byLink = !!(viaInventory && viaInventory.by === "link" && skuSource === "alias" && sku), tied = v => (byLink && varIds({ variations: [v] }).length > 0) || (named && tiesToOption(table, v, sold));
    // an option that picks the charm (a person's answer for this listing) wins over the SKU the variations share
    // TAGSKU: a Mini Tag choice ("Charm Type: Tag3 (front + back)"): the tag number is the shape (the master's design for it), the words in the brackets the engraving. A person's saved answer, then the
    // option's own SKU (Etsy's table ties one to the value, or an indexed master family on the transaction), decide first; only a SKU the whole listing shares leaves the shape to this rule
    const tagOpt = tagOptionRead(line, ctx.optionMaps, me);
    const tagOwn = !!tagOpt && (tied(tagOpt.variation) || (!!sku && !!me && !!me(sku) && ["transaction", "spelling", "variation"].includes(skuSource) && (!!skuIndexFamily(sku, me) || seenVerdict(ctx.seen, line.listingId, tagOpt.name, tagOpt.value, txSku) === "follows")));
    const tagPick = tagOpt && !tagOwn && tagOpt.state === "found" ? { sku: tagOpt.sku, name: tagOpt.name, value: tagOpt.value, variation: tagOpt.variation } : null;
    const tagMiss = tagOpt && !tagOwn && tagOpt.state === "missing" ? tagOpt : null;
    const picked = optionDesign(line, ctx.optionMaps) || tagPick, viaPick = !!picked && !tied(picked.variation);
    if (viaPick) { sku = picked.sku; skuSource = "option"; }
    // KEYLETTER: an option whose value is ONE letter finds its design by the letter's place in the master's 26-letter family (letterRead); when the one letter option of the line is not answered
    // by a person or tied by a table, it is read here: another family member is the design (base / named / said), a clash or a missing design is one plain line (below, in the loop)
    const letters = new Map(); let viaLetter = null;
    if (!viaPick && !huggieSet(line)) {
      for (const v of line.variations || []) {
        const nm = lvName(v), val = lvValue(v).trim();
        if (!nm || !val || isMetalVariation(nm, val) || isPersonalisation(nm) || isSizeOption(nm) || isChainOption(nm) || isFormOption(nm) || isEarFormOption(nm) || isFontOption(nm) || isPriceOption(nm) || tied(v) || optionLookup(ctx.optionMaps, line.listingId, nm, val)) continue;
        const r = letterRead(nm, val, sku, me, ctx.optionMaps && ctx.optionMaps[String(line.listingId)]); if (r) letters.set(v, Object.assign(r, { name: nm, value: val }));
      }
      if (letters.size > 1) letters.clear();                                    // (two letter options may be two charms: a person says, as before)
      const only = letters.size === 1 ? [...letters.values()][0] : null;
      if (only && only.sku) { sku = only.sku; skuSource = "letter"; viaLetter = only; }
    }
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
    // ZODSIGN (section 3c): a SHARED zodiac SKU (HUGGIE HOOPS-ZODIAC, one drawing in the master, the Libra huggie, for all twelve signs) takes the sign the buyer NAMED (an option, the personalisation box, the
    // buyer's note): that sign's charm of the line's form, or a plain wait ("The Capricorn charm is not in the repo yet" / "Which zodiac sign?"), never another sign's file. Only an automatic match is replaced:
    // a person's saved answer (alias, option map, saved pair) and every other reading stand.
    let zodiac = null;
    try {
      if (!quiet && !set && !viaPick && !viaLetter && !members && me && sku && ZODIAC_AUTO.has(skuSource)) zodiac = zodiacRead(line, order, sku, lf, masterForm(sku, guardCtx), ctx);
    } catch (_) { zodiac = null; }
    if (zodiac && zodiac.pick) { sku = zodiac.pick.sku; skuSource = "zodiac"; }
    // HUGGIEGUARD (stage B): an automatic match never crosses forms (see section 3b). A person's answer (source alias / option / a saved pair) and a line that is not cut are never held.
    let formHold = null, df = { form: "unknown", by: "none" };
    try {
      if (me && sku) df = masterForm(sku, guardCtx);
      if (!quiet && me && sku && !formSaid) {
        const named = skuSource === "pair" && members && (members.source === "skus" || members.source === "title");   // (two designs named in words: each is checked)
        if (FORM_AUTO.has(skuSource) || named) for (const d of named ? members.members.map(m => m.sku) : [sku]) { if (!me(d)) continue; const f = masterForm(d, guardCtx); if (formClash(lf, f)) { formHold = { design: d, df: f }; break; } }
      }
    } catch (_) { formHold = null; }
    // LOOKALIKE (section 3b): a NECKLACE line that matched a loop-less drawing by its exact name while a differently drawn lookalike with a ring exists waits for a person. A person's answer for this listing and SKU
    // (a link, a saved pair, a listing-wide answer) silences it for good; the one-word note ("agrees") is the reading when the form fits the exact name.
    let look = null;
    try {
      if (!quiet && !set && me && sku && !viaPick && !viaLetter && !members && !formHold && !(zodiac && zodiac.hold) && LOOK_AUTO.has(skuSource) && typeof ctx.masterLike === "function") {
        const a1 = ctx.aliases && ctx.aliases[String(line.listingId)], keys = [upSku(line.sku), txSku].filter(Boolean);
        const saidWord = !!a1 && (keys.some(r => (a1.bySku && bySkuOf(a1.bySku, r) !== undefined) || (a1.pairBySku && bySkuOf(a1.pairBySku, r) !== undefined)) || (!!a1.sku && !a1.v));
        if (!saidWord) look = lookalikeRead(sku, lf, { masterEntry: me, masterLike: ctx.masterLike, guardCtx });
      }
    } catch (_) { look = null; }
    const metalKey = String(line.metalKey || "");
    const material = METAL_TO_CARD[metalKey] || null;
    if (!quiet && !material) problems.push({ kind: "needsMaterial", metalKey: metalKey || null, metalLabel: line.metalLabel || "", listingId: String(line.listingId || ""), options: (line.variations || []).map(v => `${v.name}: ${v.value}`), title: line.title || "" });
    const spec = { designSku: sku || null, skuSource, material, materialKey: metalKey || null, materialLabel: line.metalLabel || (material ? CARD_LABEL[material] : ""), form: null, size: null, chain: null, quantity: Math.max(1, Math.round(+line.quantity || 1)), personalization: (line.personalization || []).map(s => visible(s).trim()).filter(Boolean), buyerMessage: visible(line.buyerMessage || order.buyerMessage || ""), staffNote: String(line.staffNote || order.staffNote || ""), messages: (line.messages || order.messages || []).slice(-5), updateTs: +order.updateTs || 0, options: [], problems, noDesign, sources: { material: "station classifier (Metal/Colour option first)", sku: skuSource } };
    if (special) spec.special = special;
    if (done) spec.customDone = done;
    spec.boughtSku = bought; if (set) spec.huggieSet = true;
    if (viaInventory) spec.viaInventory = viaInventory;   // the transaction's SKU and the listing inventory's SKU for the product bought, when that one was used
    if (viaLetter) spec.viaLetter = { letter: viaLetter.letter, sku: viaLetter.sku, how: viaLetter.how, name: viaLetter.name, value: viaLetter.value, from: txSku };   // KEYLETTER: the letter option's design is that letter's member of its family
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
    // FLAGWHY: with no table loaded, the orders the page holds can still show the SKU is the listing's one SKU (it is on other choices of this option): said in the row
    const sharedWhy = (name, value) => !table && txSku && seenVerdict(ctx.seen, line.listingId, name, value, txSku) === "shared" ? { why: `SKU ${txSku} is on orders with other choices of “${name}” too, so it does not say which charm this is: map this option once` } : {};
    // KEYLETTER: a letter option with no design says so in ONE plain line (never "not mapped, Etsy's SKU list is not loaded"), and a clash offers the letter's own member first
    const letterAsk = v => {
      const r = letters.get(v); if (!r || r.sku) return {};
      const h = optionHelp().why, why = h && /not in any master file/.test(h) && !/\(/.test(r.why) ? `${r.why} (${h})` : r.why;   // (Etsy's own list, when it names a SKU the master lacks: "Etsy's SKU for this option, X, is not in any master file"; its other answers add nothing to this line)
      return Object.assign({ letter: { letter: r.letter, why, clash: !!r.clash }, why }, r.clash && r.clash.want ? { picks: [{ sku: r.clash.want, why: `the letter ${r.letter} of the same family` }] } : {});
    };
    // a listing that engraves in ONE font whatever the buyer picks (FONT_LISTINGS): its font option never holds, asks or changes the font
    const fx = fontListing(line, sku, bought);
    for (const v of line.variations || []) {
      const name = v.name || v.formatted_name, value = String(v.value != null ? v.value : v.formatted_value || "").replace(/&quot;/g, "\"").trim();
      if (name && value && !spec.font) { const fm = fontInMetal(name, value, fx); if (fm) spec.font = fontSpecOf(fm, fm.listing ? "rule:font-listing" : "rule:font-in-metal"); }   // ("Metal? Font Choice?": the metal is read as before, the font from the same value)
      if (!name || !value || isMetalVariation(name, value) || isPersonalisation(name)) continue;   // (BFWHY: a value that is only a metal / length / sides is the metal option whatever it is called)
      const hit = optionLookup(ctx.optionMaps, line.listingId, name, value);
      let mapped = hit ? Object.assign({ field: hit.field, value: hit.value, source: hit.source }, hit.engrave ? { engrave: hit.engrave } : {}) : null;
      // a font option (Font: Stylish, Fonts: 16"/ Typewriter): read once; a person's answer for the FONT alone ("typewriter") serves every length it comes with
      const fr = fontRead(name, value, fx), fh = fr ? null : fontHidden(name, value, fx);
      if (!mapped && fr && fr.asked !== value) { const h = optionLookup(ctx.optionMaps, line.listingId, name, fr.asked); if (h) mapped = { field: h.field, value: h.value, source: h.source }; }
      // the drop-down value that says "2 symbols" and leaves the signs to the note ("2 symbols-leave note"): once the note's two signs are two charms, the option is answered by them
      if (!mapped && members && members.source === "signs" && members.byNote && name === members.optionName && value === members.optionValue) mapped = { field: "design", value: members.members[0].sku, source: "rule:signs-in-note" };
      // ZODSIGN: the option that named the sign of a shared zodiac SKU is answered by that sign's charm (a sign with no charm yet leaves the option as it was, beside the line's plain wait)
      if (!mapped && zodiac && zodiac.pick && zodiac.variation === v) mapped = { field: "design", value: zodiac.pick.sku, source: "rule:zodiac-sign" };
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
        // a Mini Tag choice: the tag number is the shape, the master's design of that shape is the charm (tagOptionRead), the bracket's words the engraving (spec.tagOption)
        else if (tagPick && v === tagPick.variation) mapped = { field: "design", value: tagPick.sku, source: "rule:tag-shape" };
        // a font option names an app font exactly: it answers itself (never a look-alike); any other font waits below with its one plain line, unless FONT_RULES lets it through
        else if (fr && fr.none) mapped = { field: "font", value: null, source: "rule:no-font" };
        else if (fr && fr.id) mapped = { field: "font", value: fr.id, source: fr.listing ? "rule:font-listing" : "rule:font" };
        else if (fr && !FONT_RULES.unknownHolds) mapped = { field: "font", value: null, source: "rule:font-asked" };
        // an option that names a number of separate pieces answers itself ("ROSEGOLD - 2 Disc"); one that may, but does not say of what
        // (letters, a range, "Set of 3"), is asked about below, once, in its own plain words, not as a generic unmapped option
        const cx = cr.opts.find(c => c.name === name && c.value === value);
        if (!mapped && cx && cx.ask) { spec.options.push({ name, value, mapped: null }); continue; }
        if (!mapped && cx && cx.certain && cx.n > 1 && cx.cls !== "design") mapped = { field: "count", value: String(cx.n), source: "rule:" + cx.rule };
        // "GOLD - 1 Disc" (one of the 15 choices of the 180-product disc listing) answers itself as ONE piece too: it used to fall to the generic "not mapped" hold (DISCREAD)
        else if (!mapped && cx && cx.certain && cx.n === 1 && cx.cls === "piece") mapped = { field: "count", value: "1", source: "rule:" + cx.rule };
      }
      // a font hidden in a length option ("16"·STYLISH·Center"): the line's font too; the option itself reads as the length it always did
      if (fh && !spec.font) spec.font = fontSpecOf(fh, fh.listing ? "rule:font-listing" : "rule:font-in-length");
      // KEYLETTER: the design the letter's place in its family gives (a member other than the line's own SKU, which letterRead found above)
      if (!mapped && viaLetter && letters.get(v) === viaLetter) mapped = { field: "design", value: sku, source: "rule:letter-" + viaLetter.how };
      spec.options.push({ name, value, mapped });
      // (an unmapped font option is asked about the FONT: the question and its answer carry the font alone, the length and alignment are read apart)
      if (!mapped) { if (!quiet) problems.push(fr ? { kind: "needsMapping", listingId: String(line.listingId || ""), optionName: name, optionValue: fr.asked, title: line.title || "", font: { asked: fr.asked, chain: fr.chain, align: fr.align, raw: value, pick: fr.pick, why: fontWhy(fr) } } : Object.assign({ kind: "needsMapping", listingId: String(line.listingId || ""), optionName: name, optionValue: value, title: line.title || "" }, tagMiss && v === tagMiss.variation ? { why: tagMiss.why } : Object.assign({}, optionHelp(), sharedWhy(name, value), letterAsk(v)))); continue; }   // (TAGSKU: a tag shape with no master design says which one, in one plain line, not Etsy's table)
      if (mapped.field === "font") {   // the font the buyer asked for and the app font it is (none: asked only); the length it came with is the line's chain
        const af = fontById(mapped.value);
        if (!spec.font) spec.font = fr ? fontSpecOf(Object.assign({}, fr, { id: af ? af.id : "", name: af ? af.name : "" }), mapped.source) : { asked: value, id: af ? af.id : "", name: af ? af.name : "", source: mapped.source };
        if (fr && fr.chain && !spec.chain) spec.chain = fr.chain;
        continue;
      }
      if (mapped.field === "ignore" || mapped.field === "design" || mapped.field === "count") continue;
      if (mapped.field === "form" && !spec.form) spec.form = mapped.value;
      else if (mapped.field === "size" && !spec.size) spec.size = mapped.value;
      else if (mapped.field === "chain" && !spec.chain) spec.chain = mapped.value;
    }
    // FLAGWHY: the transaction's SKU is a master design that is demonstrably THIS option's own (skuIndexFamily, or the page's own orders: seenVerdict; never when the page's
    // orders or the listing's table say it is shared): the one option nothing else could read is answered by it, as a table's tied SKU answers it. No hold, no person asked.
    if (!quiet && !viaPick && !viaInventory && !heldFamily && !set && sku && txSku && me && me(sku) && ["transaction", "spelling", "variation"].includes(skuSource) && !skuFamilyConflict(sku, line) && !tableShares(table, txSku)) {
      const unread = spec.options.filter(o => !o.mapped), ask = unread.length === 1 ? problems.find(p => p.kind === "needsMapping" && !p.font && p.optionName === unread[0].name && p.optionValue === unread[0].value) : null;
      // (a yes / no / none answer is never a choice of charm: "Gift Wrap: Yes" on a numbered-SKU line is still an option nobody has read)
      if (ask && !(ask.letter && ask.letter.clash) && !/^\s*(?:yes|no|none|n\/a|true|false)\b/i.test(unread[0].value)) {
        const seen = seenVerdict(ctx.seen, line.listingId, unread[0].name, unread[0].value, txSku), how = seen === "shared" ? "" : seen === "follows" ? "seen" : skuIndexFamily(sku, me) ? "index" : "";
        if (how) { unread[0].mapped = { field: "design", value: sku, source: "sku:" + how }; problems.splice(problems.indexOf(ask), 1); }
      }
    }
    // the listing's usual font (FONT_LISTINGS), ONLY for a line whose buyer named no font the shop offers (no font option, "No Font", a word the shop does not offer): the buyer's own font word is never replaced
    if (fx && !(spec.font && spec.font.id)) { const f = fontById(fx.font); spec.font = Object.assign({ asked: "" }, spec.font, { id: f.id, name: f.name, source: "rule:font-listing", listing: true }); }
    // an unknown SKU waits while an option is unanswered: the option may be what picks the charm (Zodiac Sign: Pisces on a
    // listing whose signs share one SKU), and a charm given to the SKU instead would be every sign's
    const optionOpen = problems.some(p => p.kind === "needsMapping");
    if (!quiet && !sku && !optionOpen) problems.push(set ? { kind: "unmatchedSku", reason: "no SKU on the transaction · Huggie CHARM SET: pick its huggie design", huggie: true, listingId: String(line.listingId || ""), title: line.title || "" }
      : { kind: "unmatchedSku", reason: "no SKU on the transaction and no alias for the listing", listingId: String(line.listingId || ""), title: line.title || "" });
    if (ctx.masterEntry && sku && !quiet) {
      const entry = ctx.masterEntry(sku);
      if (!entry) {
        // HUGGIEGUARD: the numbered siblings of the SKU the master does hold ("X 1", "X 2" for an "X" it has not), the ones that fit the line: for whoever offers them (spec.siblings)
        let sib = []; try { sib = siblingsOf(sku, guardCtx, { form: lf.form }); } catch (_) { sib = []; }
        if (sib.length) spec.siblings = sib;
        if (!optionOpen) problems.push(Object.assign({ kind: "unmatchedSku", reason: set && skuSource === "transaction" ? "Huggie CHARM SET: its huggie design is not in any master file" : "not in any master file", sku, listingId: String(line.listingId || ""), title: line.title || "" }, sib.length ? { siblings: sib } : {}));
      }
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
    // ZODSIGN: the sign the buyer chose, said once in the order's reading (the Pieces line) and kept as a fact; the sign word in the box is the buyer's choice of sign, never words to engrave (textOnly: nothing else was written)
    if (zodiac && zodiac.sign) {
      spec.zodiacSign = { sign: zodiac.sign, from: zodiac.from, word: zodiac.word, typo: !!zodiac.typo, text: zodiac.text, textOnly: !!zodiac.textOnly, family: zodiac.family, design: zodiac.pick ? zodiac.pick.sku : null, how: zodiac.pick ? zodiac.pick.how : "", held: zodiac.hold ? zodiac.hold.kind : "", note: zodiac.note };
      spec.pair.notes.push(zodiac.note);
    }
    // LOOKALIKE: the form fits the exact name and a lookalike of the other kind exists (a necklace line on the ringed entry, a pair of earrings on the loop-less one): said once in the reading, nothing waits
    if (look && look.state === "agrees") { spec.lookalike = { state: "agrees", design: look.design, others: look.others }; spec.pair.notes.push(look.note); }
    /* SCHROLINK (Paul, 12 Oct 2026): a pair of designs a person saved for this listing and SKU whose designs are NOT both in the master any more (renamed, deleted, blocked out of the index) must never let
       a necklace fall back, in silence, to one piece of the SKU's own design (that was the mistake: the huggie skull cat for a necklace of two cats). On a line that is not a single earring it waits for a
       person, with the reason (an earring line that says two different designs keeps its own question, asked above). It ends by itself when both designs are in the master again. */
    if (!quiet && report.link && report.link.miss.length && !members && !spec.pair.single && !spec.pair.second)
      problems.push({ kind: "unmatchedSku", reason: `a person saved this listing and SKU as two charms, ${report.link.L} and ${report.link.R}, but ${report.link.miss.join(" and ")} ${report.link.miss.length > 1 ? "are" : "is"} not in any master file: the line waits (it is never made as one piece of ${sku || "its SKU"}); put the design back in the master or save the link again`, sku: sku || upSku(line.sku), listingId: String(line.listingId || ""), title: line.title || "", savedPair: true });
    // a count the options cannot settle waits for a person: one plain question per option, like an unmatched SKU
    if (!quiet) for (const a of spec.pair.asks) if (!problems.some(p => p.kind === "needsMapping" && p.optionName === a.name && p.optionValue === a.value && (p.count || p.earWords))) problems.push(Object.assign({ kind: "needsMapping", listingId: String(line.listingId || ""), optionName: a.name, optionValue: a.value, title: line.title || "" }, a.rule === "ear:words" ? { earWords: { why: a.why } } : { count: { guess: a.guess || 0, why: a.why, rule: a.rule } }));
    // a person said this listing's words are a pair of earrings: the form it is (what an answered option does too)
    if (!spec.form && spec.pair.earForm) spec.form = spec.pair.earForm;
    // TAGSKU: the Mini Tag choice the line carries, however its charm was settled (how: a person's answer, the option's own SKU, this rule's tag number, or nothing yet), with the engraving it names:
    // mode "front" (front face only), "both" (front and back faces) or "back"; the listing sells the tag as a charm to add to a chain, so with no other word on the form it is a loose charm (never a pair, never mirrored)
    if (tagOpt) {
      // the option's entry says the engraving too (CONTRACT section 13, "engrave"): whoever settled the charm (a person's answer, Etsy's own SKU, an indexed family SKU, this rule)
      const to = spec.options.find(x => x.name === tagOpt.name && x.value === tagOpt.value);
      if (to && to.mapped && to.mapped.field === "design") to.mapped = Object.assign({}, to.mapped, { engrave: tagOpt.mode });
      spec.tagOption = { name: tagOpt.name, value: tagOpt.value, tag: tagOpt.tag, mode: tagOpt.mode, shape: tagOpt.shape, sku: tagOpt.state === "found" ? tagOpt.sku : "", how: tagOpt.state === "answered" ? "answer" : tagOwn ? "sku" : tagOpt.state === "found" ? "rule:tag-shape" : tagOpt.state };
      if (!spec.form) spec.form = "charm";
      if (tagMiss) { spec.designSku = null; spec.skuSource = null; spec.sources.sku = null; }   // (the shape has no design: no vector of a look-alike on the row)
    }
    // a line that says two different designs but names one waits: the second design, or "the same on both ears" (one plain question, like an unmatched SKU)
    if (!quiet && spec.pair.second && !spec.pair.second.answered && !spec.pair.second.viaOption && secondId) problems.push({ kind: "needsMapping", listingId: String(line.listingId || ""), optionName: SECOND_OPT, optionValue: secondId, title: line.title || "", pair: { second: true }, pairSecond: { why: spec.pair.second.why } });
    // ZODNOBACK (section 3d): NO EARRING IS EVER ENGRAVED. The form guard's verdict (lf: the line's form, df: its design's) decides, once, here; every later reader asks spec.engraveNever. The buyer's words stay in
    // spec.personalization (a zodiac sign typed in the box is the buyer's choice of sign, never words for the back) and are never a question.
    let never = null; try { never = earringNeverBackEngraves({ lineForm: lf, designForm: df, form: spec.form, pair: spec.pair, line }); } catch (_) { never = null; }
    if (never) spec.engraveNever = Object.assign({}, never, { note: (spec.personalization || []).map(x => visible(x).trim()).filter(Boolean).slice(0, 4).map(x => x.slice(0, 120)) });
    spec.engraveCandidate = !quiet && !never && (spec.personalization.length > 0 || !!spec.buyerMessage.trim() || !!spec.staffNote.trim() || spec.messages.some(m => engravingNote(m && m.text)));
    // BFFONT: a font the buyer named in their own words (personalisation, message, staff note), whatever the listing: spec.fontText (fontTextOf). A font option is read as before (spec.font) and wins over
    // it; nothing is added to a line whose words name no font. Before the faces reading (engravingOf), which takes the words of a font request out of the engraved words.
    try {
      const ft = fontTextOf(spec);
      if (ft) { if (spec.font && spec.font.id && !spec.font.listing && ft.id && ft.id !== spec.font.id) ft.differs = spec.font.asked || spec.font.name || ""; spec.fontText = ft; }
    } catch (_) { /* a font reading that fails leaves the line as it was */ }
    // front and back engraving (ENGINTAKE): what the buyer chose and wrote about the faces, for EVERY line; null (nothing added to the spec) when the line has no front / back word.
    // ENGINTAKE (CONTRACT section 2): what the buyer chose for the faces, for EVERY line. A line with no face word has no `spec.engraving` (the faces model reads that as the default back); a person's
    // word is never here (it is row.engravingOverride). Nothing else of the spec is touched, engraveCandidate included: a front / both-faces line with no words says so in its reading
    // (needsPerson, reviewKind "nowords") and the engraving pass (the faces model) raises it; here it is still cut plain exactly as before.
    try {
      const me2 = spec.designSku && me ? me(spec.designSku) : null;   // (the design's own front art, when its library entry says so: "1 side / 2 sides" options read by it)
      const eng = engravingOf(line, spec, frontArtOf(me2, line.title));
      if (eng) spec.engraving = eng;
    } catch (_) { /* a reading that fails leaves the line as it always was */ }
    // HUGGIEGUARD: the line's form and the design's form are always told; a clash holds the line with ONE plain line and the siblings that fit (spec.formHold), and no design is on the row
    spec.lineForm = lf.form; spec.designForm = df.soft ? "unknown" : df.form;   // (a necklace read only from a distinct huggie twin is not told: it only keeps a huggie line off the plain design)
    if (formHold) {
      const etsy = upSku(line.sku) || spec.boughtSku, sibs = siblingsOf(etsy, guardCtx, { form: lf.form }), reason = formReason(etsy, formHold.design, formHold.df, lf);
      spec.formHold = { sku: spec.boughtSku, design: formHold.design, lineForm: lf.form, designForm: formHold.df.form, reason, siblings: sibs };
      spec.siblings = sibs; spec.designSku = null; spec.skuSource = null; spec.sources.sku = null;
      problems.unshift({ kind: "unmatchedSku", reason, sku: spec.boughtSku, listingId: String(line.listingId || ""), title: line.title || "", formHold: true, lineForm: lf.form, designForm: formHold.df.form, siblings: sibs });
    } else if (zodiac && zodiac.hold) {
      // ZODSIGN: a sign the repo has no charm of (yet), several signs, or none: the line waits with ONE plain line and no design on the row (the held line is made by no pool: the form guard's own hold, makePool).
      // The decision is per sign (problem.zodiac), so each missing sign is its own card; the line is read again whenever the library grows, so the hold clears the day the sign's master exists.
      const reason = zodiac.hold.reason;
      spec.formHold = { sku: spec.boughtSku, design: "", lineForm: lf.form, designForm: df.form, reason, siblings: [], zodiac: zodiac.hold.kind, sign: zodiac.sign || "" };
      spec.designSku = null; spec.skuSource = null; spec.sources.sku = null;
      problems.unshift({ kind: "unmatchedSku", reason, sku: spec.boughtSku, listingId: String(line.listingId || ""), title: line.title || "", formHold: true, zodiac: zodiac.hold.kind + ":" + (zodiac.sign || ""), lineForm: lf.form, designForm: df.form, siblings: [] });
    } else if (look && look.state === "held") {
      // LOOKALIKE: the exact name is a drawing with no hanging hole, the listing is a necklace and a differently drawn lookalike with a ring exists: no design is on the row and nothing is pooled (the form guard's
      // own hold, makePool); ONE plain line names the drawings and the Review row offers each as a tap (problem.candidates). The decision is per LISTING and SKU (problem.lookalike): the tap is that listing's saved link.
      spec.formHold = { sku: spec.boughtSku, design: look.design, lineForm: lf.form, designForm: df.form, reason: look.reason, siblings: [], lookalike: true, candidates: look.candidates };
      spec.lookalike = { state: "held", design: look.design, others: look.others }; spec.siblings = [];
      spec.designSku = null; spec.skuSource = null; spec.sources.sku = null;
      problems.unshift({ kind: "unmatchedSku", reason: look.reason, sku: spec.boughtSku, listingId: String(line.listingId || ""), title: line.title || "", formHold: true, lookalike: String(line.listingId || "?"), candidates: look.candidates, lineForm: lf.form, designForm: df.form, siblings: [] });
    } else if (skuSource === "generic" && !quiet && me && spec.designSku) {
      // CHAMMAP: the SKU is the shop's shared charm-only placeholder (genericSku) and this listing has no saved answer for it: the master label of that name is on some other design (the astronaut), so no design is on
      // the row and nothing is pooled; the line waits for a person with ONE plain line (the form guard's own hold, makePool). The decision is per LISTING (problem.generic): one listing's answer is never another's.
      const etsy = upSku(line.sku) || spec.boughtSku, reason = `SKU “${etsy}” is a generic charm-only SKU the shop uses on many listings, so it does not say which charm this listing is: say which charm it is`;
      spec.formHold = { sku: spec.boughtSku, design: "", lineForm: lf.form, designForm: df.form, reason, siblings: [], generic: true };
      spec.designSku = null; spec.skuSource = null; spec.sources.sku = null;
      problems.unshift({ kind: "unmatchedSku", reason, sku: spec.boughtSku, listingId: String(line.listingId || ""), title: line.title || "", formHold: true, generic: String(line.listingId || "?"), lineForm: lf.form, designForm: df.form, siblings: [] });
    }
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
  return { questionGone, staleQuestionHold, orderQuery, orderMatches, orderGroups, SPECIAL, specialOf, engravingNote, sortingLabel, sortingMetal, visible, purchaseDetails, purchaseOptions, libraryGroup, METAL_TO_CARD, CARD_TO_METAL, CARD_TAG, CARD_LABEL, DEFAULT_OPTION_MAP, FORM_VALUES, SIZE_VALUES, norm, decodeEntities, cleanLine, cleanOrders, cleanMaps, bySkuOf, optionLookup, isNoDesign, resolveSku, variationBase, genericSku, optionDesign, huggieSku, looseKey, masterSku, KNOWN_SKU_TYPOS, knownTypo, typoSku, inventorySku, tiesToOption, listingWide, linkViaKeys, linkedSku, skuIndexFamily, seenSkus, seenVerdict, metalSpecValue, interpretLine, lineKey, poolId, PIECE_RULES, pieceCountOf, countedPieces, countedFact, piecesOf, sidesOf, kindFor, glue, solo, pinPieces, countRead, optionCount, noteCountOf, singleSideOf, lineSignals, lineMismatched, splitSkus, setDesignsOf, pairMembers, signsOfText, signsOfLine, signValueSku, pairInfo, discsIn,
    engravingOf, engravingModeIn, engravingModeOfOption, faceParse, sidesOfValue, frontArtOf, SIDES_POLICY, earringNeverBackEngraves, EAR_NEVER_WHY,
    ENGRAVING_FONTS, FONT_WORDS, FONT_STEMS, FONT_LISTINGS, DEFAULT_FONT, FONT_RULES, fontById, fontRead, fontHidden, fontInMetal, fontParts, isFontOption, fontOfWord, fontOfPhrase, fontListing, pieceFont, faceFontWord, fontCaps, capsText, fontLine, fontOfRecord, FONT_STYLES, fontDisagree, fontResolve, fontReadText, fontScanLine, fontTextOf, nearestFont, fontStyleLabel, damerau,
    orderPlacedAt, frontOf, byQueue, rankDate, orderDay, intakePlan, completionDay, completionTime, compareCompleted, completedTitle, localDay, dateTag, dateTagOfDay, setId, setLabel, setFolder, sheetName, sheetFolder, toB36, encodeOrderList, safeChunks, evaluateOrder, planRelease, sheetRelease, kinGroups, FAST_MATERIALS, SLOW_MATERIALS, RUN_STEPS, HALF, nextStep, stepIndex, DONE_STATES,
    skuFamily, lineFamily, skuFamilyConflict, familyTwins, listingTwin, inventoryPicks, inventoryWhy, suggestCharms, letterIndex, letterFamily, letterRead, oneChar, tagOptionOf, tagOptionRead, TAG_SHAPES,
    KNOWN_SKU_NAMES, knownName, namedSku, sameDesign, designFile, FORMS, formOfText, masterForm, lineForm, formClash, siblingsOf, formOfLine, lookalikeRead,
    SIGN_ORDER, skuSign, sharedZodiac, signsInText, signsNamed, zodiacRead,
    RUN_RECORD, FINISHED_LINE, closedOrders, utf8Bytes, textHash, indexEntries, archiveParts };
});
