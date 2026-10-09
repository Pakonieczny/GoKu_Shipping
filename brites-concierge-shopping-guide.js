(function (root, factory) {
  'use strict';
  var api = factory(typeof module === 'object' && module.exports ? require('./brites-catalogue-intents.js') : root.BritesCatalogueIntents);
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.BritesConciergeShoppingGuide = api;
})(typeof window !== 'undefined' ? window : globalThis, function (defaultVocabulary) {
  'use strict';

  // Preparing help is local and read-only. The host owns rendering, fresh
  // storefront checks, current-request authority and every executed action.
  var PRODUCT = /^gid:\/\/shopify\/Product\/[1-9][0-9]{0,19}$/;
  var VARIANT = /^gid:\/\/shopify\/ProductVariant\/[1-9][0-9]{0,19}$/;
  var HANDLE = /^[a-z0-9]+(?:[-_][a-z0-9]+)*$/;
  var KINDS = ['alternatives', 'matching', 'options', 'product', 'bag'];
  var METAL_SOURCE = { title: 'FTC: Precious-metal jewelry', url: 'https://consumer.ftc.gov/articles/buying-platinum-gold-and-silver-jewelry' };
  var SILVER_SOURCE = { title: 'Canadian Conservation Institute: Silver tarnish', url: 'https://www.canada.ca/en/conservation-institute/services/preventive-conservation/guidelines-collections/metal-objects/understanding-silver-tarnish.html' };
  var CATEGORY_LABELS = { necklace: 'necklace', earrings: 'earrings', bracelet: 'bracelet', ring: 'ring', charm: 'charm' };
  var MOTIFS = [
    ['butterfly', 'animal', 'butterfly butterflies'], ['bunny', 'animal', 'bunny bunnies rabbit rabbits'],
    ['cat', 'animal', 'cat cats kitten kittens'], ['dog', 'animal', 'dog dogs puppy puppies'],
    ['bird', 'animal', 'bird birds'], ['owl', 'animal', 'owl owls'], ['hummingbird', 'animal', 'hummingbird hummingbirds'],
    ['bear', 'animal', 'bear bears'], ['bee', 'animal', 'bee bees'], ['dolphin', 'animal', 'dolphin dolphins'],
    ['elephant', 'animal', 'elephant elephants'], ['fox', 'animal', 'fox foxes'], ['frog', 'animal', 'frog frogs'],
    ['horse', 'animal', 'horse horses'], ['lion', 'animal', 'lion lions'], ['panda', 'animal', 'panda pandas'],
    ['penguin', 'animal', 'penguin penguins'], ['turtle', 'animal', 'turtle turtles tortoise tortoises'],
    ['wolf', 'animal', 'wolf wolves'], ['whale', 'animal', 'whale whales'], ['fish', 'animal', 'fish fishes'],
    ['dragonfly', 'animal', 'dragonfly dragonflies'], ['ladybug', 'animal', 'ladybug ladybugs ladybird ladybirds'],
    ['moth', 'animal', 'moth moths'], ['snake', 'animal', 'snake snakes'], ['spider', 'animal', 'spider spiders'],
    ['deer', 'animal', 'deer'], ['giraffe', 'animal', 'giraffe giraffes'], ['shark', 'animal', 'shark sharks'],
    ['dinosaur', 'animal', 'dinosaur dinosaurs'], ['unicorn', 'fantasy', 'unicorn unicorns'],
    ['dragon', 'fantasy', 'dragon dragons'], ['phoenix', 'fantasy', 'phoenix'],
    ['flower', 'botanical', 'flower flowers floral'], ['rose', 'botanical', 'rose roses'],
    ['sunflower', 'botanical', 'sunflower sunflowers'], ['daisy', 'botanical', 'daisy daisies'],
    ['leaf', 'botanical', 'leaf leaves'], ['tree', 'botanical', 'tree trees'], ['lotus', 'botanical', 'lotus'],
    ['moon', 'celestial', 'moon moons lunar'], ['star', 'celestial', 'star stars'],
    ['sun', 'celestial', 'sun suns sunburst'], ['planet', 'celestial', 'planet planets saturn'],
    ['heart', 'shape', 'heart hearts'], ['circle', 'shape', 'circle circles disc discs disk disks'],
    ['triangle', 'shape', 'triangle triangles'], ['diamond', 'shape', 'diamond diamonds'],
    ['cross', 'symbol', 'cross crosses'], ['anchor', 'symbol', 'anchor anchors'],
    ['music', 'symbol', 'music musical treble'], ['infinity', 'symbol', 'infinity'],
    ['snowflake', 'seasonal', 'snowflake snowflakes'], ['pumpkin', 'seasonal', 'pumpkin pumpkins']
  ];
  var motifWords = new Map();
  MOTIFS.forEach(function (entry) { entry[2].split(' ').forEach(function (word) { motifWords.set(word, entry); }); });
  var STOP = new Set(('the a an and with of for in on sterling silver gold filled solid rose yellow white plated 14k 10k 18k 14 20 necklace necklaces pendant pendants earring earrings stud studs hoop hoops huggie huggies bracelet bracelets ring rings charm charms jewelry jewellery inch inches mm cm regular beady handmade small large tiny mini synthetic').split(' '));

  function text(value, max) {
    if (typeof value !== 'string' || value.length > max || /[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f-\u009f\u202a-\u202e\u2066-\u2069]/.test(value)) return '';
    return value.trim();
  }
  function key(value) { return text(value, 16000).normalize('NFKC').toLowerCase().replace(/[’']/g, '').replace(/[^a-z0-9]+/g, ' ').trim(); }
  function words(value) { return key(value).split(' ').filter(Boolean); }
  function unique(values) { return Array.from(new Set(values)); }
  function visibleDescription(value) { return value.replace(/<!--[^]*?-->/g, ' ').replace(/<(script|style|template|noscript)\b[^>]*>[^]*?<\/\1\s*>/gi, ' ').replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim(); }
  function freeze(value) { if (value && typeof value === 'object' && !Object.isFrozen(value)) { Object.keys(value).forEach(function (name) { freeze(value[name]); }); Object.freeze(value); } return value; }
  function nativeMoney(price, currency) { try { return new Intl.NumberFormat('en', { style: 'currency', currency: currency }).format(price) + ' ' + currency; } catch (e) { return price.toFixed(2) + ' ' + currency; } }
  function category(value) {
    var v = key(value);
    if (/^(?:necklaces?|pendants?)$/.test(v)) return 'necklace';
    if (/^(?:earrings?|(?:studs?|hoops?|huggies?)(?: earrings?)?)$/.test(v)) return 'earrings';
    if (/^bracelets?$/.test(v)) return 'bracelet';
    if (/^rings?$/.test(v)) return 'ring';
    if (/^(?:charms?|charm only)$/.test(v)) return 'charm';
    return '';
  }
  function ordinaryCharm(p) {
    return /^charms?(?:[ -]only)?$/i.test(String(p && p.type || '').trim()) && !/\b(?:custom|personalized|personalised|engraving|design fee|chain extender|components?|add[ -]?ons?)\b/i.test(String(p && p.title || '') + ' ' + String(p && p.type || ''));
  }
  function restrictedParts(p) { return p.partsOnly && !ordinaryCharm(p); }
  function productCategory(p) {
    var explicit = category(p.type);
    if (explicit && explicit !== 'charm') return explicit;
    var title = key(p.title), matches = [];
    if (explicit === 'charm' && /\b(?:necklace|pendant) charms?\b/.test(title) && !/\b(?:earrings?|studs?|huggies?|bracelets?|rings?)\b/.test(title)) return 'charm';
    [['earrings', /\b(?:earrings?|studs?|huggies?)\b/], ['necklace', /\bnecklaces?\b/], ['bracelet', /\bbracelets?\b/], ['ring', /\brings?\b/]].forEach(function (entry) { if (entry[1].test(title)) matches.push(entry[0]); });
    if (matches.length === 1) return matches[0];
    return matches.length ? '' : explicit || (/\bcharms?\b/.test(title) ? 'charm' : '');
  }
  function ownURL(value, handle) {
    try { var u = new URL(value); return u.protocol === 'https:' && !u.username && !u.password && !u.port && !u.search && !u.hash && ['britesjewelry.com', 'www.britesjewelry.com'].includes(u.hostname) && new RegExp('^/(?:[a-zA-Z]{2}(?:-[a-zA-Z]{2})?/)?products/' + handle + '/?$').test(u.pathname) ? u.href : ''; } catch (e) { return ''; }
  }
  function imageURL(value) { try { var u = new URL(value); return u.protocol === 'https:' && !u.username && !u.password && !u.port && ['cdn.shopify.com', 'britesjewelry.com', 'www.britesjewelry.com'].includes(u.hostname) ? u.href : ''; } catch (e) { return ''; } }
  function motifEvidence(p, vocabulary, knownMotifs) {
    // Descriptions often mention cross-sells. Only actual identity fields and
    // literal or explicitly named motif tags establish a design relationship.
    var tags = (Array.isArray(p.tags) ? p.tags : []).slice(0, 100).flatMap(function (tag) {
      var clean = text(tag, 120), match = clean.match(/^(?:motif|symbol|theme|style)\s*:\s*([a-z0-9 -]+)$/i);
      return match ? [match[1]] : /^[a-z0-9-]+$/i.test(clean) ? [clean] : [];
    });
    var tokenize = vocabulary && typeof vocabulary.words === 'function' ? vocabulary.words : words;
    var finishFree = function (value) { return value.replace(/\brose[ -]+gold\b/gi, 'gold'); };
    // The current title and explicit tags outrank an obsolete design in a
    // legacy handle. A generic title can still use its canonical handle.
    var primary = tokenize(finishFree(p.title + ' ' + tags.join(' ')));
    var named = primary.some(function (word) { return knownMotifs.has(word) || motifWords.has(word); });
    var identity = primary.concat(named ? [] : tokenize(finishFree(p.handle))), motifs = [], groups = [];
    identity.forEach(function (word) { var m = knownMotifs.get(word) || motifWords.get(word); if (m) { motifs.push(m[0]); groups.push(m[1]); } });
    return { motifs: unique(motifs), groups: unique(groups), tokens: unique(identity.filter(function (w) { return w.length > 2 && !STOP.has(w) && !/^\d+$/.test(w); })) };
  }
  function material(value) {
    var v = key(value), rose = /\brose\b/.test(v);
    if (/\b(?:gold filled|gf)\b/.test(v) || /\b14 20\b/.test(v)) return rose ? 'rose gold filled' : 'gold filled';
    if (/\b(?:plated|vermeil)\b/.test(v) && /\bgold\b/.test(v)) return rose ? 'rose gold plated' : 'gold plated';
    if (/\b(?:silver|sterling)\b/.test(v)) return 'silver';
    if (/\bsolid\b/.test(v) && /\bgold\b/.test(v) || /\b(?:10|14|18|22|24)(?:k|kt|karat)\b/.test(v) && /\bgold\b/.test(v)) return rose ? 'rose solid gold' : 'solid gold';
    if (/\bgold\b/.test(v)) return rose ? 'rose gold' : 'gold';
    return '';
  }
  function variantMaterial(v) {
    var options = v.options.filter(function (option) { return /\b(?:metal|material|finish|color|colour)\b/.test(key(option.name)); });
    return material(options.length ? options.map(function (option) { return option.value; }).join(' ') : v.title);
  }
  function materialMatches(actual, wanted) {
    if (!wanted) return true;
    if (wanted === 'gold') return ['gold', 'gold filled', 'solid gold', 'gold plated'].includes(actual);
    if (wanted === 'rose gold') return actual.indexOf('rose ') === 0;
    return actual === wanted;
  }
  function groupsFrom(p, variants) {
    var provided = Array.isArray(p.options) ? p.options : null;
    if (provided && provided.length > 12) return null;
    var groups = provided ? provided.map(function (group) {
      var name = text(group && group.name, 120), values = (Array.isArray(group && group.values) ? group.values : []).map(function (value) { return text(value, 300); });
      return name && values.length && values.length <= 100 && values.every(Boolean) && unique(values.map(key)).length === values.length ? { name: name, values: values } : null;
    }) : unique(variants.flatMap(function (v) { return v.options.map(function (option) { return option.name; }); })).map(function (name) {
      return { name: name, values: unique(variants.flatMap(function (v) { return v.options.filter(function (option) { return option.name === name; }).map(function (option) { return option.value; }); })) };
    });
    if (groups.some(function (group) { return !group; }) || unique(groups.map(function (group) { return key(group.name); })).length !== groups.length) return null;
    return groups.length <= 12 && variants.every(function (variant) { return variant.options.length === groups.length && groups.every(function (group) { return variant.options.filter(function (option) { return option.name === group.name && group.values.includes(option.value); }).length === 1; }); }) ? groups : null;
  }
  function normalizeProduct(p, vocabulary, knownMotifs) {
    if (!p || !PRODUCT.test(p.id || '') || !HANDLE.test(p.handle || '') || p.handle.length > 180 || !text(p.title, 300) || !/^[A-Z]{3}$/.test(p.currency || '') || !Array.isArray(p.variants) || !p.variants.length || p.variants.length > 500) return null;
    var url = ownURL(p.url, p.handle), seen = new Set();
    if (!url) return null;
    var variants = p.variants.map(function (v) {
      if (!v || !VARIANT.test(v.id || '') || seen.has(v.id) || !text(v.title, 300) || !Number.isFinite(v.price) || v.price < 0 || typeof v.available !== 'boolean' || !Array.isArray(v.options) || v.options.length > 12) return null;
      seen.add(v.id);
      var options = v.options.map(function (option) { return text(option && option.name, 120) && text(option && option.value, 300) ? { name: option.name.trim(), value: option.value.trim() } : null; });
      if (options.some(function (option) { return !option; }) || unique(options.map(function (option) { return key(option.name); })).length !== options.length) return null;
      var row = { id: v.id, title: v.title.trim(), price: v.price, available: v.available === true && v.availabilityKnown !== false, options: options };
      row.material = variantMaterial(row); return row;
    });
    if (variants.some(function (v) { return !v; })) return null;
    var groups = groupsFrom(p, variants);
    if (!groups) return null;
    var checkedAt = typeof p.checkedAt === 'number' ? p.checkedAt : typeof p.checkedAt === 'string' ? Date.parse(p.checkedAt) : NaN;
    var row = { id: p.id, handle: p.handle, title: p.title.trim(), url: url, image: imageURL(p.image), imageAlt: text(p.imageAlt, 300), currency: p.currency, type: text(p.type, 120), description: text(p.description, 16000), tags: (Array.isArray(p.tags) ? p.tags : []).slice(0, 100).map(function (tag) { return text(tag, 120); }).filter(Boolean), variants: variants, options: groups, checkedAt: checkedAt, variantsComplete: p.variantsComplete === true, detailState: p.detailState, recommendationHold: p.recommendationHold === true, cartHold: p.cartHold === true, meaningHold: p.meaningHold === true, partsOnly: p.partsOnly === true };
    row.category = productCategory(row);
    row.evidence = motifEvidence(p, vocabulary, knownMotifs);
    row.service = /\b(?:design fee|engraving service|membership|credit pack|digital studio|chain extender)\b/.test(key(row.title));
    return freeze(row);
  }
  function preferences(value) {
    value = value && typeof value === 'object' ? value : {};
    var budget = value.budget && typeof value.budget === 'object' ? value.budget : {}, max = typeof value.budget === 'number' ? value.budget : budget.max, min = value.minBudget === undefined ? budget.min : value.minBudget;
    var desired = text(typeof value.material === 'string' ? value.material : value.materialLabel || value.metal, 120), currency = value.budgetCurrency || budget.currency || '';
    return { material: material(desired) || (/^silver$/i.test(desired) ? 'silver' : ''), materialLabel: desired, max: Number.isFinite(max) && max >= 0 ? max : null, min: Number.isFinite(min) && min >= 0 ? min : null, currency: /^[A-Z]{3}$/.test(currency) ? currency : '', type: category(value.type), themes: unique((Array.isArray(value.themes) ? value.themes : Array.isArray(value.interests) ? value.interests : []).map(function (v) { return key(text(v, 120)); }).filter(Boolean)).slice(0, 30), excludedThemes: unique((Array.isArray(value.excludedInterests) ? value.excludedInterests : []).map(function (v) { return key(text(v, 120)); }).filter(Boolean)).slice(0, 30), excludedMaterials: unique((Array.isArray(value.excludedMetals) ? value.excludedMetals : []).map(function (v) { return material(text(v, 120)); }).filter(Boolean)).slice(0, 12), excludedTypes: unique((Array.isArray(value.excludedTypes) ? value.excludedTypes : []).map(category).filter(Boolean)).slice(0, 5), length: text(value.length || value.necklaceLength, 100), allowProactive: value.allowProactive !== false };
  }
  function matchesTheme(p, theme, vocabulary) {
    if (vocabulary && typeof vocabulary.motifMatches === 'function') return vocabulary.motifMatches(p, theme);
    var entry = motifWords.get(theme), normalized = entry ? entry[0] : theme;
    if (p.evidence.motifs.includes(normalized) || p.evidence.groups.includes(normalized)) return true;
    return words(theme).length > 0 && words(theme).every(function (term) { return p.evidence.tokens.includes(term); });
  }
  function identity(p) { return { id: p.id, handle: p.handle, title: p.title }; }
  function incompleteContext(context, p) { var pc = context.productControls; return !pc || pc.handle !== p.handle || pc.productId !== p.id ? null : pc; }
  function knownChoices(p, pc) {
    return (Array.isArray(pc && pc.selectedOptions) ? pc.selectedOptions : []).slice(0, 12).flatMap(function (choice) {
      var group = p.options.find(function (g) { return g.name === choice.name; });
      return group && group.values.includes(choice.value) ? [{ name: group.name, value: choice.value }] : [];
    });
  }
  function selectedVariant(p, pc) {
    if (!pc || !Array.isArray(pc.selectedOptions)) return null;
    var choices = knownChoices(p, pc), variant = p.variants.find(function (v) { return v.id === pc.variantId && v.available; });
    if (!variant || choices.length !== p.options.length || choices.length !== pc.selectedOptions.length || unique(choices.map(function (choice) { return choice.name; })).length !== choices.length) return null;
    return choices.every(function (choice) { return variant.options.some(function (option) { return option.name === choice.name && option.value === choice.value; }); }) ? variant : null;
  }
  function personalized(p, variant) {
    return /\b(?:handwriting|monogram|custom studio)\b/.test(key(p.title)) || variant.options.some(function (option) { return /engrav|personali[sz]|custom/i.test(option.name) && !/^(?:no|none|without|not included|no engraving|without engraving)$/i.test(option.value); });
  }
  function materialAdvice(value) {
    var actual = material(value);
    if (actual === 'silver' && /sterling/i.test(value)) return { text: 'Sterling silver is a silver alloy. It can develop tarnish over time.', sources: [METAL_SOURCE, SILVER_SOURCE] };
    if (/gold filled$/.test(actual)) return { text: 'Gold-filled has a mechanically applied gold layer over a base metal.', sources: [METAL_SOURCE] };
    if (/solid gold$/.test(actual)) return { text: 'Karat gold contains gold mixed with other metals; its karat describes the gold proportion.', sources: [METAL_SOURCE] };
    if (/gold plated$/.test(actual)) return { text: 'Gold plating is a surface layer. Wear depends on the coating and how it is used.', sources: [METAL_SOURCE] };
    return null;
  }
  function requestedCategories(message) {
    var found = [];
    [['necklace', /\b(?:necklaces?|pendants?)\b/], ['earrings', /\b(?:earrings?|studs?|hoops?|huggies?)\b/], ['bracelet', /\bbracelets?\b/], ['ring', /\brings?\b/], ['charm', /\bcharms?\b/]].forEach(function (pair) { if (pair[1].test(message)) found.push(pair[0]); });
    return found;
  }
  function dimensionValue(value, unit) {
    var amount = Number(value), normalized = unit.toLowerCase();
    return amount > 0 && Number.isFinite(amount) ? amount * (/^(?:cm|centimet)/.test(normalized) ? 10 : /^(?:in|inch|\")/.test(normalized) ? 25.4 : 1) : null;
  }
  function dimensionsFor(p, variant) {
    var unit = '(mm|cm|millimet(?:er|re)s?|centimet(?:er|re)s?|inches?|inch|in|\")';
    var relevantOption = /^(?:charm|pendant|hoop|disc|design)\s+(?:size|diameter|width|height|dimensions?)$/i;
    var selected = variant && variant.options.filter(function (option) { return relevantOption.test(option.name); });
    if (selected && selected.length === 1) {
      var literal = selected[0].value.match(new RegExp('^\\s*(\\d+(?:\\.\\d+)?)\\s*' + unit + '\\s*$', 'i'));
      if (literal) { var result = { kind: /diameter/i.test(selected[0].name) ? 'diameter' : /width/i.test(selected[0].name) ? 'width' : /height/i.test(selected[0].name) ? 'height' : 'option-size', quote: selected[0].name + ': ' + selected[0].value, source: 'published-variant-option', optionName: selected[0].name, sizeMm: dimensionValue(literal[1], literal[2]) }; if (result.kind === 'diameter') result.diameterMm = result.sizeMm; return result; }
    }
    if (p.options.some(function (group) { return relevantOption.test(group.name) && group.values.length > 1; })) return null;
    var detail = visibleDescription(p.description), sentences = detail.match(/(?:[^.!?]|\.(?=\d))+(?:[.!?](?!\d)|$)/g) || [];
    var measurements = {};
    var quotes = [];
    function accept(axis, value, units, sentence) {
      var n = dimensionValue(value, units); if (n === null) return;
      if (Object.hasOwn(measurements, axis) && Math.abs(measurements[axis] - n) > .000001) measurements.conflict = true;
      else measurements[axis] = n;
      if (!quotes.includes(sentence.trim())) quotes.push(sentence.trim());
    }
    sentences.forEach(function (sentence) {
      if (!/\b(?:charm|pendant|disc|design|hoop|stud)\b/i.test(sentence) || sentence.length > 500) return;
      var after = new RegExp('(\\d+(?:\\.\\d+)?)\\s*' + unit + '\\s*(?:in\\s+)?(wide|width|high|height|tall|diameter|long|length)\\b', 'gi');
      var before = new RegExp('\\b(width|height|diameter|length)\\s*(?:is|of|:)?\\s*(\\d+(?:\\.\\d+)?)\\s*' + unit, 'gi');
      for (var hit of sentence.matchAll(after)) accept(/wide|width/.test(hit[3].toLowerCase()) ? 'widthMm' : /diameter/.test(hit[3].toLowerCase()) ? 'diameterMm' : 'heightMm', hit[1], hit[2], sentence);
      for (var leading of sentence.matchAll(before)) accept(/width/.test(leading[1].toLowerCase()) ? 'widthMm' : /diameter/.test(leading[1].toLowerCase()) ? 'diameterMm' : 'heightMm', leading[2], leading[3], sentence);
    });
    if (measurements.conflict || !quotes.length) return null;
    if (measurements.widthMm && measurements.heightMm) return { kind: 'width-height', widthMm: measurements.widthMm, heightMm: measurements.heightMm, quote: quotes.join(' '), source: 'product-description' };
    if (measurements.diameterMm) return { kind: 'diameter', diameterMm: measurements.diameterMm, quote: quotes.join(' '), source: 'product-description' };
    return null;
  }
  function isSmaller(candidate, reference) {
    if (!candidate || !reference || candidate.kind !== reference.kind) return false;
    if (reference.kind === 'width-height') return candidate.widthMm <= reference.widthMm && candidate.heightMm <= reference.heightMm && (candidate.widthMm < reference.widthMm || candidate.heightMm < reference.heightMm);
    if (reference.kind === 'diameter') return candidate.diameterMm < reference.diameterMm;
    return candidate.kind === 'option-size' && candidate.optionName === reference.optionName && candidate.sizeMm < reference.sizeMm;
  }
  function detailBullets(p, pc, fresh) {
    var out = [{ kind: 'identity', label: 'Viewing', text: p.title, source: 'product-title' }];
    if (!fresh) { out.push({ kind: 'availability', label: 'Check needed', text: 'These saved details need a fresh product check before selecting or recommending an option.', source: 'catalogue-check-time' }); return out; }
    var chosen = selectedVariant(p, pc);
    var available = p.variants.filter(function (v) { return v.available; });
    if (chosen) out.push({ kind: 'price', label: 'Selected price', text: nativeMoney(chosen.price, p.currency), source: 'selected-variant' });
    else if (available.length) out.push({ kind: 'price', label: 'Available options from', text: nativeMoney(Math.min.apply(null, available.map(function (v) { return v.price; })), p.currency), source: 'available-variants' });
    p.options.forEach(function (group) {
      if (/\b(?:metal|material|finish|length|size|diameter|engraving|personalization)\b/i.test(group.name)) out.push({ kind: /length|size|diameter/i.test(group.name) ? 'dimensions' : /engrav|personali/i.test(group.name) ? 'engraving' : 'materials', label: group.name, text: group.values.slice(0, 10).join(' · ') + (group.values.length > 10 ? ' · more published choices' : ''), source: 'published-options' });
    });
    // Retain a literal measurement sentence. Never infer dimensions from a
    // photograph or a chain length, and never invent historical symbolism.
    var detail = visibleDescription(p.description);
    var sentences = detail.match(/(?:[^.!?]|\.(?=\d))+(?:[.!?](?!\d)|$)/g) || [];
    var measurement = sentences.find(function (sentence) { return /\b\d+(?:\.\d+)?\s*(?:mm|cm|millimet(?:er|re)s?|centimet(?:er|re)s?)\b/i.test(sentence) && /\b(?:charm|pendant|width|wide|height|high|diameter|thick|measures?|dimensions?)\b/i.test(sentence); });
    if (measurement && measurement.trim().length <= 360) out.push({ kind: 'dimensions', label: 'Published dimensions', text: measurement.trim(), source: 'product-description' });
    if (p.cartHold || p.recommendationHold) out.push({ kind: 'availability', label: 'Check with shop', text: 'This piece has a product check pending; I can explain its published details but cannot suggest adding it.', source: 'product-hold' });
    else if (!available.length) out.push({ kind: 'availability', label: 'Availability', text: 'No confirmed available option in this checked listing.', source: 'available-variants' });
    return out.slice(0, 9);
  }

  function create(options) {
    options = options && typeof options === 'object' ? options : {};
    var now = typeof options.now === 'function' ? options.now : Date.now;
    var maxAge = Number.isFinite(options.maxAgeMs) ? Math.max(1000, Math.min(300000, options.maxAgeMs)) : 300000;
    var cooldown = Number.isFinite(options.promptCooldownMs) ? Math.max(0, options.promptCooldownMs) : 45000;
    var storage = options.storage, storageKey = text(options.sessionKey, 160) || 'brites-concierge-shopping-guide-v1';
    var vocabulary = options.vocabulary || defaultVocabulary, knownMotifs = new Map(motifWords);
    if (vocabulary && typeof vocabulary.searchTerms === 'function' && Array.isArray(vocabulary.themeNames)) {
      var family = { animals: 'animal', birds: 'animal', pets: 'animal', insects: 'animal', ocean: 'ocean', flowers: 'botanical', nature: 'botanical', celestial: 'celestial' };
      vocabulary.themeNames.forEach(function (theme) { vocabulary.searchTerms({ themes: [theme] }).forEach(function (word) { if (word.length > 2 && !STOP.has(word) && !['forget', 'not'].includes(word) && !knownMotifs.has(word)) knownMotifs.set(word, [word, family[theme] || theme]); }); });
    }
    var products = new Map(), byCategory = new Map(), byMotif = new Map(), pref = preferences(options.preferences), muted = false, dismissed = new Set(), shown = new Set(), shownProducts = new Set(), lastShownAt = -Infinity;
    function persist() { try { if (storage && typeof storage.setItem === 'function') storage.setItem(storageKey, JSON.stringify({ muted: muted, dismissed: Array.from(dismissed).slice(-300), shown: Array.from(shown).slice(-300), shownProducts: Array.from(shownProducts).slice(-300) })); } catch (e) {} }
    try {
      var saved = storage && typeof storage.getItem === 'function' && JSON.parse(storage.getItem(storageKey) || 'null');
      if (saved && typeof saved === 'object') {
        muted = saved.muted === true;
        ['dismissed', 'shown'].forEach(function (name) { var target = name === 'dismissed' ? dismissed : shown; (Array.isArray(saved[name]) ? saved[name] : []).slice(-300).forEach(function (v) { if (text(v, 1000)) target.add(v); }); });
        (Array.isArray(saved.shownProducts) ? saved.shownProducts : []).slice(-300).forEach(function (v) { if (HANDLE.test(v || '') && v.length <= 180) shownProducts.add(v); });
      }
    } catch (e) {}
    function isFresh(p) { var time = now(); return Number.isFinite(p.checkedAt) && p.checkedAt > 0 && p.checkedAt <= time + 60000 && time - p.checkedAt < maxAge && p.detailState !== 'unconfirmed' && p.variantsComplete === true; }
    function updateProducts(rows) {
      products = new Map(); byCategory = new Map(); byMotif = new Map();
      var seenIds = new Map(), conflicts = new Set();
      (Array.isArray(rows) ? rows : []).slice(0, 15000).forEach(function (row) { var p = normalizeProduct(row, vocabulary, knownMotifs); if (!p) return; if (seenIds.has(p.id)) { conflicts.add(p.handle); conflicts.add(seenIds.get(p.id)); } if (products.has(p.handle)) conflicts.add(p.handle); if (!seenIds.has(p.id)) seenIds.set(p.id, p.handle); products.set(p.handle, p); });
      conflicts.forEach(function (handle) { products.delete(handle); });
      products.forEach(function (p) { if (!byCategory.has(p.category)) byCategory.set(p.category, new Set()); byCategory.get(p.category).add(p.handle); p.evidence.motifs.forEach(function (motif) { if (!byMotif.has(motif)) byMotif.set(motif, new Set()); byMotif.get(motif).add(p.handle); }); });
      return products.size;
    }
    function setPreferences(value) { pref = preferences(value); return freeze(Object.assign({}, pref, { themes: pref.themes.slice(), excludedThemes: pref.excludedThemes.slice(), excludedMaterials: pref.excludedMaterials.slice(), excludedTypes: pref.excludedTypes.slice() })); }
    function comparisonFor(p, context, request) {
      if (!request || !['cheaper', 'smaller', 'category'].includes(request.kind)) return null;
      var result = { kind: request.kind, status: 'missing-reference', requestedCategories: (request.requestedCategories || []).filter(function (v) { return Object.hasOwn(CATEGORY_LABELS, v); }), requireSmaller: request.kind === 'smaller' || request.requireSmaller === true, reference: null };
      if (!p || !isFresh(p) || p.cartHold || p.recommendationHold) return result;
      if (request.kind === 'category') { result.status = 'confirmed'; return result; }
      var pc = incompleteContext(context, p), chosen = selectedVariant(p, pc), matching = p.variants.filter(function (v) { return v.available && materialMatches(v.material, pref.material); }).sort(function (a, b) { return a.price - b.price || a.id.localeCompare(b.id); });
      var reference = chosen || matching[0];
      if (!reference) return result;
      if (request.kind === 'cheaper') {
        var materialLabel = reference.options.filter(function (option) { return /metal|material|finish/i.test(option.name); }).map(function (option) { return option.value; }).join(' / ');
        if (!pref.material && !reference.material) return result;
        result.reference = { price: reference.price, currency: p.currency, material: pref.material || reference.material, materialLabel: pref.material ? '' : materialLabel, materialIsPreference: !!pref.material, variantId: reference.id, source: chosen ? 'selected-variant-unit-price' : 'available-variant-from-price' };
        if (result.requireSmaller) { result.reference.dimensions = dimensionsFor(p, chosen); if (!result.reference.dimensions) { result.reference = null; return result; } }
      }
      else { var dimensions = dimensionsFor(p, chosen); if (dimensions) result.reference = { dimensions: dimensions, variantId: chosen && chosen.id || null, source: dimensions.source }; }
      if (result.reference) result.status = 'confirmed'; return result;
    }
    function candidates(p, kind, context, passive, comparison) {
      if (!p || !p.category || !isFresh(p) || p.service || restrictedParts(p) || p.recommendationHold || p.cartHold) return [];
      var handles = new Set(), bag = new Set((Array.isArray(context.bagControls && context.bagControls.lines) ? context.bagControls.lines : []).map(function (line) { return line.productId; }));
      if (kind === 'alternatives') (byCategory.get(p.category) || []).forEach(function (h) { handles.add(h); });
      else p.evidence.motifs.forEach(function (motif) { (byMotif.get(motif) || []).forEach(function (h) { handles.add(h); }); });
      var pc = incompleteContext(context, p), current = selectedVariant(p, pc);
      var preferredMaterial = pref.material || current && current.material || '';
      if (comparison && comparison.kind === 'cheaper' && comparison.reference) preferredMaterial = comparison.reference.material;
      var budgetCurrency = pref.currency || p.currency;
      var results = [], eligibleExactMotif = false;
      handles.forEach(function (handle) {
        var other = products.get(handle);
        if (other.id === p.id || bag.has(other.id) || !isFresh(other) || other.service || restrictedParts(other) || other.recommendationHold || other.cartHold || pref.excludedTypes.includes(other.category)) return;
        if (kind === 'matching' && (!other.category || other.category === p.category)) return;
        if (comparison && comparison.requestedCategories.length && !comparison.requestedCategories.includes(other.category)) return;
        if (comparison && comparison.kind !== 'category' && !comparison.reference) return;
        var motifs = p.evidence.motifs.filter(function (m) { return other.evidence.motifs.includes(m); });
        var groups = p.evidence.groups.filter(function (g) { return other.evidence.groups.includes(g); });
        if (kind === 'matching' && !motifs.length) return;
        if (kind === 'alternatives' && p.evidence.motifs.length && !motifs.length && !groups.length) return;
        // A manual current design owns exact motif comparisons. Earlier
        // positive discovery themes still constrain broader fallback designs;
        // explicit exclusions apply even to a direct current-motif match.
        if (!motifs.length && pref.themes.length && !pref.themes.some(function (theme) { return matchesTheme(other, theme, vocabulary); }) || pref.excludedThemes.some(function (theme) { return matchesTheme(other, theme, vocabulary); })) return;
        // A single real available variant must meet metal AND budget. Prices
        // from one option can never justify recommending another option.
        var variants = other.variants.filter(function (v) { return v.available && materialMatches(v.material, preferredMaterial) && !pref.excludedMaterials.some(function (m) { return materialMatches(v.material, m); }) && ((pref.max === null && pref.min === null) || budgetCurrency === other.currency && (pref.max === null || v.price <= pref.max) && (pref.min === null || v.price >= pref.min)) && (!comparison || comparison.kind !== 'cheaper' || other.currency === comparison.reference.currency && v.price < comparison.reference.price && (comparison.reference.materialIsPreference || v.material === comparison.reference.material && (!comparison.reference.materialLabel || key(v.options.filter(function (option) { return /metal|material|finish/i.test(option.name); }).map(function (option) { return option.value; }).join(' / ')) === key(comparison.reference.materialLabel)))) && (!comparison || !comparison.requireSmaller || isSmaller(dimensionsFor(other, v), comparison.reference.dimensions)); });
        if (!variants.length) return;
        // Rejected direct matches still define the requested design scope.
        // Exhausting them must not turn a rejection into unrelated upselling.
        if (kind === 'alternatives' && motifs.length) eligibleExactMotif = true;
        if (dismissed.has('handle:' + handle) || passive && shownProducts.has(handle)) return;
        variants.sort(function (a, b) { return a.price - b.price || a.id.localeCompare(b.id); });
        var v = variants[0], reasons = [], score = motifs.length * 30 + groups.length * 4;
        if (motifs.length) reasons.push('Shares the ' + motifs.slice(0, 2).join(' and ') + ' motif with this piece');
        else if (groups.length) reasons.push('Another ' + groups[0] + ' ' + CATEGORY_LABELS[other.category] + ' design');
        else reasons.push('Another ' + CATEGORY_LABELS[other.category] + ' option');
        if (preferredMaterial && v.material) { reasons.push('Available ' + v.options.filter(function (o) { return /metal|material|finish/i.test(o.name); }).map(function (o) { return o.value; }).join(' / ')); score += 3; }
        if (pref.max !== null || pref.min !== null) { reasons.push('Within your ' + budgetCurrency + ' item budget'); score += 2; }
        if (kind === 'matching') reasons.push('A suggested pairing; each piece is sold separately');
        if (comparison && comparison.kind === 'cheaper') reasons.push('Lower than the current ' + nativeMoney(comparison.reference.price, comparison.reference.currency) + (comparison.reference.source === 'available-variant-from-price' ? ' available from-price' : ' unit price') + '; shipping and taxes are separate');
        var candidateDimensions = comparison && comparison.requireSmaller ? dimensionsFor(other, v) : null;
        if (candidateDimensions) reasons.push('Smaller by the comparable published ' + (candidateDimensions.kind === 'diameter' ? 'diameter' : candidateDimensions.kind === 'width-height' ? 'width and height' : candidateDimensions.optionName));
        results.push({ score: score, exactMotif: motifs.length > 0, id: other.id, handle: other.handle, title: other.title, image: other.image, imageAlt: other.imageAlt || other.title, why: reasons.filter(function (s) { return s !== 'Available '; }).join(' · '), price: v.price, currency: other.currency, variantId: v.id, variantTitle: v.title, checkedAt: other.checkedAt, dimensions: candidateDimensions, requiresFreshCheck: true, action: { type: 'open', handle: other.handle } });
      });
      // Prefer the exact current motif after all availability and preference
      // checks. Broader theme designs help only when no direct match remains.
      if (kind === 'alternatives' && eligibleExactMotif) results = results.filter(function (row) { return row.exactMotif; });
      return results.sort(function (a, b) { return b.score - a.score || (a.currency === b.currency ? a.price - b.price : a.currency.localeCompare(b.currency)) || a.title.localeCompare(b.title); }).slice(0, 3).map(function (row) { delete row.score; delete row.exactMotif; return row; });
    }
    function optionHelp(p, context) {
      var pc = incompleteContext(context, p), choices = knownChoices(p, pc), suggestions = [], next = null;
      // A newly stated preference can replace an earlier chosen material or
      // length. Keep the other current choices and propose the actual change.
      choices = choices.filter(function (choice) { return !(/metal|material|finish/i.test(choice.name) && pref.material && !materialMatches(material(choice.value), pref.material)) && !(/length/i.test(choice.name) && pref.length && key(choice.value) !== key(pref.length)); });
      if (!isFresh(p) || p.cartHold || p.recommendationHold || restrictedParts(p) || p.service) return { suggestions: suggestions, next: null };
      var variants = p.variants.filter(function (v) { return v.available && choices.every(function (choice) { return v.options.some(function (option) { return option.name === choice.name && option.value === choice.value; }); }) && materialMatches(v.material, pref.material) && !pref.excludedMaterials.some(function (m) { return materialMatches(v.material, m); }) && (!pref.length || !v.options.some(function (option) { return /length/i.test(option.name); }) || v.options.some(function (option) { return /length/i.test(option.name) && key(option.value) === key(pref.length); })) && ((pref.max === null && pref.min === null) || (!pref.currency || pref.currency === p.currency) && (pref.max === null || v.price <= pref.max) && (pref.min === null || v.price >= pref.min)); });
      if (!variants.length) return { suggestions: suggestions, next: null };
      var selected = selectedVariant(p, pc); if (selected && !variants.includes(selected)) selected = null;
      var unresolved = p.options.filter(function (group) { return !choices.some(function (choice) { return choice.name === group.name; }); });
      unresolved.forEach(function (group) {
        var values = unique(variants.flatMap(function (v) { return v.options.filter(function (o) { return o.name === group.name; }).map(function (o) { return o.value; }); }));
        var reason = '', value = null;
        if (/metal|material|finish/i.test(group.name) && pref.material && values.length === 1) { value = values[0]; reason = 'Matches your ' + (pref.materialLabel || pref.material) + ' preference'; }
        else if (/length/i.test(group.name) && pref.length) { var exact = values.filter(function (v) { return key(v) === key(pref.length); }); if (exact.length === 1) { value = exact[0]; reason = 'Matches the length you requested'; } }
        else if (values.length === 1) { value = values[0]; reason = choices.length ? 'The only available choice compatible with your selections' : 'The only confirmed available choice for this option'; }
        else if (/metal|material|finish/i.test(group.name) && pref.max !== null && variants.length) { var cheapest = variants.slice().sort(function (a, b) { return a.price - b.price; })[0], option = cheapest.options.find(function (o) { return o.name === group.name; }); value = option && option.value; reason = 'The least expensive available material within your item budget'; }
        if (value) {
          var matching = variants.filter(function (v) { return v.options.some(function (o) { return o.name === group.name && o.value === value; }); }).sort(function (a, b) { return a.price - b.price; });
          suggestions.push({ name: group.name, value: value, reason: reason, price: matching[0].price, currency: p.currency, priceIsFrom: true, advice: /metal|material|finish/i.test(group.name) ? materialAdvice(value) : null, action: { type: 'select-option', handle: p.handle, optionName: group.name, optionValue: value } });
        } else if (/metal|material|finish/i.test(group.name) && values.length) {
          var options = values.map(function (value) { var v = variants.filter(function (v) { return v.options.some(function (o) { return o.name === group.name && o.value === value; }); }).sort(function (a, b) { return a.price - b.price; })[0]; return { value: value, variant: v }; }).sort(function (a, b) { return a.variant.price - b.variant.price || a.value.localeCompare(b.value); }).slice(0, 2);
          options.forEach(function (option, index) { suggestions.push({ name: group.name, value: option.value, reason: index === 0 ? 'The least expensive available material for the compatible published options' : 'Another available material to compare before choosing', price: option.variant.price, currency: p.currency, priceIsFrom: true, advice: materialAdvice(option.value), action: { type: 'select-option', handle: p.handle, optionName: group.name, optionValue: option.value } }); });
        }
      });
      if (pc && pc.reviewReady === true && selected) next = { kind: 'review', text: 'Your ' + selected.title + ' selection is ready at ' + nativeMoney(selected.price, p.currency) + '. Use the visible confirmation button to add it.', action: null, requiresCustomerClick: true };
      else if (pc && pc.selectionStatus === 'ready' && selected && !personalized(p, selected)) {
        var quantity = Number.isInteger(pc.quantity) && pc.quantity >= 1 && pc.quantity <= 20 ? pc.quantity : 1, subtotal = Math.round(selected.price * quantity * 100) / 100;
        next = { kind: 'add', text: selected.title + ' · quantity ' + quantity + ' · item subtotal ' + nativeMoney(subtotal, p.currency) + '. Add it when you’re happy with your choices.', action: { type: 'add', handle: p.handle, variantId: selected.id }, quantity: quantity, price: selected.price, subtotal: subtotal, currency: p.currency, requiresCustomerClick: true };
      }
      else if (unresolved.length || !selected) {
        var first = unresolved.find(function (group) { return /metal|material|finish/i.test(group.name); }) || unresolved[0] || p.options[0];
        if (first) next = { kind: 'options', text: 'Let’s choose ' + first.name + ' for ' + p.title + '.', choices: first.values.slice(), action: { type: 'options', handle: p.handle, optionName: first.name } };
      } else if (!personalized(p, selected)) next = { kind: 'review', text: selected.title + ' is ' + nativeMoney(selected.price, p.currency) + '. I can prepare that exact choice for your review.', action: { type: 'review-add', handle: p.handle, variantId: selected.id }, requiresCustomerClick: true };
      else next = { kind: 'options', text: 'Let’s confirm the published personalization requirements for this exact option before adding it.', action: { type: 'options', handle: p.handle } };
      return { suggestions: suggestions, next: next };
    }
    function prepare(context, flags) {
      context = context && typeof context === 'object' ? context : {}; flags = flags || {};
      var scope = context.pageKind === 'product' ? context.currentHandle : context.focusedHandle || context.currentHandle;
      var p = products.get(scope), pack = { current: p ? identity(p) : null, details: [], alternatives: [], matching: [], optionSuggestions: [], nextStep: null, warnings: [] };
      // A bag/checkout page never borrows an old listing as its active target.
      if (['bag', 'cart', 'checkout'].includes(context.pageKind)) {
        var lines = Array.isArray(context.bagControls && context.bagControls.lines) ? context.bagControls.lines : [];
        pack.current = null;
        if (context.pageKind !== 'checkout' && lines.length) pack.nextStep = { kind: 'bag', text: 'Your bag is ready to review. Continue to checkout when you’re happy with your choices.', action: { type: 'checkout' } };
        return freeze(pack);
      }
      if (!p) return freeze(pack);
      var fresh = isFresh(p), pc = incompleteContext(context, p), help = optionHelp(p, context);
      var comparison = comparisonFor(p, context, flags.comparison);
      pack.details = detailBullets(p, pc, fresh);
      pack.optionSuggestions = help.suggestions; pack.nextStep = help.next;
      pack.alternatives = candidates(p, 'alternatives', context, flags.passive === true, comparison);
      pack.matching = candidates(p, 'matching', context, flags.passive === true, comparison);
      if (comparison) { if (comparison.status === 'confirmed' && !(comparison.kind === 'category' ? pack.matching : pack.alternatives).length) comparison.status = 'no-match'; pack.comparison = comparison; }
      if ((pref.max !== null || pref.min !== null) && pref.currency && pref.currency !== p.currency) pack.warnings.push('This listing is priced in ' + p.currency + '; your ' + pref.currency + ' budget cannot be compared without a verified exchange rate.');
      if ((pref.max !== null || pref.min !== null) && !pref.currency) pack.warnings.push('Your item budget is being interpreted in ' + p.currency + ', the current listing’s currency.');
      if (!fresh) pack.warnings.push('The exact listing needs a fresh product check.');
      if (fresh && p.variants.some(function (v) { return v.available; }) && !pack.nextStep && !p.cartHold && !p.recommendationHold) pack.warnings.push('No available choice satisfies all of the current material, option and item-budget preferences.');
      return freeze(pack);
    }
    function suggest(request) {
      request = request && typeof request === 'object' ? request : {};
      var context = request.context || {}, message = key(text(request.message, 2000)), trigger = request.trigger || '';
      var cheaper = /\b(?:cheaper|less expensive|lower priced|lower price)\b/.test(message), smaller = /\b(?:smaller|more petite|more delicate|tinier)\b/.test(message);
      var moreLike = /\bmore like (?:that|this)\b/.test(message), rejected = /\bnot those\b/.test(message);
      var explicit = cheaper || smaller || moreLike || rejected || /\b(?:similar|alternatives?|other options|other designs|other pieces|anything else|something else|what else|another design|different design|not sure|unsure|undecided|what matches|what would match|(?:go|goes) with|matching|pair with|complete the set|make a set|help me choose|help me pick|walk me through|help me select|suggest|recommend)\b/.test(message);
      var kind = moreLike || rejected ? 'alternatives' : /\b(?:matching|what matches|what would match|(?:go|goes) with|pair with|set)\b/.test(message) || trigger === 'matching' ? 'matching' : cheaper || smaller || /\b(?:similar|alternatives?|other designs|other pieces|anything else|something else|what else|another|different|not sure|unsure|undecided)\b/.test(message) || trigger === 'uncertain' ? 'alternatives' : 'options';
      var categories = requestedCategories(message), comparative = cheaper ? 'cheaper' : smaller ? 'smaller' : categories.length && kind === 'matching' ? 'category' : '';
      var pack = prepare(context, { passive: !explicit, comparison: comparative ? { kind: comparative, requireSmaller: smaller, requestedCategories: categories } : null }), current = pack.current && pack.current.handle || '', suggestion = null;
      if (context.loading === true || context.busy === true || context.speaking === true || context.hidden === true) return { pack: pack, suggestion: null };
      if (!explicit && (muted || !pref.allowProactive || now() - lastShownAt < cooldown)) return { pack: pack, suggestion: null };
      if (['bag', 'cart'].includes(context.pageKind)) kind = 'bag';
      if (!explicit && (dismissed.has('kind:' + kind) || dismissed.has('handle:' + current))) return { pack: pack, suggestion: null };
      if (kind === 'alternatives' || kind === 'matching') {
        var rows = pack[kind].slice(0, 2);
        if (rows.length) suggestion = { kind: kind, handle: current, text: cheaper ? 'I found ' + rows.length + (smaller ? ' lower-priced, smaller alternative' : ' lower-priced alternative') + (rows.length === 1 ? '' : 's') + ' in the same currency and ' + (pack.comparison.reference.materialIsPreference ? 'your requested material' : 'material') + '.' : smaller ? 'I found ' + rows.length + ' alternative' + (rows.length === 1 ? '' : 's') + ' with smaller comparable published dimensions.' : kind === 'matching' ? 'I have ' + rows.length + (rows.length === 1 ? ' piece that shares' : ' pieces that share') + ' this design’s motif for a possible pairing.' : 'I have ' + rows.length + ' other ' + CATEGORY_LABELS[products.get(current).category] + ' option' + (rows.length === 1 ? '' : 's') + ' ready if you’d like to compare.', products: rows, actions: rows.map(function (row) { return row.action; }) };
        else if (explicit && pack.comparison && current) {
          var reason = cheaper ? pack.comparison.status === 'missing-reference' ? 'I need a confirmed current material and price' + (smaller ? ' plus comparable published dimensions' : '') + ' before I can verify that comparison.' : 'I haven’t found a confirmed lower-priced' + (smaller ? ', smaller' : '') + ' alternative in this material and currency.' : smaller ? pack.comparison.status === 'missing-reference' ? 'I don’t have comparable published dimensions for this exact choice yet. I can show its actual size options instead.' : 'I haven’t found a piece with confirmed smaller comparable dimensions in this checked selection.' : 'I haven’t found ' + (categories.length ? 'checked matching ' + categories.map(function (c) { return CATEGORY_LABELS[c] + (c === 'earrings' ? '' : 's'); }).join(' or ') : 'a checked matching piece') + ' for this design.';
          suggestion = { kind: kind, handle: current, text: reason, products: [], actions: [] };
        }
        else if (explicit && kind === 'alternatives' && current) {
          var scopeText = products.get(current).evidence.motifs.length ? 'that shares this piece’s motif' : 'for this design';
          suggestion = { kind: kind, handle: current, text: (rejected ? 'After excluding those pieces, I haven’t found another checked alternative ' : 'I haven’t found another checked alternative ') + scopeText + ' within your current preferences. We can keep this selection or adjust a preference.', products: [], actions: [] };
        }
      } else if (pack.nextStep && (trigger === 'options' || trigger === 'product-open' || trigger === 'bag-ready' || explicit)) suggestion = { kind: kind, handle: current, text: pack.nextStep.text, products: [], optionSuggestions: pack.optionSuggestions, actions: pack.nextStep.action ? [pack.nextStep.action] : [], requiresCustomerClick: pack.nextStep.requiresCustomerClick === true };
      if (suggestion) {
        suggestion.key = kind + ':' + current + ':' + (suggestion.products || []).map(function (p) { return p.handle; }).join(',') + ':' + suggestion.actions.map(function (a) { return a.optionName || a.variantId || a.type; }).join(',');
        if (!explicit && shown.has(suggestion.key)) suggestion = null;
        else suggestion = freeze(suggestion);
      }
      return { pack: pack, suggestion: suggestion, reply: suggestion ? suggestion.text : null };
    }
    function markShown(suggestion) {
      if (!suggestion || !KINDS.includes(suggestion.kind) || !text(suggestion.key, 1000)) return false;
      shown.add(suggestion.key); (Array.isArray(suggestion.products) ? suggestion.products : []).forEach(function (p) { if (HANDLE.test(p.handle || '') && p.handle.length <= 180) shownProducts.add(p.handle); }); lastShownAt = now(); persist(); return true;
    }
    function dismiss(value) {
      if (!value) muted = true;
      else { if (KINDS.includes(value.kind)) dismissed.add('kind:' + value.kind); if (HANDLE.test(value.handle || '') && value.handle.length <= 180) dismissed.add('handle:' + value.handle); }
      persist(); return true;
    }
    function reset() { muted = false; dismissed.clear(); shown.clear(); shownProducts.clear(); lastShownAt = -Infinity; persist(); }
    updateProducts(options.products);
    return Object.freeze({ updateProducts: updateProducts, setPreferences: setPreferences, prepare: prepare, suggest: suggest, markShown: markShown, dismiss: dismiss, reset: reset });
  }
  function productMeasurements(facts,text){
    var question=String(text||'').normalize('NFKC').toLowerCase();
    [facts.title,facts.handle].filter(Boolean).forEach(function(label){question=question.split(String(label).normalize('NFKC').toLowerCase()).join(' ');});
    var components=[{name:'charm',pattern:/\b(?:charms?|pendants?|discs?|disks?)\b/i},{name:'chain',pattern:/\bchains?\b/i},{name:'hoop',pattern:/\b(?:hoops?|huggies?)\b/i}],component=components.find(function(row){return row.pattern.test(question);});
    var axes=[{name:'width',pattern:/\b(?:width|wide)\b/i},{name:'height',pattern:/\b(?:height|high|tall)\b/i},{name:'diameter',pattern:/\b(?:diameter|across)\b/i},{name:'thickness',pattern:/\b(?:thickness|thick)\b/i},{name:'length',pattern:/\b(?:lengths?|long)\b/i},{name:'weight',pattern:/\b(?:weight|heavy|weighs?)\b/i}],requestedAxes=axes.filter(function(row){return row.pattern.test(question);}),broad=/\b(?:dimensions?|measurements?)\b/i.test(question);
    // Keep measurements attached to their published component and axis. A
    // chain length never establishes charm width, even in one mixed sentence.
    var split=/;\s*|,\s*(?:on|with)\s+|\s+(?:on|attached to|hanging from)\s+|\s+(?:and|with|while|plus)\s+(?=(?:(?:the|an?|included)\s+)?(?:\d+(?:\.\d+)?\s*(?:inches?|mm|cm)\s+)?(?:chains?|hoops?|huggies?|charms?|pendants?|discs?|disks?)\b)/i;
    var componentDimensions=(Array.isArray(facts.dimensions)?facts.dimensions:[]).flatMap(function(sentence){return sentence.split(split);}).map(function(sentence){return sentence.trim();}).filter(function(sentence){
      if(!/\d+(?:\.\d+|\/\d+)?\s*(?:mm|cm|millimet(?:er|re)s?|centimet(?:er|re)s?|inch(?:es)?|grams?|oz|["″])/i.test(sentence))return false;
      if(component&&(!component.pattern.test(sentence)||components.some(function(other){return other!==component&&other.pattern.test(sentence);})))return false;
      return true;
    });
    var dimensions=componentDimensions.filter(function(sentence){return broad||!requestedAxes.length||requestedAxes.some(function(axis){return axis.pattern.test(sentence);});});
    var wantsSize=/\b(?:sizes?|sizing|dimensions?|measurements?)\b/i.test(question),optionGroups=(wantsSize||requestedAxes.length?facts.options||[]:[]).filter(function(group){
      return /\b(?:sizes?|sizing|lengths?|width|height|diameter|circumference|fit|thickness|weight)\b/i.test(group.name)&&(!component||component.pattern.test(group.name))&&(broad||!requestedAxes.length||requestedAxes.some(function(axis){return axis.pattern.test(group.name);}));
    });
    var unknownAxes=requestedAxes.filter(function(axis){return !componentDimensions.some(function(sentence){return axis.pattern.test(sentence);})&&!optionGroups.some(function(group){return axis.pattern.test(group.name);});}).map(function(axis){return axis.name;}),requested=(component?component.name+'\u2019s ':"piece\u2019s ")+(unknownAxes.length?unknownAxes.join(' and '):'measurements');
    return {dimensions:dimensions,optionGroups:optionGroups,unknown:unknownAxes.length||!dimensions.length&&(broad||!optionGroups.length)?'The published details do not confirm the '+requested+'. I can help you ask the studio for the exact measurement.':''};
  }
  return Object.freeze({ create: create, productMeasurements: productMeasurements });
});
