// Free ad-design previews for the Opportunities tab (assets/ad-preview-thumbs.js): which formats each campaign
// type shows, that every word comes from the opportunity, that hostile text and bad image URLs are contained, and
// that the inspect dialog is accessible. No network, no AI, no image generation: the module has none to call.
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path');
const root = path.resolve(__dirname, '../..');
const A = require(root + '/assets/ad-preview-thumbs.js'), S = require(root + '/brites-campaign-styles.js');
const source = fs.readFileSync(root + '/assets/ad-preview-thumbs.js', 'utf8');
let checks = 0;
const ok = (v, m) => { assert(v, m); checks++; }, eq = (a, b, m) => { assert.deepEqual(a, b, m); checks++; };
const keys = (opp, kind, opts) => A.previewSet(opp, kind, opts).map(d => d.key);
const PIC = 'https://cdn.shopify.com/s/files/1/0001/hero.jpg?v=1&width=800&height=800&crop=center';

(async () => {
  const {JSDOM} = require(process.env.BRITES_EDITOR_DOM_RUNTIME ? path.join(process.env.BRITES_EDITOR_DOM_RUNTIME, 'jsdom') : 'jsdom');
  const textOf = html => { const d = new JSDOM('<body></body>').window.document, n = d.createElement('div'); n.innerHTML = html; return n; };

  const search = { collectionTitle: 'Charm Bracelets', collectionHandle: 'charm-bracelets', occasion: 'Mother’s Day',
    keywords: ['personalized mom charm bracelet', 'custom name bracelet for mom'], keyPhrases: ['A gift she will keep', 'Made just for her'] };
  const pmax = { collectionTitle: 'Birthstone Necklaces', feedLabel: 'US', angle: 'A birthstone for every story',
    offerDetails: [{ itemId: 'shopify_US_1_11', title: 'Birthstone Necklace, Silver', productTitle: 'Personalized Birthstone Necklace', imageUrl: PIC, price: 39, currency: 'USD' },
      { itemId: 'shopify_US_2_21', title: 'Family Circle Necklace', imageUrl: 'https://cdn.shopify.com/s/files/1/0001/two.jpg', price: 52.5, currency: 'USD' }],
    research: { listingFit: [{ itemId: 'shopify_US_1_11', role: 'hero' }], creativeAngles: ['A birthstone for every story'] } };

  // 1. Which formats each campaign type shows, and that the campaign types are real ones.
  eq(keys(search, 'search'), ['search-text', 'search-links'], 'Search: the text ad and the same ad with the sitelinks and callouts the publisher adds');
  eq(keys(pmax, 'pmax'), ['pmax-shopping', 'pmax-display', 'pmax-feed', 'pmax-video'], 'Performance Max: listing, website tile, feed card, video slot');
  eq(keys(pmax, 'responsive_display'), ['rd-landscape', 'rd-square', 'rd-native'], 'Responsive Display: landscape, square, native');
  eq(keys(pmax, 'fixed_display').length, 4, 'Fixed Display: four banner sizes');
  eq(A.KINDS, ['search', 'pmax', 'responsive_display', 'fixed_display']);
  for (const kind of A.KINDS) for (const d of A.previewSet(pmax, kind)) {
    for (const f of ['key', 'label', 'where', 'note', 'what', 'why']) ok(typeof d[f] === 'string' && d[f].length > 3 && d[f].length <= 130, kind + ' ' + d.key + ' has a short plain ' + f);
    ok(d.aspect > 0 && typeof d.render === 'function' && d.kind === kind && d.example === A.NOTE, d.key + ' has an aspect, a renderer and the example label');
    ok(d.campaign.name && d.campaign.icon, d.key + ' names its campaign type');
    ok(d.render('sm') !== d.render('lg') && d.render('lg').includes('data-size="lg"') && d.render().includes('data-size="sm"'), d.key + ' renders a small and a large size');
  }
  eq(new Set(A.KINDS.flatMap(k => keys(pmax, k))).size, 13, 'every format key is unique');
  ok(S.byKey.pmax && S.byKey.responsive_display && S.byKey.fixed_display && S.CHANNELS.SEARCH, 'every kind is a campaign type the console already names');
  eq(A.previewSet(search, 'search')[0].campaign.name, S.describe('SEARCH').name, 'the campaign name comes from brites-campaign-styles');
  eq(A.previewSet(pmax, 'pmax')[0].campaign.name, S.byKey.pmax.name);
  ok(/Shopping/.test(S.byKey.pmax.whenToChoose) && /YouTube/.test(S.byKey.pmax.whenToChoose) && /Display/.test(S.byKey.pmax.whenToChoose) && /Discover/.test(S.byKey.pmax.whenToChoose), 'Performance Max really runs on the surfaces its previews show');
  ok(/native/i.test(S.byKey.responsive_display.whenToChoose), 'Responsive Display really includes native formats');
  const {FIXED_SIZES} = require(root + '/netlify/functions/googleAdsCampaignStyles');
  ok(A.FIXED_SIZES.length === 4 && A.FIXED_SIZES.every(s => FIXED_SIZES.has(s)), 'Fixed Display only shows sizes the publisher accepts');
  A.previewSet(pmax, 'fixed_display').forEach((d, i) => ok(Math.abs(d.aspect - A.FIXED_SIZES[i].split('x')[0] / A.FIXED_SIZES[i].split('x')[1]) < 1e-9, d.key + ' keeps its true proportions'));
  eq(keys({}, 'bogus'), [], 'an unknown campaign type has no previews'); eq(keys(null, 'search').length, 2, 'a missing opportunity still previews');
  eq(keys({ feedLabel: 'US' }, undefined), keys(pmax, 'pmax'), 'a feed opportunity is a Performance Max one when no kind is given');
  eq(keys({}, ''), keys(search, 'search'), 'anything else defaults to Search');
  eq(keys(pmax, 'display'), keys(pmax, 'responsive_display')); eq(keys({ displayStyle: 'fixed_display' }, 'display'), keys(pmax, 'fixed_display'));
  eq(keys(pmax, 'performance_max'), keys(pmax, 'pmax'), 'performance_max is accepted');
  eq(keys(pmax, 'video'), [], 'video and demand gen are not styles this app creates, so nothing is shown for them');

  // 2. The sitelinks and callouts mirror what the publisher really attaches (fails if either side changes).
  const server = fs.readFileSync(root + '/netlify/functions/googleAdsAutopilot.js', 'utf8');
  eq(JSON.parse(server.match(/const BRAND_CALLOUTS = (\[[^\]]*\]);/)[1]), A.CALLOUTS, 'callouts match BRAND_CALLOUTS');
  ok(server.includes('d1: "' + A.SITELINK_LINES.collection[0] + '", d2: "' + A.SITELINK_LINES.collection[1] + '"'), 'collection sitelink lines match buildCampaignAssets');
  ok(server.includes('linkText: "' + A.SITELINK_LINES.bestSellers[0] + '", d1: "' + A.SITELINK_LINES.bestSellers[1] + '", d2: "' + A.SITELINK_LINES.bestSellers[2] + '"'), 'Best Sellers sitelink matches buildCampaignAssets');
  const links = textOf(A.previewSet(search, 'search')[1].render('lg')).textContent;
  ok(links.includes('Shop Charm Bracelets') && links.includes('Best Sellers') && A.CALLOUTS.every(c => links.includes(c)), 'the links preview shows the collection sitelink, Best Sellers and every callout');
  eq([...textOf(A.previewSet({ ...search, collectionHandle: 'best-sellers', collectionTitle: 'Best Sellers' }, 'search')[1].render('lg')).querySelectorAll('.abp-sl-t')].map(n => n.textContent), ['Shop Best Sellers'], 'the Best Sellers ad does not link to its own page twice');
  eq([...textOf(A.previewSet(search, 'search')[1].render('lg')).querySelectorAll('.abp-sl-t')].map(n => n.textContent), ['Shop Charm Bracelets', 'Best Sellers'], 'other Search ads link to the collection and Best Sellers');

  // 3. Every word comes from the opportunity or a fixed label; nothing is invented.
  const TEMPLATE = 'ad sponsored shop at for the browse full collection personalized made to order best sellers our most-loved pieces top customer favorites handcrafted jewelry charms custom-made gifts unique handmade designs collections brites now video only if you add one'.split(' ');
  const allowed = (opp, extra) => { const set = new Set(TEMPLATE); JSON.stringify([opp, extra || []]).toLowerCase().split(/[^a-z0-9'’.-]+/).forEach(w => set.add(w.replace(/^[.'-]+|[.'-]+$/g, ''))); return set; };
  const words = html => textOf(html.replace(/></g, '> <')).textContent.toLowerCase().replace(/[|›·]/g, ' ').split(/\s+/).map(w => w.replace(/^[.,'’-]+|[.,'’:-]+$/g, '')).filter(Boolean);
  for (const opp of [search, { collectionTitle: 'Rings' }, {}, pmax]) for (const kind of ['search', 'pmax', 'responsive_display', 'fixed_display']) for (const d of A.previewSet(opp, kind)) {
    const okWords = allowed(opp, ['britesjewelry.com', 'jewelry']), nums = new Set((JSON.stringify(opp).match(/\d+(\.\d+)?/g) || []).map(Number));
    const stray = words(d.render('lg')).filter(w => /\d/.test(w) ? !nums.has(parseFloat(w.replace(/[^\d.]/g, ''))) : !okWords.has(w) && !okWords.has(w.replace(/\./g, '')) && !/^[a-z]{1,2}$/.test(w));
    eq(stray, [], d.key + ' for ' + JSON.stringify(opp).slice(0, 40) + ' says only what the opportunity says (any number comes from its data)');
  }
  const copy = textOf(A.previewSet(search, 'search')[0].render('lg'));
  ok(copy.querySelector('.abp-sr-head').textContent === 'Custom Name Bracelet for Mom | Charm Bracelets | Mother’s Day', 'headlines are the keyword, the collection and the occasion');
  ok(copy.querySelector('.abp-sr-desc').textContent === 'A gift she will keep. Made just for her. Charm Bracelets for Mother’s Day.', 'descriptions are the researched phrases');
  ok(copy.querySelector('.abp-sr-url span').textContent === 'britesjewelry.com › collections › charm-bracelets' && copy.querySelector('.abp-sr-q').textContent.includes('personalized mom charm bracelet'), 'the green line is the store address and the search box shows the first keyword');
  ok([...copy.querySelectorAll('.abp-sr-head')].every(h => h.textContent.split(' | ').every(t => t.length <= 30)), 'headlines fit 30 characters');
  const bare = textOf(A.previewSet({ collectionTitle: 'Rings' }, 'search')[0].render('lg'));
  eq(bare.querySelector('.abp-sr-head').textContent, 'Rings | Brites Jewelry', 'with only a title the ad says only the title and the store');
  ok(!bare.querySelector('.abp-sr-q'), 'no keyword, no search box'); ok(!/\d|\$/.test(bare.textContent), 'no invented numbers or prices');
  eq(textOf(A.previewSet({ ...search, headlines: ['Real headline one', 'Real headline two'], descriptions: ['Real description.'] }, 'search')[0].render('lg')).querySelector('.abp-sr-head').textContent, 'Real headline one | Real headline two', 'drafted copy, when present, is shown as written');
  eq(textOf(A.previewSet({ ...search, occasion: 'Evergreen gifting' }, 'search')[0].render('lg')).querySelector('.abp-sr-head').textContent.includes('Evergreen'), false, 'an evergreen label is not an occasion');
  eq(A.previewSet({ ...search, store: { name: 'Other Shop', domain: 'other.example' } }, 'search').map(d => textOf(d.render('sm')).textContent).join('').includes('other.example'), true, 'store name and domain can be supplied');
  eq(textOf(A.previewSet({ ...search, store: { domain: 'bad domain/../x' } }, 'search')[0].render('sm')).querySelector('.abp-sr-url span').textContent.startsWith('britesjewelry.com'), true, 'a malformed store domain falls back');

  // Picture ads: price only when the listing states one with a currency.
  const shop = (opp, o) => textOf(A.previewSet(opp, 'pmax', o)[0].render('lg'));
  ok(shop(pmax).querySelector('.abp-shop-price').textContent === '$39.00' && shop(pmax).querySelector('.abp-shop-title').textContent === 'Personalized Birthstone Necklace', 'the hero listing supplies its own title and price');
  ok(!shop({ ...pmax, offerDetails: [{ itemId: 'a', title: 'Charm', price: 24 }] }).querySelector('.abp-shop-price'), 'a price without a currency is left out, never guessed');
  ok(!shop({ collectionTitle: 'Rings', offerDetails: [{ itemId: 'a', title: 'Ring' }] }).querySelector('.abp-shop-price'), 'no price in the data, no price on the card');
  ok(!/[$0-9]/.test(shop({ collectionTitle: 'Rings' }).textContent.replace(/Sponsored/, '')), 'a bare collection shows no price or numbers');
  eq(shop({ offerDetails: [{ title: 'Charm', price: '$24.00' }] }).querySelector('.abp-shop-price').textContent, '$24.00', 'a formatted price is shown as the feed wrote it');
  eq(shop({ offerDetails: [{ title: 'Charm', price: { amount: '18.5', currencyCode: 'CAD' } }] }).querySelector('.abp-shop-price').textContent, 'CA$18.50', 'a Canadian price keeps its currency');
  eq(shop({ offerDetails: [{ title: 'Charm', price: -5, currency: 'USD' }, { title: 'Ring', price: 0, currency: 'USD' }] }).querySelector('.abp-shop-price'), null, 'zero and negative prices are ignored');
  eq(textOf(A.previewSet(pmax, 'pmax')[1].render('lg')).querySelector('.abp-ad-h').textContent, 'A birthstone for every story', 'the researched creative angle is the headline');
  eq(textOf(A.previewSet({ ...pmax, angle: '', research: {} }, 'pmax')[1].render('lg')).querySelector('.abp-ad-h').textContent, 'Personalized Birthstone Necklace', 'without an angle the featured product names the ad');
  eq(textOf(A.previewSet({ collectionTitle: 'Rings' }, 'pmax')[1].render('lg')).querySelector('.abp-ad-h').textContent, 'Rings', 'without either the collection names the ad');
  const video = textOf(A.previewSet(pmax, 'pmax')[3].render('lg'));
  ok(video.textContent === 'Video: made only if you add one' && !video.querySelector('img,video,iframe,source'), 'the video slot is an honest empty placeholder');
  ok(A.previewSet(pmax, 'pmax')[3].hasImage === false && A.previewSet(pmax, 'pmax')[0].hasImage === true, 'descriptors say whether a real photo is used');
  ok(A.previewSet(pmax, 'pmax')[3].why.includes('may build a simple one from your images'), 'the video note matches the campaign settings: Google may make a slideshow');

  // Which listing leads: the shopper’s ticks, then the researcher’s heroes, then any with a photo.
  const three = { offerDetails: [{ itemId: 'a', title: 'Alpha Pendant' }, { itemId: 'b', title: 'Beta Pendant', imageUrl: 'https://cdn.shopify.com/b.jpg' }, { itemId: 'c', title: 'Gamma Pendant', imageUrl: 'https://cdn.shopify.com/c.jpg' }] };
  eq(shop(three).querySelector('.abp-shop-title').textContent, 'Beta Pendant', 'a listing with a photo leads');
  eq(shop(three, { selectedItemIds: ['a'] }).querySelector('.abp-shop-title').textContent, 'Alpha Pendant', 'a ticked product leads even without a photo');
  eq(shop({ ...three, research: { listingFit: [{ itemId: 'c', role: 'hero' }] } }).querySelector('.abp-shop-title').textContent, 'Gamma Pendant', 'a researched hero leads');
  eq(shop({ offerDetails: [{ itemId: 'a', title: 'Same Ring' }, { itemId: 'b', title: 'same ring', imageUrl: 'https://cdn.shopify.com/b.jpg' }] }).querySelector('img').getAttribute('src').startsWith('https://cdn.shopify.com/b.jpg'), true, 'variants of one product share the photo the other has');
  eq(shop({}, { listings: [{ title: 'Given Charm', imageUrl: 'https://cdn.shopify.com/g.jpg' }] }).querySelector('.abp-shop-title').textContent, 'Given Charm', 'listings can be passed in directly');
  eq(shop({ previewListings: [{ title: 'Server Charm', image: { url: 'https://cdn.shopify.com/s.jpg' } }] }).querySelector('img').getAttribute('src').startsWith('https://cdn.shopify.com/s.jpg'), true, 'a server previewListings field with an image object works');

  // 4. Hostile strings are escaped everywhere they can land.
  const evil = 'x" onmouseover="alert(9)\' autofocus onfocus=\'alert(8) <img src=x onerror=alert(1)> & <script>alert(2)</script>';
  const hostile = { collectionTitle: evil, collectionHandle: '"><x', occasion: '"><script>1</script>', keywords: [evil], keyPhrases: [evil], angle: evil, storeName: evil, storeDomain: 'a"><b',
    offerDetails: [{ itemId: '"><i', title: evil, imageUrl: 'https://cdn.shopify.com/a.jpg?x="onerror="alert(3)', price: evil, currency: '"><u' }] };
  for (const kind of A.KINDS) {
    const html = A.stripHtml(hostile, kind), dom = textOf(html);
    ok(!dom.querySelector('script') && !html.includes('<script') && !html.includes('<img src=x'), kind + ': no injected element');
    ok([...dom.querySelectorAll('*')].every(el => [...el.attributes].every(a => !/^on/i.test(a.name))), kind + ': no injected handler attribute');
    ok([...dom.querySelectorAll('img')].every(i => /^https:\/\/cdn\.shopify\.com\//.test(i.getAttribute('src')) && ![...i.attributes].some(a => a.name === 'onerror')), kind + ': images stay on the CDN');
    for (const d of A.previewSet(hostile, kind)) for (const size of ['sm', 'lg']) { const html = d.render(size), dom = textOf(html); ok(!/<script|<img src=x/.test(html) && !dom.querySelector('script') && [...dom.querySelectorAll('*')].every(el => [...el.attributes].every(a => !/^on/i.test(a.name))), d.key + ' ' + size + ' is escaped'); }
  }
  ok(textOf(A.previewSet(hostile, 'search')[0].render('lg')).textContent.includes('onmouseover'), 'hostile text is shown as plain text, not dropped silently');
  ok(!/\son\w+\s*=/i.test(A.KINDS.map(k => A.stripHtml(pmax, k) + A.stripHtml(search, k)).join('')), 'no inline event handlers in any strip');

  // 5. Bad image URLs never become a picture; good ones are sized, lazy and described.
  const bad = ['http://cdn.shopify.com/a.jpg', '//cdn.shopify.com/a.jpg', 'javascript:alert(1)', 'data:image/svg+xml,<svg/>', 'https://evil.example/a.jpg', 'https://cdn.shopify.com.evil.example/a.jpg',
    'https://evilcdn.shopify.com/a.jpg', 'https://user:pw@cdn.shopify.com/a.jpg', 'https://cdn.shopify.com:8443/a.jpg', 'https://britesjewelry.com.evil.example/a.jpg', 'ftp://cdn.shopify.com/a.jpg',
    '', '   ', null, undefined, 42, {}, ['https://cdn.shopify.com/a.jpg'], 'not a url', 'https://cdn.shopify.com/' + 'a'.repeat(2100)];
  for (const u of bad) eq(A.safeImageUrl(u), '', 'refused: ' + String(u).slice(0, 50));
  for (const u of ['https://cdn.shopify.com/s/files/1/a.jpg', 'https://britesjewelry.com/cdn/shop/files/a.jpg', 'https://www.britesjewelry.com/cdn/shop/a.jpg', 'https://CDN.SHOPIFY.COM/a.jpg'])
    ok(A.safeImageUrl(u).startsWith('https://'), 'allowed: ' + u);
  for (const u of ['https://storage.googleapis.com/b/a.jpg', 'https://lh3.googleusercontent.com/a', 'https://bucket.storage.googleapis.com/a.jpg', 'https://evil.britesjewelry.com/a.jpg']) eq(A.safeImageUrl(u), '', 'not a store photo host: ' + u);
  const ctxSrc = fs.readFileSync(root + '/netlify/functions/googleAdsAdDesignContext.js', 'utf8');
  eq(ctxSrc.match(/allowed=image\?\[([^\]]*)\]/)[1].replace(/'/g, '').split(',').sort(), [...A.IMAGE_HOSTS].sort(), 'the photo hosts match the ones Ad Design accepts');
  eq(A.IMAGE_HOSTS.every(h => !/\*|http/.test(h)), true);
  const noPic = A.previewSet({ collectionTitle: 'Stud Earrings', offerDetails: [{ itemId: 'a', title: 'Tiny Gold Stud Earrings', imageUrl: 'http://evil.example/x.jpg', image: 'javascript:1' }] }, 'pmax');
  for (const d of noPic) for (const size of ['sm', 'lg']) ok(!d.render(size).includes('<img') && !d.render(size).includes('evil.example'), d.key + ' shows no broken image for a refused URL');
  ok(noPic[0].hasImage === false && textOf(noPic[0].render('sm')).querySelector('.abp-ph b').textContent === 'TG', 'the placeholder carries the product initials');
  ok(textOf(noPic[0].render('sm')).querySelector('.abp-ph svg'), 'and the shopping-bag type icon from the shared vocabulary');
  eq(textOf(A.previewSet({ offerDetails: [{ title: '12 3 !!' }] }, 'pmax')[0].render('sm')).querySelector('.abp-ph b'), null, 'no letters, no initials');
  const pic = textOf(A.previewSet(pmax, 'pmax')[0].render('sm')).querySelector('img'), big = textOf(A.previewSet(pmax, 'pmax')[0].render('lg')).querySelector('img');
  ok(pic.getAttribute('loading') === 'lazy' && pic.getAttribute('decoding') === 'async' && pic.getAttribute('referrerpolicy') === 'no-referrer' && pic.getAttribute('alt') === 'Photo of Personalized Birthstone Necklace', 'images are lazy, referrer-free and have alt text');
  const url = new URL(pic.getAttribute('src')), urlBig = new URL(big.getAttribute('src'));
  ok(url.searchParams.get('width') === '360' && urlBig.searchParams.get('width') === '900' && !url.searchParams.has('height') && !url.searchParams.has('crop') && url.searchParams.get('v') === '1', 'the CDN is asked for a right-sized, uncropped picture');
  ok(!A.previewSet(search, 'search').some(d => d.render('lg').includes('<img')), 'Search text ads carry no photo');

  // 6. Stepping through a set.
  eq([0, 1, 2].map(i => A.stepIndex(i, 3, 'ArrowRight')), [1, 2, 0], 'Right steps forward and wraps');
  eq([0, 1, 2].map(i => A.stepIndex(i, 3, 'ArrowLeft')), [2, 0, 1], 'Left steps back and wraps');
  eq([A.stepIndex(2, 4, 'Home'), A.stepIndex(1, 4, 'End')], [0, 3], 'Home and End'); eq([A.stepIndex(1, 4, 'next'), A.stepIndex(0, 4, 'prev')], [2, 3], 'button names step too');
  eq([A.stepIndex(1, 4, 'a'), A.stepIndex(1, 4), A.stepIndex(1, 4, '')], [1, 1, 1], 'other keys stay put');
  eq([A.stepIndex(0, 1, 'ArrowRight'), A.stepIndex(0, 1, 'ArrowLeft')], [0, 0], 'a single format stays put');
  eq([A.stepIndex(5, 3, ''), A.stepIndex(-2, 3, ''), A.stepIndex(NaN, 3, 'ArrowRight'), A.stepIndex('2', '3', 'ArrowRight'), A.stepIndex(1, 0, 'ArrowRight'), A.stepIndex(1, -4, 'End')], [2, 0, 1, 0, 0, 0], 'out-of-range input is clamped, never NaN');

  // 7. Source, markup and styles.
  ok(!/\b(alert|confirm|prompt)\s*\(/.test(source), 'no native alert, confirm or prompt');
  ok(!/\b(fetch|XMLHttpRequest|WebSocket|sendBeacon|EventSource|importScripts)\b|new Image\b|\bimport\s*\(|require\(['"](http|https|node-fetch|net)/.test(source), 'no network, no image constructor');
  ok(!/openai|anthropic|gemini|api[_-]?key/i.test(source), 'no AI provider is mentioned or reachable');
  const emoji = /\p{Extended_Pictographic}/u;
  ok(!emoji.test(source) && !emoji.test(A.css), 'no emoji in the module or its styles');
  ok(A.KINDS.every(k => !emoji.test(A.stripHtml(pmax, k)) && A.previewSet(pmax, k).every(d => !emoji.test([d.label, d.where, d.note, d.what, d.why].join(' ')))), 'no emoji in any label or markup');
  ok(!/[—–]/.test(A.KINDS.map(k => A.previewSet(pmax, k).map(d => d.what + d.why + d.note + d.where + d.label).join('')).join('')), 'plain punctuation in user-facing sentences');
  ok(/@media \(prefers-reduced-motion:reduce\)\{[^}]*animation:none/.test(A.css) && /\.abp-well,\.abp-zoom[^{]*\{transition:none/.test(A.css), 'motion is switched off for reduced-motion users');
  ok(/\[data-theme="dark"\] \.abp-modal/.test(A.css) && /--abp-surface:#2b2823/.test(A.css), 'a dark theme is defined for the strip and the dialog');
  ok(/var\(--gold,#a9823f\)/.test(A.css) && /var\(--line,#e4ddd0\)/.test(A.css) && /var\(--card,#fffefb\)/.test(A.css), 'the console tokens lead, with the same values as fallbacks');
  ok(/\.abp\{min-width:0;max-width:100%/.test(A.css) && /\.abp-row\{display:flex;flex-wrap:nowrap[^}]*overflow-x:auto/.test(A.css), 'the strip scrolls sideways inside its card and never widens the page');
  ok(/:focus-visible/.test(A.css) && /\.abp-lock\{overflow:hidden/.test(A.css), 'visible focus and a scroll lock behind the dialog');

  const strip = A.stripHtml(pmax, 'pmax'), first = textOf(strip);
  ok(first.querySelector('.abp[role="group"]') && first.querySelector('.abp-foot').textContent === A.NOTE, 'the strip is a labelled group with the example note');
  const thumbs = [...first.querySelectorAll('button.abp-thumb')];
  eq(thumbs.length, 4); ok(thumbs.every(b => b.type === 'button' && b.getAttribute('aria-label') === 'Inspect ' + b.querySelector('.abp-cap').textContent && b.getAttribute('aria-haspopup') === 'dialog' && b.querySelector('.abp-well')), 'each thumbnail is a button labelled "Inspect <label>"');
  ok(thumbs.every(b => b.querySelector('.abp-where').textContent.length > 3) && thumbs.every(b => b.style.getPropertyValue('--a') > 0), 'each shows its label, where it runs and a fixed aspect');
  eq(A.stripHtml(pmax, 'pmax'), strip, 'the same opportunity draws the same strip'); eq(A.stripHtml({}, 'bogus'), '', 'no formats, no strip');
  ok(!A.stripHtml(pmax, 'pmax', { head: false, foot: false }).includes('abp-head') && !A.stripHtml(pmax, 'pmax', { foot: false }).includes('abp-foot'), 'head and foot can be left out');
  ok(A.thumbHtml(A.previewSet(pmax, 'pmax')[0]).includes('aria-label="Inspect Product listing"') && !A.thumbHtml(A.previewSet(pmax, 'pmax')[0]).includes('data-abp-set'), 'a stand-alone thumbnail works');

  // 8. The dialog, in a DOM.
  const win = new JSDOM('<!doctype html><html lang="en"><head></head><body><main id="page"><div id="host"></div><button id="after">Next control</button></main></body></html>', { runScripts: 'outside-only', pretendToBeVisual: true, url: 'https://example.test' }).window;
  win.eval(fs.readFileSync(root + '/brites-campaign-styles.js', 'utf8')); win.eval(source);
  const D = win.document, P = win.BritesAdPreview;
  ok(P && D.getElementById('abp-css') && D.getElementById('abp-css').textContent === P.css, 'loading in a page injects the styles once');
  P.injectCss(D); P.install(D); eq(D.querySelectorAll('#abp-css').length, 1, 'injecting again changes nothing');
  D.getElementById('host').innerHTML = P.stripHtml(pmax, 'pmax');
  const btn = i => D.querySelectorAll('.abp-thumb')[i], dlg = () => D.querySelector('[role="dialog"]'), press = (key, o) => D.dispatchEvent(new win.KeyboardEvent('keydown', { key, bubbles: true, cancelable: true, ...o }));
  const title = () => D.querySelector('.abp-dtitle').textContent;
  eq(P.isOpen(), false); eq(dlg(), null, 'nothing opens by itself');
  btn(0).focus(); btn(0).click();
  ok(P.isOpen() && dlg(), 'clicking a thumbnail opens the dialog');
  const dl = dlg();
  ok(dl.getAttribute('aria-modal') === 'true' && dl.getAttribute('role') === 'dialog' && dl.getAttribute('tabindex') === '-1', 'role dialog, aria-modal');
  ok(D.getElementById(dl.getAttribute('aria-labelledby')).textContent === 'Product listing' && D.getElementById(dl.getAttribute('aria-describedby')).textContent.includes('What this is'), 'named by its title and described by its facts');
  eq([...dl.querySelectorAll('dt')].map(n => n.textContent), ['What this is', 'Where it shows', 'Why this campaign type uses it'], 'three plain lines');
  eq([...dl.querySelectorAll('dd')].map(n => n.textContent), [A.previewSet(pmax, 'pmax')[0].what, 'On Google Shopping and Search', A.previewSet(pmax, 'pmax')[0].why], 'and their text');
  ok(dl.querySelector('.abp-dnote').textContent === A.NOTE && dl.querySelector('.abp-kind').textContent === 'Performance Max' && dl.querySelector('.abp-kind svg'), 'the example note, and the campaign type with its icon');
  ok(dl.querySelector('[data-abp-stage] .abp-mock[data-size="lg"]') && D.documentElement.classList.contains('abp-lock'), 'the large preview is shown and the page behind is locked');
  ok(dl.parentNode.parentNode === D.body && D.body.lastElementChild.classList.contains('abp-modal'), 'the dialog sits at the end of the body');
  ok(D.activeElement === dl.querySelector('.abp-x') && dl.contains(D.activeElement), 'focus moves into the dialog, on the close button');
  eq(dl.querySelector('.abp-count').textContent, 'Product listing, 1 of 4'); ok(dl.querySelector('.abp-count').getAttribute('aria-live') === 'polite', 'the position is announced');
  ok(dl.querySelector('[data-abp-step="prev"]').getAttribute('aria-label') === 'Previous format' && dl.querySelector('.abp-x').getAttribute('aria-label') === 'Close preview', 'controls have names');
  // focus trap
  const f = [...dl.querySelectorAll('button')]; eq(f.length, 3);
  f[2].focus(); press('Tab'); ok(D.activeElement === f[0], 'Tab from the last control wraps to the first');
  press('Tab', { shiftKey: true }); ok(D.activeElement === f[2], 'Shift+Tab from the first wraps to the last');
  D.getElementById('after').focus(); ok(dl.contains(D.activeElement), 'focus cannot leave for the page behind');
  // stepping
  f[0].focus(); press('ArrowRight'); eq(title(), 'Website ad'); ok(D.activeElement === f[0] && D.body.contains(f[0]), 'Right arrow steps on and focus stays put');
  eq(dl.querySelector('.abp-count').textContent, 'Website ad, 2 of 4'); eq(dl.querySelector('dd[data-abp-where]').textContent, 'On websites and apps');
  ok(dl.querySelector('[data-abp-stage] .abp-ad'), 'the preview changes with it');
  press('ArrowLeft'); press('ArrowLeft'); eq(title(), 'Video', 'Left arrow wraps to the last format'); press('Home'); eq(title(), 'Product listing'); press('End'); eq(title(), 'Video');
  dl.querySelector('[data-abp-step="next"]').click(); eq(title(), 'Product listing', 'the Next button wraps too'); dl.querySelector('[data-abp-step="prev"]').click(); eq(title(), 'Video');
  eq(D.querySelectorAll('[role="dialog"]').length, 1, 'stepping never stacks dialogs');
  // Esc, focus return
  const esc = new win.KeyboardEvent('keydown', { key: 'Escape', bubbles: true, cancelable: true }); D.dispatchEvent(esc);
  ok(esc.defaultPrevented && !P.isOpen() && dlg() === null, 'Esc closes and is not passed on to the page');
  ok(D.activeElement === btn(3), 'focus returns to the thumbnail that is showing (the last one we stepped to)');
  ok(!D.documentElement.classList.contains('abp-lock') && D.documentElement.style.paddingRight === '', 'the page unlocks');
  press('ArrowRight'); eq(dlg(), null, 'arrow keys do nothing once closed');
  // backdrop, close button, re-render while open
  btn(1).focus(); btn(1).click(); eq(title(), 'Website ad', 'opens on the format that was clicked');
  D.querySelector('.abp-backdrop').click(); ok(dlg() === null && D.activeElement === btn(1), 'a backdrop click closes and returns focus');
  btn(2).click(); dlg().querySelector('.abp-x').click(); ok(dlg() === null && D.activeElement === btn(2), 'the close button closes and returns focus');
  btn(2).click(); dlg().querySelector('.abp-dtitle').click(); ok(dlg() !== null, 'clicking inside the dialog does not close it'); P.close();
  btn(0).click(); D.getElementById('host').innerHTML = P.stripHtml(pmax, 'pmax'); press('Escape');
  ok(D.activeElement === D.querySelector('.abp-thumb'), 'if the card was redrawn while open, focus lands on the new thumbnail');
  eq(P.close(), false, 'closing twice is harmless');
  btn(0).click(); btn(1).click(); eq(D.querySelectorAll('[role="dialog"]').length, 1, 'opening another replaces the first'); P.close();
  // the same strip drawn twice: focus goes back to the one that was used
  D.getElementById('host').innerHTML = P.stripHtml(pmax, 'pmax') + P.stripHtml(pmax, 'pmax');
  const twin = D.querySelectorAll('.abp-thumb')[5]; twin.click(); press('ArrowRight'); press('Escape');
  ok(D.activeElement === D.querySelectorAll('.abp-thumb')[6], 'with two identical strips, focus returns to the format on show in the strip that was used');
  D.getElementById('host').innerHTML = P.stripHtml(pmax, 'pmax');
  // a single-format set has no stepping; the search set does
  const one = P.previewSet(pmax, 'pmax').slice(0, 1); P.open(one[0], { set: one, index: 0 }); ok(!dlg().querySelector('.abp-nav') && dlg().querySelectorAll('button').length === 1, 'one format, no stepping controls'); P.close();
  eq(P.open(null), null, 'nothing to open'); eq(P.open({}), null);
  D.getElementById('host').innerHTML = P.stripHtml(search, 'search'); btn(0).click(); press('ArrowRight'); eq(title(), 'With extra links'); ok(dlg().querySelector('.abp-kind').textContent === 'Search · text ads', 'Search names its own type'); P.close();
  // names from the shared vocabulary are escaped too
  const realDescribe = win.BritesCampaignStyles.describe; win.BritesCampaignStyles.describe = () => ({ known: true, name: '<b>Bold</b> type', icon: 'pmax', accent: 'red;x' });
  const odd = P.previewSet(pmax, 'pmax'); win.BritesCampaignStyles.describe = realDescribe; P.open(odd[0], { set: odd, index: 0 });
  ok(!dlg().querySelector('.abp-kind b') && dlg().querySelector('.abp-kind').textContent === '<b>Bold</b> type' && dlg().querySelector('.abp-kind').style.getPropertyValue('--abp-accent') === '#8a8a8a', 'a campaign name is shown as text and an odd accent colour is ignored'); P.close();
  // theme follows the page
  D.documentElement.setAttribute('data-theme', 'dark'); btn(0).click(); eq(D.querySelector('.abp-modal').getAttribute('data-theme'), 'dark', 'the dialog follows the page theme'); P.close(); D.documentElement.removeAttribute('data-theme');
  // inside a modal <dialog> it must sit in the top layer with it
  const native = D.createElement('dialog'); native.setAttribute('open', ''); native.matches = function (s) { return s === ':modal' ? true : win.Element.prototype.matches.call(this, s); }; D.body.appendChild(native);
  native.innerHTML = '<div id="in">' + P.stripHtml(pmax, 'pmax') + '</div>'; native.querySelector('.abp-thumb').click();
  ok(native.querySelector('.abp-modal [role="dialog"]'), 'opened from inside a modal dialog it is drawn inside that dialog, above it'); P.close(); native.remove();
  // broken pictures fall back to the placeholder
  D.getElementById('host').innerHTML = P.stripHtml(pmax, 'pmax'); const img = D.querySelector('.abp-thumb img'); img.dispatchEvent(new win.Event('error'));
  ok(img.closest('.abp-img').classList.contains('abp-broken') && /\.abp-broken \.abp-photo\{display:none/.test(P.css), 'a picture that fails to load is hidden, revealing the placeholder');
  // animation path: leaves quickly, then is removed
  win.matchMedia = () => ({ matches: false }); btn(0).click(); P.close(); ok(D.querySelector('.abp-modal.is-leaving') && !P.isOpen(), 'with motion allowed the dialog fades out'); await new Promise(r => setTimeout(r, 260)); eq(D.querySelector('.abp-modal'), null, 'and is then removed');
  win.matchMedia = () => ({ matches: true }); btn(0).click(); P.close(); eq(D.querySelector('.abp-modal'), null, 'with reduced motion it is removed at once');
  win.close();

  console.log('PASS ' + checks + ' ad preview thumbnails: formats per campaign type, wording from data only, escaping, image safety, accessible dialog and stepping');
  require('./suite-guard.cjs').done();
})().catch(e => { console.error(e); process.exit(1); });
