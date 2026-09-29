/* Free, honest previews of the ads a campaign type would run, for the Opportunities tab.

   Nothing has been generated yet at the opportunity stage (generation costs money), so nothing here
   calls a network, an AI model or an image generator. Every word and picture comes from the
   opportunity itself: its collection title, keywords, researched wording and real listing photos.
   Where the opportunity supplies no photo, price or wording, the preview leaves it out or shows a
   neutral placeholder. It never invents a price or a claim. Every preview is labelled as an example.

   Browser global BritesAdPreview; the same file loads in node for tests.

   Formats come only from what each campaign type in brites-campaign-styles.js really delivers:
     search              text ad, and the same ad with the sitelinks and callouts the publisher adds
     pmax                product listing, website ad, feed card, video slot (never generated)
     responsive_display  landscape, square, native card
     fixed_display       four of the Google-supported banner sizes (FIXED_SIZES)
   Sitelink and callout wording mirrors buildCampaignAssets in googleAdsAutopilot.js; the test suite
   fails if the two drift apart. */
(function (root, factory) {
  var api = factory(root);
  if (typeof module === 'object' && module.exports) module.exports = api; else root.BritesAdPreview = api;
  if (typeof document !== 'undefined' && root.document === document) api.install(document);
})(typeof globalThis !== 'undefined' ? globalThis : this, function (root) {
  'use strict';

  var NOTE = 'Example layout. The final wording and images are made when you create the draft.';
  var KINDS = ['search', 'pmax', 'responsive_display', 'fixed_display'];
  var STORE = { name: 'Brites Jewelry', domain: 'britesjewelry.com' };

  // Only the store's own image hosts: the Shopify CDN that feeds Merchant Center, and the storefront. This is
  // the same list the Ad Design context builder accepts for product photos (sourceUrl in
  // googleAdsAdDesignContext.js); anything else gets the neutral placeholder.
  var IMAGE_HOSTS = ['cdn.shopify.com', 'britesjewelry.com', 'www.britesjewelry.com'];

  // What buildCampaignAssets (googleAdsAutopilot.js) attaches to every Search campaign it publishes.
  var SITELINK_LINES = { collection: ['Browse the full collection', 'Personalized, made to order'], bestSellers: ['Best Sellers', 'Our most-loved pieces', 'Top customer favorites'] };
  var CALLOUTS = ['Handcrafted Jewelry', 'Personalized Charms', 'Custom-Made Gifts', 'Unique Handmade Designs'];

  // Banner sizes the publisher accepts for Fixed Display (FIXED_SIZES in googleAdsCampaignStyles.js).
  var FIXED = [
    { w: 300, h: 250, name: 'Rectangle', layout: 'top', img: 56, kf: .9 },
    { w: 970, h: 250, name: 'Billboard', layout: 'side', img: 30, kf: .7 },
    { w: 300, h: 600, name: 'Half page', layout: 'top', img: 58, kf: 1.4 },
    { w: 320, h: 100, name: 'Mobile banner', layout: 'side', img: 30, kf: .72 }
  ];

  /* ---------- small helpers ---------- */
  function esc(v) { return String(v == null ? '' : v).replace(/[&<>"']/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]; }); }
  function list(v) { return Array.isArray(v) ? v : []; }
  function num(v) { var n = typeof v === 'string' ? parseFloat(v.replace(/,/g, '')) : Number(v); return isFinite(n) ? n : null; }
  function wordClip(s, n) {
    if (s.length <= n) return s;
    var cut = s.slice(0, n + 1).replace(/\s+\S*$/, '').replace(/[\s&,·:;\-–—|]+$/, '');
    return cut || s.slice(0, n);
  }
  var CONTROL = new RegExp('[\\u0000-\\u001f\\u007f-\\u009f\\u200b-\\u200f\\u2028\\u2029\\ufeff]', 'g');
  // Plain text from untrusted data: strings and finite numbers only, control characters removed.
  function txt(v, max) {
    var s = typeof v === 'string' ? v : (typeof v === 'number' && isFinite(v) ? String(v) : '');
    s = s.replace(CONTROL, ' ').replace(/\s+/g, ' ').trim();
    return max ? wordClip(s, max) : s;
  }
  function textOf(v, max) { return txt(v && typeof v === 'object' ? (v.text || v.keyword || v.t || v.title || '') : v, max); }
  var SMALL = { a: 1, an: 1, and: 1, as: 1, at: 1, by: 1, for: 1, in: 1, of: 1, on: 1, or: 1, the: 1, to: 1, with: 1 };
  function titleCase(s) {
    return String(s || '').split(' ').map(function (w, i) {
      if (!w) return w;
      if (/[A-Z]/.test(w.slice(1))) return w;
      return i > 0 && SMALL[w.toLowerCase()] ? w.toLowerCase() : w.charAt(0).toUpperCase() + w.slice(1);
    }).join(' ');
  }
  function sentence(s) {
    s = txt(s);
    if (!s) return '';
    s = s.charAt(0).toUpperCase() + s.slice(1);
    return /[.!?]$/.test(s) ? s : s + '.';
  }
  function initials(title) {
    var w = String(title || '').split(/[^A-Za-z0-9]+/).filter(function (x) { return /^[A-Za-z]/.test(x); });
    return w.slice(0, 2).map(function (x) { return x.charAt(0).toUpperCase(); }).join('');
  }
  function norm(s) { return String(s || '').toLowerCase().replace(/['\u2019]/g, '').replace(/[^a-z0-9]+/g, ' ').trim(); }
  function hash(s) {
    var h = 2166136261;
    for (var i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 16777619); }
    return (h >>> 0).toString(36);
  }

  /* ---------- image safety ---------- */
  function hostOk(host) { return IMAGE_HOSTS.indexOf(String(host || '').toLowerCase()) >= 0; }
  function safeImageUrl(v) {
    if (typeof v !== 'string') return '';
    var s = v.trim();
    if (!s || s.length > 2048) return '';
    var u;
    try { u = new URL(s); } catch (e) { return ''; }
    if (u.protocol !== 'https:' || u.username || u.password || (u.port && u.port !== '443') || !hostOk(u.hostname)) return '';
    return u.href;
  }
  // Shopify's CDN resizes on request; ask for about twice the displayed width, uncropped.
  function sizedImageUrl(url, width) {
    try {
      var u = new URL(url);
      if (u.hostname === 'cdn.shopify.com' || /^(www\.)?britesjewelry\.com$/.test(u.hostname)) {
        u.searchParams.set('width', String(width)); u.searchParams.delete('height'); u.searchParams.delete('crop');
      }
      return u.href;
    } catch (e) { return url; }
  }
  function imageOf(x) {
    var c = [x.imageUrl, x.image_link, x.imageLink, x.productImageUri, x.image, x.featuredImage, x.hero, x.crops && (x.crops.square || x.crops.tile), list(x.images)[0]];
    for (var i = 0; i < c.length; i++) {
      var v = c[i];
      if (v && typeof v === 'object') v = v.url || v.src;
      var ok = safeImageUrl(v);
      if (ok) return ok;
    }
    return '';
  }

  /* ---------- shared vocabulary (brites-campaign-styles.js) ---------- */
  function styles() {
    if (root.BritesCampaignStyles) return root.BritesCampaignStyles;
    try { if (typeof require === 'function') return require('../brites-campaign-styles'); } catch (e) { /* stand-alone */ }
    return null;
  }
  var FALLBACK_ICON = {
    search: '<circle cx="8.6" cy="8.6" r="5.4"/><path d="M12.6 12.6 17.4 17.4"/>',
    shopping: '<path d="M4.2 6.4h11.6l-1 9.2a1.4 1.4 0 0 1-1.4 1.2H6.6a1.4 1.4 0 0 1-1.4-1.2z"/><path d="M7.4 6.4V5a2.6 2.6 0 0 1 5.2 0v1.4"/>',
    pmax: '<circle cx="10" cy="10" r="2.6"/><path d="M10 1.6v3M10 15.4v3M1.6 10h3M15.4 10h3"/>',
    responsive_display: '<rect x="2.4" y="5.4" width="15.2" height="9.2" rx="1.4"/>',
    fixed_display: '<rect x="2.4" y="4.2" width="15.2" height="11.6" rx="1.4"/><path d="M6.6 8.4h6.8M6.6 11.6h4.2"/>'
  };
  function icon(name, px) {
    var s = styles();
    if (s && s.iconSvg && s.ICONS && s.ICONS[name]) return s.iconSvg(name, px);
    return '<svg class="campaignIcon" viewBox="0 0 20 20" width="' + px + '" height="' + px + '" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" focusable="false">' + (FALLBACK_ICON[name] || FALLBACK_ICON.responsive_display) + '</svg>';
  }
  function campaignOf(kind) {
    var s = styles(), d = s && s.describe ? s.describe(kind) : null;
    if (d && d.known) return { key: kind, name: d.name, icon: d.icon, accent: d.accent };
    return { key: kind, name: { search: 'Search · text ads', pmax: 'Performance Max', responsive_display: 'Responsive Display', fixed_display: 'Fixed Display' }[kind] || kind, icon: kind, accent: '#8a8a8a' };
  }
  function glyph(name, px) {
    var p = {
      play: '<circle cx="12" cy="12" r="10"/><path d="M10 8.4v7.2l6-3.6z"/>',
      close: '<path d="M5 5l10 10M15 5 5 15"/>',
      prev: '<path d="M12.5 4.5 7 10l5.5 5.5"/>',
      next: '<path d="M7.5 4.5 13 10l-5.5 5.5"/>',
      zoom: '<circle cx="8.6" cy="8.6" r="5.4"/><path d="M12.6 12.6 17.4 17.4M8.6 6.5v4.2M6.5 8.6h4.2"/>',
      search: '<circle cx="8.6" cy="8.6" r="5.4"/><path d="M12.6 12.6 17.4 17.4"/>'
    }[name];
    var vb = name === 'play' ? '0 0 24 24' : '0 0 20 20';
    return '<svg viewBox="' + vb + '" width="' + px + '" height="' + px + '" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" focusable="false">' + p + '</svg>';
  }

  /* ---------- reading the opportunity ---------- */
  function normKind(kind, opp) {
    var k = String(kind || '').toLowerCase().replace(/[\s-]+/g, '_');
    if (k === 'display') k = opp && opp.displayStyle === 'fixed_display' ? 'fixed_display' : 'responsive_display';
    if (k === 'product' || k === 'product_ads' || k === 'shopping' || k === 'performance_max') k = 'pmax';
    if (!k && opp) k = opp.kind || (opp.schedule && opp.schedule.kind) || (opp.offerDetails || opp.feedLabel ? 'pmax' : 'search');
    if (k === 'display') k = 'responsive_display';
    return KINDS.indexOf(k) >= 0 ? k : '';
  }
  function storeOf(opp, opts) {
    var s = (opts && opts.store) || (opp && opp.store) || {};
    var domain = txt(s.domain || (opp && opp.storeDomain), 80).toLowerCase().replace(/^https?:\/\//, '').replace(/\/.*$/, '');
    return {
      name: txt(s.name || (opp && opp.storeName), 40) || STORE.name,
      domain: /^[a-z0-9]([a-z0-9.-]*[a-z0-9])?$/.test(domain) ? domain : STORE.domain
    };
  }
  function keywordsOf(opp) {
    var out = [], seen = {};
    function add(v) { var t = textOf(v, 80).toLowerCase(); if (t && !seen[t]) { seen[t] = 1; out.push(t); } }
    [opp.keywords, opp.keywordData, opp.searchThemes, opp.research && opp.research.keywords].forEach(function (src) {
      if (!out.length) list(src).forEach(add);
    });
    return out.slice(0, 12);
  }
  function occasionOf(opp) {
    var o = txt(opp.occasion || (opp.schedule && opp.schedule.event && opp.schedule.event.label), 40);
    return /^evergreen/i.test(o) ? '' : o;
  }
  function priceOf(x) {
    var p = x.price, cur = x.currency || x.currencyCode, amount = null;
    if (p && typeof p === 'object') { cur = cur || p.currency || p.currencyCode; amount = num(p.amount != null ? p.amount : p.value); }
    else if (typeof p === 'string' && /[^\d\s.,]/.test(p) && /\d/.test(p)) return txt(p, 16);
    else amount = num(p != null ? p : x.priceAmount);
    cur = String(cur || '').toUpperCase();
    if (!(amount > 0) || !/^[A-Z]{3}$/.test(cur)) return '';
    try { return new Intl.NumberFormat('en-US', { style: 'currency', currency: cur }).format(amount); } catch (e) { return ''; }
  }
  // Real listings first (the ones the researcher marked as heroes or the shopper ticked), then ones
  // that have a photo, so the preview shows something real when the opportunity carries it.
  function listingsOf(opp, opts) {
    var pool = [], byTitle = {}, out = [];
    [opts && opts.listings, opp.previewListings, opp.offerDetails, opp.products, opp.topProducts].forEach(function (src) {
      list(src).forEach(function (x) { if (x && typeof x === 'object') pool.push(x); });
    });
    pool.forEach(function (x) {
      var title = txt(x.productTitle || x.title || x.name, 120), key = norm(title);
      if (!key) return;
      var id = x.itemId != null ? String(x.itemId) : x.id != null ? String(x.id) : '', image = imageOf(x), price = priceOf(x), at = byTitle[key];
      if (at) { if (id) at.ids.push(id); if (!at.image && image) at.image = image; if (!at.price && price) at.price = price; return; }
      byTitle[key] = { title: title, image: image, price: price, ids: id ? [id] : [] };
      out.push(byTitle[key]);
    });
    var chosen = {}, hero = {};
    list(opts && opts.selectedItemIds).forEach(function (id) { chosen[String(id)] = 1; });
    list(opp.research && opp.research.listingFit).forEach(function (f) { if (f && f.role === 'hero' && f.itemId != null) hero[String(f.itemId)] = 1; });
    var rank = function (l) {
      var has = function (m) { return l.ids.some(function (i) { return m[i]; }); };
      return (has(chosen) ? 0 : 4) + (has(hero) ? 0 : 2) + (l.image ? 0 : 1);
    };
    return out.map(function (l, i) { return { l: l, i: i, r: rank(l) }; }).sort(function (a, b) { return a.r - b.r || a.i - b.i; }).map(function (x) { return x.l; });
  }

  // Search wording: the real drafted copy when the opportunity has it, otherwise built only from the
  // opportunity's own keyword, collection, occasion and researched phrases.
  function searchCopy(opp, store) {
    var kws = keywordsOf(opp), coll = txt(opp.collectionTitle || opp.title, 60), occ = occasionOf(opp);
    var supplied = function (src, max) { return list(src).map(function (x) { return textOf(x, max); }).filter(Boolean); };
    var copy = opp.adCopy || {}, heads = supplied(opp.headlines || copy.headlines, 30), descs = supplied(opp.descriptions || copy.descriptions, 90);
    if (!heads.length) {
      var seen = [];
      [(kws.map(titleCase).filter(function (k) { return k.length <= 30; })[0] || ''), wordClip(coll, 30), wordClip(occ, 30), store.name].forEach(function (h) {
        var n = norm(h);
        if (n && !seen.some(function (s) { return s === n || s.indexOf(n) >= 0 || n.indexOf(s) >= 0; })) { seen.push(n); heads.push(h); }
      });
      heads = heads.slice(0, 3);
    }
    if (!descs.length) {
      var phrases = list(opp.keyPhrases).map(function (p) { return txt(p, 60); }).filter(Boolean);
      var shop = coll ? 'Shop ' + coll + ' at ' + store.name + '.' : 'Shop at ' + store.name + '.';
      descs = [phrases.length ? wordClip(phrases.slice(0, 2).map(sentence).join(' '), 90) : shop];
      if (occ && coll) descs.push(wordClip(coll + ' for ' + occ + '.', 90));
      else if (phrases.length && coll) descs.push(shop);
    }
    var handle = String(opp.collectionHandle || opp.handle || '');
    return {
      heads: heads.slice(0, 3), descs: descs.slice(0, 2), query: kws[0] ? wordClip(kws[0], 40) : '',
      handle: /^[a-z0-9][a-z0-9-]{0,80}$/i.test(handle) ? handle : '', coll: coll
    };
  }
  // Wording for picture ads: the researched creative angle, else the featured product, else the collection.
  function pictureCopy(opp, hero, store) {
    var angle = txt(opp.angle || list(opp.research && opp.research.creativeAngles)[0], 90);
    var coll = txt(opp.collectionTitle || opp.title, 60);
    return { headline: angle || (hero && hero.title ? wordClip(hero.title, 60) : '') || coll || store.name, brand: store.name };
  }

  function context(opp, kind, opts) {
    opp = opp && typeof opp === 'object' ? opp : {};
    var store = storeOf(opp, opts), listings = kind === 'search' ? [] : listingsOf(opp, opts), hero = listings[0] || null;
    var ctx = { kind: kind, store: store, hero: hero, listings: listings };
    if (kind === 'search') { ctx.search = searchCopy(opp, store); ctx.callouts = list(opp.callouts).length ? list(opp.callouts).map(function (c) { return txt(c, 25); }).filter(Boolean).slice(0, 4) : CALLOUTS; }
    else {
      ctx.pic = pictureCopy(opp, hero, store);
      var title = hero ? hero.title : txt(opp.collectionTitle || opp.title, 60);
      ctx.card = { title: title, price: hero ? hero.price : '', image: hero ? hero.image : '' };
      ctx.initials = initials(title);
    }
    return ctx;
  }

  /* ---------- the mock-ups ----------
     Every mock is plain spans, so it can sit inside a button. Type sizes are in container units of the
     mock itself (cqw), so one markup serves the thumbnail and the large view. */
  // The large view keeps type near a constant on-screen size whatever the mock's width: --ksl for the
  // desktop stage (about 600 x 324), --ksp for a phone stage (about 300 x 260).
  function largeScale(a, wide, boost) { return Math.round(Math.min(1, Math.max(.4, 225 * (boost || 1) / Math.min(wide, (wide === 600 ? 340 : 280) * a))) * 100) / 100; }
  function mock(cls, aspect, size, kf, inner, extra, boost) {
    var a = Number(aspect);
    return '<span class="abp-mock ' + cls + '" data-size="' + esc(size) + '" style="--a:' + aspect + ';--kf:' + kf + ';--ksl:' + largeScale(a, 600, boost) + ';--ksp:' + largeScale(a, 300, boost) + (extra ? ';' + extra : '') + '">' + inner + '</span>';
  }
  function pic(ctx, size, cls) {
    var img = ctx.card && ctx.card.image ? '<img class="abp-photo" src="' + esc(sizedImageUrl(ctx.card.image, size === 'lg' ? 900 : 360)) + '" alt="' + esc('Photo of ' + ctx.card.title) + '" loading="lazy" decoding="async" referrerpolicy="no-referrer">' : '';
    return '<span class="abp-img ' + (cls || '') + '"><span class="abp-ph" aria-hidden="true">' + icon('shopping', 16) + (ctx.initials ? '<b>' + esc(ctx.initials) + '</b>' : '') + '</span>' + img + '</span>';
  }
  function searchUrl(ctx) {
    var s = ctx.search;
    return '<span class="abp-sr-url"><b>Ad</b><span>' + esc(ctx.store.domain + (s.handle ? ' › collections › ' + s.handle : '')) + '</span></span>';
  }
  function searchHead(ctx) { return '<span class="abp-sr-head">' + esc(ctx.search.heads.join(' | ')) + '</span>'; }
  function rSearchText(ctx, size) {
    var s = ctx.search;
    return mock('abp-search', size === 'lg' ? 2.3 : 1.6, size, 1,
      (s.query ? '<span class="abp-sr-q">' + glyph('search', 12) + '<span>' + esc(s.query) + '</span></span>' : '') +
      searchUrl(ctx) + searchHead(ctx) + '<span class="abp-sr-desc">' + esc(s.descs.join(' ')) + '</span>', '', 1.2);
  }
  function rSearchLinks(ctx, size) {
    var s = ctx.search, coll = s.coll ? wordClip('Shop ' + s.coll, 25) : 'Shop the collection';
    var links = [[coll, SITELINK_LINES.collection[0], SITELINK_LINES.collection[1]]];
    if (s.handle !== 'best-sellers') links.push(SITELINK_LINES.bestSellers);
    return mock('abp-search abp-links', size === 'lg' ? 2.1 : 1.6, size, .92,
      searchUrl(ctx) + searchHead(ctx) +
      '<span class="abp-sr-links">' + links.map(function (l) {
        return '<span class="abp-sl"><span class="abp-sl-t">' + esc(l[0]) + '</span><span class="abp-sl-d">' + esc(l[1]) + '</span><span class="abp-sl-d">' + esc(l[2]) + '</span></span>';
      }).join('') + '</span>' +
      '<span class="abp-callouts">' + ctx.callouts.map(function (c) { return '<span>' + esc(c) + '</span>'; }).join('') + '</span>', '', 1.2);
  }
  function rShopping(ctx, size) {
    var c = ctx.card;
    return mock('abp-shop', .7, size, 1,
      pic(ctx, size, 'abp-shop-img') +
      '<span class="abp-shop-body">' + (c.price ? '<span class="abp-shop-price">' + esc(c.price) + '</span>' : '') +
      '<span class="abp-shop-title">' + esc(c.title) + '</span><span class="abp-shop-store">' + esc(ctx.store.name) + '</span><span class="abp-shop-spons">Sponsored</span></span>');
  }
  // One picture-ad layout serves the website tile, square, banners and native card.
  function tile(ctx, size, o) {
    return mock('abp-ad abp-' + o.layout + (o.cls ? ' ' + o.cls : ''), o.aspect, size, o.kf || 1,
      pic(ctx, size) + '<span class="abp-ad-body"><span class="abp-ad-h">' + esc(ctx.pic.headline) + '</span>' +
      '<span class="abp-ad-row"><span class="abp-ad-brand">' + esc(ctx.pic.brand) + '</span>' + (o.button === false ? '' : '<span class="abp-btn">Shop now</span>') + '</span></span>',
      (o.img ? '--abp-img:' + o.img + '%;' : '') + '--hl:' + (size === 'lg' ? 2 : o.hl || 2));
  }
  function rFeed(ctx, size) {
    return mock('abp-feed', 1.15, size, 1,
      pic(ctx, size, 'abp-feed-img') +
      '<span class="abp-feed-body"><span class="abp-feed-src"><i aria-hidden="true">' + esc(ctx.store.name.charAt(0).toUpperCase()) + '</i><span>Sponsored · ' + esc(ctx.store.name) + '</span></span>' +
      '<span class="abp-feed-h">' + esc(ctx.pic.headline) + '</span></span>');
  }
  function rVideo(ctx, size) {
    return mock('abp-video', 16 / 9, size, 1,
      '<span class="abp-video-in">' + glyph('play', 24) + '<span class="abp-video-t">Video: made only if you add one</span></span>');
  }

  /* ---------- the catalogue ---------- */
  var ON_SITES = 'On websites and apps';
  var WHY_RD = 'Responsive Display lets Google fit your photos and words to almost any space, so the final look varies.';
  function fixedFormats() {
    return FIXED.map(function (f) {
      return {
        key: 'fixed-' + f.w + 'x' + f.h, label: f.name + ' ' + f.w + '×' + f.h, where: ON_SITES, aspect: f.w / f.h,
        note: 'Your finished banner is shown exactly as approved.',
        what: 'A ' + f.name.toLowerCase() + ' banner, ' + f.w + ' by ' + f.h + ' pixels, with your photo, headline and store name. It runs only in spaces of exactly this size.',
        why: 'Fixed Display keeps your finished design unchanged in each size, with no text rotation or animation.',
        render: function (ctx, size) { return tile(ctx, size, { layout: f.layout, aspect: (f.w / f.h).toFixed(3), kf: f.kf, cls: 'abp-fixed', img: f.img }); }
      };
    });
  }
  var FORMATS = {
    search: [
      { key: 'search-text', label: 'Text ad', where: 'On Google Search results', aspect: 1.6,
        note: 'Shown when someone searches for words you chose.',
        what: 'A text-only ad in the search results: blue headlines, your web address in green and two lines of description.',
        why: 'Search ads appear when someone types what you sell, so the words do the work.', render: rSearchText },
      { key: 'search-links', label: 'With extra links', where: 'Under the ad on Google Search', aspect: 1.6,
        note: 'Search campaigns made here add links to store pages and short highlights.',
        what: 'The same text ad with links to other store pages and short highlights underneath.',
        why: 'The links give the ad more room and more places to click. Search campaigns made here add them.', render: rSearchLinks }
    ],
    pmax: [
      { key: 'pmax-shopping', label: 'Product listing', where: 'On Google Shopping and Search', aspect: .7,
        note: 'Built from the photo, title and price in your product feed.',
        what: 'A product card with the photo, title, price and store name from your Merchant Center feed.',
        why: 'Performance Max shows the exact products you choose, so people see the item before they click.', render: rShopping },
      { key: 'pmax-display', label: 'Website ad', where: ON_SITES, aspect: 1.91,
        note: 'Google fits your photo, headline and Shop now button to each space.',
        what: 'A flexible ad with a photo, a headline and a Shop now button. Google changes its shape to fit each space.',
        why: 'Performance Max reuses your photos and words on websites and apps, so you do not need a separate Display campaign.',
        render: function (ctx, size) { return tile(ctx, size, { layout: 'top', aspect: '1.91', kf: .8, hl: 1 }); } },
      { key: 'pmax-feed', label: 'Feed card', where: 'In the Google app feed and Gmail', aspect: 1.15,
        note: 'A large photo with a short headline in a feed people scroll.',
        what: 'A card in a scrolling feed: a large photo, a short headline and your store name.',
        why: 'Performance Max also reaches people who are browsing rather than searching, using the same photos and headlines.', render: rFeed },
      { key: 'pmax-video', label: 'Video', where: 'On YouTube', aspect: 16 / 9,
        note: 'Used only if you add a video.',
        what: 'A video slot. Nothing is made for it here, so it stays empty unless you add your own video.',
        why: 'Performance Max can run on YouTube. Without your video, Google may build a simple one from your images.', render: rVideo }
    ],
    responsive_display: [
      { key: 'rd-landscape', label: 'Landscape ad', where: ON_SITES, aspect: 1.91,
        note: 'Google fits your photos, text and logo to the space.',
        what: 'A wide ad with a photo, a headline and a Shop now button.', why: WHY_RD,
        render: function (ctx, size) { return tile(ctx, size, { layout: 'top', aspect: '1.91', kf: .8, hl: 1 }); } },
      { key: 'rd-square', label: 'Square ad', where: ON_SITES, aspect: 1,
        note: 'The same photo and words in a boxy space.',
        what: 'A square ad with the same photo, headline and button, for boxy spaces.', why: WHY_RD,
        render: function (ctx, size) { return tile(ctx, size, { layout: 'top', aspect: '1', kf: .95, cls: 'abp-square' }); } },
      { key: 'rd-native', label: 'Native card', where: 'In feeds and articles', aspect: 2.4,
        note: 'A small card that blends into the page around it.',
        what: 'A small card that blends into a page or app feed, with a photo, a headline and your store name.',
        why: 'Responsive Display includes native spaces, which a fixed banner cannot fill.',
        render: function (ctx, size) { return tile(ctx, size, { layout: 'side', aspect: '2.4', kf: .8, cls: 'abp-native', button: false, img: 34 }); } }
    ],
    fixed_display: fixedFormats()
  };

  /* ---------- public: previewSet ---------- */
  function previewSet(opp, kind, opts) {
    var k = normKind(kind, opp);
    if (!k) return [];
    var ctx = context(opp, k, opts), camp = campaignOf(k), hasImage = !!(ctx.card && ctx.card.image);
    return FORMATS[k].map(function (f) {
      return {
        key: f.key, label: f.label, where: f.where, note: f.note, what: f.what, why: f.why, aspect: f.aspect, kind: k, campaign: camp,
        hasImage: hasImage && f.key !== 'pmax-video' && k !== 'search', example: NOTE,
        render: function (size) { return f.render(ctx, size === 'lg' ? 'lg' : 'sm'); }
      };
    });
  }

  /* ---------- public: thumbnails and the strip ---------- */
  var SETS = {}, SET_IDS = [];
  function register(set) {
    var id = 'abp' + hash(set.map(function (d) { return d.key + '\u0001' + d.render('sm'); }).join('\u0002'));
    if (!SETS[id]) { SET_IDS.push(id); if (SET_IDS.length > 400) delete SETS[SET_IDS.shift()]; }
    SETS[id] = set;
    return id;
  }
  function thumbHtml(d, o) {
    o = o || {};
    var a = Number(d.aspect) > 0 ? Number(d.aspect) : 1.6;
    return '<button type="button" class="abp-thumb" aria-label="' + esc('Inspect ' + d.label) + '" aria-haspopup="dialog" data-abp-key="' + esc(d.key) + '"' +
      (o.setId ? ' data-abp-set="' + esc(o.setId) + '" data-abp-index="' + (o.index | 0) + '"' : '') + ' style="--a:' + a.toFixed(3) + '" title="' + esc(d.where) + '">' +
      '<span class="abp-well">' + d.render('sm') + '<span class="abp-zoom">' + glyph('zoom', 12) + '</span></span>' +
      '<span class="abp-cap">' + esc(d.label) + '</span><span class="abp-where">' + esc(d.where) + '</span></button>';
  }
  function stripHtml(opp, kind, opts) {
    opts = opts || {};
    var set = previewSet(opp, kind, opts);
    if (!set.length) return '';
    var id = register(set), camp = set[0].campaign;
    return '<div class="abp" data-abp-kind="' + esc(set[0].kind) + '" data-abp-set="' + esc(id) + '" role="group" aria-label="' + esc('Ad designs for ' + camp.name) + '">' +
      (opts.head === false ? '' : '<div class="abp-head"><span class="abp-title">How the ads would look</span><span class="abp-hint">Select one to enlarge</span></div>') +
      '<div class="abp-row">' + set.map(function (d, i) { return thumbHtml(d, { setId: id, index: i }); }).join('') + '</div>' +
      (opts.foot === false ? '' : '<p class="abp-foot">' + esc(NOTE) + '</p>') + '</div>';
  }

  /* ---------- public: the inspect dialog ---------- */
  function stepIndex(index, count, key) {
    var n = Math.max(0, Math.floor(Number(count)) || 0), i = Math.floor(Number(index)) || 0;
    if (n < 1) return 0;
    i = Math.min(n - 1, Math.max(0, i));
    if (key === 'ArrowRight' || key === 'Right' || key === 'next') return (i + 1) % n;
    if (key === 'ArrowLeft' || key === 'Left' || key === 'prev') return (i - 1 + n) % n;
    if (key === 'Home') return 0;
    if (key === 'End') return n - 1;
    return i;
  }
  var current = null, uid = 0;
  function reducedMotion() { try { return !root.matchMedia || root.matchMedia('(prefers-reduced-motion: reduce)').matches; } catch (e) { return true; } }
  function hostFor(doc) {
    try {
      var open = doc.querySelectorAll('dialog[open]'), modal = null;
      for (var i = 0; i < open.length; i++) if (open[i].matches(':modal')) modal = open[i];
      if (modal) return modal;
    } catch (e) { /* no :modal support */ }
    return doc.body;
  }
  function focusables(el) {
    return Array.prototype.slice.call(el.querySelectorAll('button:not([disabled]):not([hidden]),[href],[tabindex]:not([tabindex="-1"])'));
  }
  // The dialog is built once; stepping only swaps its parts, so keyboard focus stays where it is.
  function shell(id, many) {
    return '<div class="abp-dhead"><span class="abp-kind"></span><button type="button" class="abp-x" data-abp-close aria-label="Close preview">' + glyph('close', 18) + '</button></div>' +
      '<h2 class="abp-dtitle" id="' + id + '-t" data-abp-title></h2>' +
      '<div class="abp-stage" data-abp-stage></div>' +
      '<p class="abp-dnote">' + esc(NOTE) + '</p>' +
      '<dl class="abp-facts" id="' + id + '-d"><div><dt>What this is</dt><dd data-abp-what></dd></div><div><dt>Where it shows</dt><dd data-abp-where></dd></div>' +
      '<div><dt>Why this campaign type uses it</dt><dd data-abp-why></dd></div></dl>' +
      (many ? '<div class="abp-nav"><button type="button" class="abp-step" data-abp-step="prev" aria-label="Previous format">' + glyph('prev', 16) + '<span>Previous</span></button>' +
        '<span class="abp-count" data-abp-count aria-live="polite"></span>' +
        '<button type="button" class="abp-step" data-abp-step="next" aria-label="Next format"><span>Next</span>' + glyph('next', 16) + '</button></div>' : '');
  }
  function onKey(e) {
    if (!current) return;
    var k = e.key;
    if (k === 'Escape' || k === 'Esc') { e.preventDefault(); e.stopPropagation(); close(); return; }
    if (k === 'Tab') {
      var f = focusables(current.panel);
      if (!f.length) { e.preventDefault(); current.panel.focus(); return; }
      var doc = current.doc, first = f[0], last = f[f.length - 1], at = doc.activeElement;
      if (e.shiftKey && (at === first || at === current.panel || !current.panel.contains(at))) { e.preventDefault(); last.focus(); }
      else if (!e.shiftKey && (at === last || !current.panel.contains(at))) { e.preventDefault(); first.focus(); }
      return;
    }
    if (current.set.length > 1 && (k === 'ArrowLeft' || k === 'ArrowRight' || k === 'Home' || k === 'End')) {
      e.preventDefault(); e.stopPropagation(); show(stepIndex(current.index, current.set.length, k), true);
    }
  }
  function onFocusIn(e) {
    if (current && !current.panel.contains(e.target)) { var f = focusables(current.panel); (f[0] || current.panel).focus(); }
  }
  function show(index, animate) {
    var c = current; if (!c) return;
    c.index = index;
    var P = c.panel, d = c.set[index], camp = d.campaign || {}, kind = P.querySelector('.abp-kind'), stage = P.querySelector('[data-abp-stage]');
    var put = function (sel, text) { var n = P.querySelector(sel); if (n) n.textContent = text; };
    kind.style.setProperty('--abp-accent', /^#[0-9a-f]{3,8}$/i.test(camp.accent || '') ? camp.accent : '#8a8a8a');
    kind.innerHTML = icon(camp.icon || 'unknown', 15) + '<span>' + esc(camp.name || '') + '</span>';
    put('[data-abp-title]', d.label); put('[data-abp-what]', d.what); put('[data-abp-where]', d.where); put('[data-abp-why]', d.why);
    put('[data-abp-count]', d.label + ', ' + (index + 1) + ' of ' + c.set.length);
    stage.innerHTML = d.render('lg');
    if (animate && !reducedMotion()) { stage.classList.remove('abp-swap'); void stage.offsetWidth; stage.classList.add('abp-swap'); }
  }
  function open(d, o) {
    o = o || {};
    if (!d || typeof d.render !== 'function') return null;
    var doc = (o.opener && o.opener.ownerDocument) || root.document;
    if (!doc || !doc.body) return null;
    injectCss(doc);
    var set = list(o.set).length ? o.set : [d], index = stepIndex(o.index != null ? o.index : set.indexOf(d), set.length, ''), opener = o.opener || doc.activeElement;
    if (current) { current.el.parentNode && current.el.parentNode.removeChild(current.el); teardown(false); }
    var id = 'abpd' + (++uid), el = doc.createElement('div');
    el.className = 'abp-modal';
    var theme = (opener && opener.closest && opener.closest('[data-theme]') || doc.documentElement).getAttribute('data-theme');
    if (theme) el.setAttribute('data-theme', theme);
    el.innerHTML = '<div class="abp-backdrop" data-abp-close></div><div class="abp-dialog" role="dialog" aria-modal="true" aria-labelledby="' + id + '-t" aria-describedby="' + id + '-d" tabindex="-1">' + shell(id, set.length > 1) + '</div>';
    var panel = el.querySelector('.abp-dialog');
    current = { el: el, panel: panel, doc: doc, set: set, index: index, id: id, opener: opener, setId: o.setId || (opener && opener.getAttribute && opener.getAttribute('data-abp-set')) || '' };
    show(index, false);
    hostFor(doc).appendChild(el);
    el.addEventListener('click', function (e) {
      if (!current || current.el !== el) return;
      var t = e.target.closest ? e.target.closest('[data-abp-close],[data-abp-step]') : null;
      if (!t) return;
      if (t.hasAttribute('data-abp-close')) close();
      else show(stepIndex(current.index, current.set.length, t.getAttribute('data-abp-step')), true);
    });
    doc.addEventListener('keydown', onKey, true);
    doc.addEventListener('focusin', onFocusIn);
    lock(doc);
    var x = panel.querySelector('.abp-x'); (x || panel).focus();
    return panel;
  }
  function lock(doc) {
    var html = doc.documentElement, gap = html.clientWidth > 0 && root.innerWidth ? root.innerWidth - html.clientWidth : 0;
    current.padding = html.style.paddingRight;
    html.classList.add('abp-lock');
    if (gap > 0) html.style.paddingRight = gap + 'px';
  }
  function teardown(restoreFocus) {
    var c = current; if (!c) return;
    current = null;
    c.doc.removeEventListener('keydown', onKey, true);
    c.doc.removeEventListener('focusin', onFocusIn);
    c.doc.documentElement.classList.remove('abp-lock');
    c.doc.documentElement.style.paddingRight = c.padding || '';
    if (restoreFocus) {
      // The thumbnail of the format on show: in the strip it was opened from, else wherever that strip is drawn now.
      var d = c.set[c.index], op = c.opener, back = null, find = function (scope) {
        try { return scope.querySelector('.abp-thumb[data-abp-set="' + c.setId + '"][data-abp-key="' + d.key + '"]'); } catch (e) { return null; }
      };
      if (op && op.isConnected !== false && op.getAttribute && op.getAttribute('data-abp-key') === d.key) back = op;
      else if (c.setId && d) {
        var strip = op && op.closest && op.closest('.abp');
        back = strip && strip.isConnected !== false && find(strip) || find(c.doc);
      }
      if (!back && op && op.isConnected !== false) back = op;
      if (back && back.focus) back.focus();
    }
    return c;
  }
  function close() {
    var c = teardown(true); if (!c) return false;
    if (reducedMotion()) { if (c.el.parentNode) c.el.parentNode.removeChild(c.el); }
    else { c.el.classList.add('is-leaving'); setTimeout(function () { if (c.el.parentNode) c.el.parentNode.removeChild(c.el); }, 160); }
    return true;
  }
  function isOpen() { return !!current; }

  /* ---------- page wiring ---------- */
  function onClick(e) {
    var b = e.target && e.target.closest ? e.target.closest('.abp-thumb[data-abp-set]') : null;
    if (!b) return;
    var set = SETS[b.getAttribute('data-abp-set')], i = parseInt(b.getAttribute('data-abp-index'), 10);
    if (!set || !set[i]) return;
    e.preventDefault();
    open(set[i], { set: set, index: i, opener: b, setId: b.getAttribute('data-abp-set') });
  }
  function onImgError(e) {
    var t = e.target;
    if (t && t.tagName === 'IMG' && t.classList && t.classList.contains('abp-photo') && t.closest) { var w = t.closest('.abp-img'); if (w) w.classList.add('abp-broken'); }
  }
  function injectCss(doc) {
    doc = doc || root.document;
    if (!doc || !doc.head || doc.getElementById('abp-css')) return;
    var s = doc.createElement('style'); s.id = 'abp-css'; s.textContent = css; doc.head.appendChild(s);
  }
  function install(doc) {
    doc = doc || root.document;
    if (!doc || doc.__abpInstalled) return;
    doc.__abpInstalled = true;
    if (doc.head) injectCss(doc); else doc.addEventListener('DOMContentLoaded', function () { injectCss(doc); });
    doc.addEventListener('click', onClick);
    doc.addEventListener('error', onImgError, true);
  }

  /* ---------- styles ---------- */
  var css = [
    '.abp,.abp-modal{--abp-card:var(--card,#fffefb);--abp-card2:var(--card-2,#faf7f1);--abp-ink:var(--ink,#1c1a17);--abp-ink70:var(--ink-70,#5b554c);--abp-ink45:var(--ink-45,#938c80);--abp-line:var(--line,#e4ddd0);--abp-line2:var(--line-2,#efe9dd);--abp-gold:var(--gold,#a9823f);--abp-goldsoft:var(--gold-soft,#f0e6cd);--abp-goldline:var(--gold-line,#e3d3a6);--abp-well:var(--paper-2,#ece6da);--abp-sans:var(--sans,-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,system-ui,sans-serif);--abp-serif:var(--serif,"Hoefler Text","Iowan Old Style",Georgia,serif);',
    '--abp-surface:#fff;--abp-sink:#202124;--abp-smute:#5f6368;--abp-mline:#e3e0da;--abp-link:#1a0dab;--abp-url:#188038;--abp-btn:#2b2520;--abp-btn-ink:#fff;--abp-ph1:#efe7d8;--abp-ph2:#e2d6bf;--abp-ph-ink:#8d7a55;font-family:var(--abp-sans)}',
    '[data-theme="dark"] .abp,[data-theme="dark"] .abp-modal,.abp[data-theme="dark"],.abp-modal[data-theme="dark"]{--abp-card:#25221d;--abp-card2:#2b2822;--abp-ink:#efe9de;--abp-ink70:#c3bbab;--abp-ink45:#968d7d;--abp-line:#3b362f;--abp-line2:#312d27;--abp-gold:#caa861;--abp-goldsoft:#3a3122;--abp-goldline:#5a4a2b;--abp-well:#1f1c18;--abp-surface:#2b2823;--abp-sink:#e8eaed;--abp-smute:#a4a9ae;--abp-mline:#413c35;--abp-link:#8ab4f8;--abp-url:#81c995;--abp-btn:#e9dcc0;--abp-btn-ink:#2a2114;--abp-ph1:#39332a;--abp-ph2:#2e2921;--abp-ph-ink:#a08c60}',
    '.abp *,.abp-modal *{box-sizing:border-box}',

    /* the strip */
    '.abp{min-width:0;max-width:100%;margin:14px 0 0;color:var(--abp-ink)}',
    '.abp-head{display:flex;align-items:baseline;justify-content:space-between;flex-wrap:wrap;gap:2px 12px;margin:0 0 9px}',
    '.abp-title{font:700 10.5px/1.4 var(--abp-sans);letter-spacing:.16em;text-transform:uppercase;color:var(--abp-ink45)}',
    '.abp-hint{font:500 11px/1.4 var(--abp-sans);color:var(--abp-ink45)}',
    '.abp-row{display:flex;flex-wrap:nowrap;align-items:flex-start;gap:14px;overflow-x:auto;overscroll-behavior-x:contain;scroll-snap-type:x proximity;padding:3px 3px 7px;margin:0 -3px;scrollbar-width:thin}',
    '.abp-thumb{--h:108px;flex:none;display:flex;flex-direction:column;align-items:stretch;gap:7px;width:clamp(116px,calc(var(--h) * var(--a)),184px);margin:0;padding:0;border:0;background:none;color:inherit;font:inherit;text-align:left;cursor:pointer;scroll-snap-align:start;border-radius:12px;-webkit-tap-highlight-color:transparent}',
    '.abp-well{position:relative;display:grid;place-items:center;width:100%;height:var(--h);container-type:size;overflow:hidden;border:1px solid var(--abp-line);border-radius:11px;background:var(--abp-well);transition:border-color .16s ease,box-shadow .16s ease,transform .16s ease}',
    '.abp-thumb:hover .abp-well{border-color:var(--abp-goldline);box-shadow:0 6px 16px rgba(30,26,20,.09);transform:translateY(-1px)}',
    '.abp-thumb:active .abp-well{transform:none}',
    '.abp-thumb:focus-visible{outline:2px solid var(--abp-gold);outline-offset:3px}',
    '.abp-zoom{position:absolute;right:5px;top:5px;display:grid;place-items:center;width:20px;height:20px;border-radius:50%;background:var(--abp-card);color:var(--abp-ink70);border:1px solid var(--abp-line);opacity:0;transition:opacity .16s ease}',
    '.abp-thumb:hover .abp-zoom,.abp-thumb:focus-visible .abp-zoom{opacity:1}',
    '@media (hover:none){.abp-zoom{opacity:.85}}',
    '.abp-cap{font:600 12px/1.3 var(--abp-sans);color:var(--abp-ink);overflow-wrap:anywhere}',
    '.abp-where{margin-top:-4px;font:500 11px/1.35 var(--abp-sans);color:var(--abp-ink45);overflow-wrap:anywhere}',
    '.abp-foot{margin:6px 0 0;font:11.5px/1.5 var(--abp-sans);color:var(--abp-ink45)}',

    /* mock-ups */
    '.abp-mock{--ks:1;--k:calc(var(--kf,1) * var(--ks));position:relative;display:flex;flex-direction:column;width:min(100cqw,calc(var(--fit,100cqh) * var(--a)));aspect-ratio:var(--a);container-type:inline-size;overflow:hidden;background:var(--abp-surface);color:var(--abp-sink);border-radius:6px;box-shadow:0 0 0 1px var(--abp-mline);font-family:Arial,Roboto,"Helvetica Neue",sans-serif;text-align:left}',
    '.abp-mock[data-size="lg"]{--ks:var(--ksl,.44);--fit:var(--stage-h);border-radius:10px}',
    '@container abpstage (max-width:520px){.abp-mock[data-size="lg"]{--ks:var(--ksp,.7)}}',
    '.abp-mock>*{min-width:0}',

    /* search */
    '.abp-search[data-size="lg"]{aspect-ratio:auto;width:min(100cqw,640px)}',
    '.abp-search{justify-content:center;gap:calc(var(--k) * 2.4cqw);padding:calc(var(--k) * 4.6cqw) calc(var(--k) * 5.4cqw)}',
    '.abp-sr-q{display:flex;align-items:center;gap:calc(var(--k) * 2cqw);align-self:flex-start;max-width:100%;padding:calc(var(--k) * 1.4cqw) calc(var(--k) * 3.4cqw);border:1px solid var(--abp-mline);border-radius:999px;font-size:calc(var(--k) * 4.6cqw);line-height:1.2;white-space:nowrap;overflow:hidden}',
    '.abp-sr-q svg{flex:none;width:calc(var(--k) * 4.8cqw);height:calc(var(--k) * 4.8cqw);color:var(--abp-smute)}',
    '.abp-sr-q span{overflow:hidden;text-overflow:ellipsis}',
    '.abp-sr-url{display:flex;align-items:baseline;gap:calc(var(--k) * 1.8cqw);font-size:calc(var(--k) * 4.6cqw);line-height:1.25;white-space:nowrap;overflow:hidden}',
    '.abp-sr-url b{flex:none;font-weight:700;color:var(--abp-sink)}',
    '.abp-sr-url span{overflow:hidden;text-overflow:ellipsis;color:var(--abp-url)}',
    '.abp-sr-head,.abp-sr-desc,.abp-ad-h,.abp-shop-title,.abp-feed-h,.abp-sl-t,.abp-sl-d{display:-webkit-box;-webkit-box-orient:vertical;overflow:hidden;overflow-wrap:anywhere}',
    '.abp-sr-head{-webkit-line-clamp:2;font-size:calc(var(--k) * 7.6cqw);line-height:1.2;color:var(--abp-link)}',
    '.abp-sr-desc{-webkit-line-clamp:2;font-size:calc(var(--k) * 4.6cqw);line-height:1.35;color:var(--abp-smute)}',
    '.abp-mock[data-size="lg"] .abp-sr-desc{-webkit-line-clamp:3}',
    '.abp-links[data-size="sm"] .abp-sr-head{-webkit-line-clamp:1}',
    '.abp-sr-links{display:grid;grid-template-columns:1fr 1fr;gap:calc(var(--k) * 3cqw)}',
    '.abp-sl{display:flex;flex-direction:column;gap:calc(var(--k) * .6cqw);min-width:0}',
    '.abp-sl-t{-webkit-line-clamp:1;font-size:calc(var(--k) * 5.2cqw);line-height:1.25;color:var(--abp-link)}',
    '.abp-sl-d{-webkit-line-clamp:1;font-size:calc(var(--k) * 4.2cqw);line-height:1.3;color:var(--abp-smute)}',
    '.abp-mock[data-size="sm"] .abp-sl-d{display:none}',
    '.abp-callouts{display:flex;flex-wrap:wrap;gap:0 calc(var(--k) * 2.6cqw);font-size:calc(var(--k) * 4.2cqw);line-height:1.35;color:var(--abp-smute)}',
    '.abp-callouts span+span::before{content:"\\00b7";margin-right:calc(var(--k) * 2.6cqw)}',
    '.abp-mock[data-size="sm"] .abp-callouts span:nth-child(n+3){display:none}',

    /* pictures and placeholders */
    '.abp-img{position:relative;display:block;overflow:hidden;background:var(--abp-ph1)}',
    '.abp-ph{position:absolute;inset:0;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:calc(var(--k) * 1.6cqw);background:linear-gradient(145deg,var(--abp-ph1),var(--abp-ph2));color:var(--abp-ph-ink)}',
    '.abp-ph svg{width:calc(var(--k) * 8cqw);height:calc(var(--k) * 8cqw);opacity:.75}',
    '.abp-ph b{font:500 calc(var(--k) * 10cqw)/1 var(--abp-serif);letter-spacing:.06em;opacity:.8}',
    '.abp-photo{position:absolute;inset:0;display:block;width:100%;height:100%;object-fit:cover}',
    '.abp-broken .abp-photo{display:none}',

    /* product listing */
    '.abp-shop-img{flex:none;width:100%;aspect-ratio:1}',
    '.abp-shop-body{flex:1;display:flex;flex-direction:column;gap:calc(var(--k) * 1cqw);min-height:0;padding:calc(var(--k) * 3.6cqw) calc(var(--k) * 4.6cqw) calc(var(--k) * 3cqw);overflow:hidden}',
    '.abp-shop-price{font-size:calc(var(--k) * 7.6cqw);font-weight:700;line-height:1.1;color:var(--abp-sink)}',
    '.abp-shop-title{-webkit-line-clamp:2;font-size:calc(var(--k) * 6cqw);line-height:1.22;color:var(--abp-sink)}',
    '.abp-shop-store,.abp-shop-spons{font-size:calc(var(--k) * 5cqw);line-height:1.25;color:var(--abp-smute);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}',
    '.abp-mock[data-size="sm"] .abp-shop-spons{display:none}',

    /* website ad, square, banners, native card */
    '.abp-ad.abp-top{display:grid;grid-template-rows:var(--abp-img,56%) minmax(0,1fr)}',
    '.abp-ad.abp-side{display:grid;grid-template-columns:var(--abp-img,36%) minmax(0,1fr)}',
    '.abp-ad .abp-img{min-height:0}',
    '.abp-ad-body{display:flex;flex-direction:column;justify-content:space-between;gap:calc(var(--k) * 2cqw);min-height:0;padding:calc(var(--k) * 3.6cqw) calc(var(--k) * 4.2cqw);overflow:hidden}',
    '.abp-ad-h{-webkit-line-clamp:var(--hl,2);font-size:calc(var(--k) * 6.6cqw);font-weight:700;line-height:1.16;color:var(--abp-sink)}',
    '.abp-ad-row{display:flex;align-items:center;justify-content:space-between;gap:calc(var(--k) * 2.4cqw)}',
    '.abp-ad-brand{min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:calc(var(--k) * 4.6cqw);color:var(--abp-smute)}',
    '.abp-btn{flex:none;padding:calc(var(--k) * 1.5cqw) calc(var(--k) * 3.6cqw);border-radius:999px;background:var(--abp-btn);color:var(--abp-btn-ink);font-size:calc(var(--k) * 4.6cqw);font-weight:700;line-height:1.2;white-space:nowrap}',
    '.abp-side .abp-ad-body{justify-content:center}',
    '.abp-native .abp-ad-brand{font-size:calc(var(--k) * 5.6cqw)}',
    '.abp-native .abp-ad-h{font-size:calc(var(--k) * 8.6cqw)}',
    '.abp-fixed.abp-top .abp-ad-body{justify-content:center;gap:calc(var(--k) * 3cqw)}',

    /* feed card */
    '.abp-feed-img{flex:none;width:100%;aspect-ratio:1.91}',
    '.abp-feed-body{flex:1;display:flex;flex-direction:column;gap:calc(var(--k) * 2cqw);min-height:0;padding:calc(var(--k) * 3.4cqw) calc(var(--k) * 4.2cqw);overflow:hidden}',
    '.abp-feed-src{display:flex;align-items:center;gap:calc(var(--k) * 2cqw);min-width:0;font-size:calc(var(--k) * 4.2cqw);line-height:1.2;color:var(--abp-smute)}',
    '.abp-feed-src i{flex:none;display:grid;place-items:center;width:calc(var(--k) * 6.4cqw);height:calc(var(--k) * 6.4cqw);border-radius:50%;background:var(--abp-btn);color:var(--abp-btn-ink);font:700 calc(var(--k) * 3.6cqw)/1 Arial,sans-serif}',
    '.abp-feed-src span{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}',
    '.abp-feed-h{-webkit-line-clamp:2;font-size:calc(var(--k) * 6.6cqw);font-weight:700;line-height:1.2;color:var(--abp-sink)}',

    /* video placeholder: honest about being empty */
    '.abp-video{justify-content:center;align-items:center;padding:calc(var(--k) * 3cqw);background:var(--abp-well)}',
    '.abp-video-in{display:flex;flex-direction:column;align-items:center;justify-content:center;gap:calc(var(--k) * 2.6cqw);width:100%;height:100%;border:1.5px dashed var(--abp-ph-ink);border-radius:calc(var(--k) * 4cqw);color:var(--abp-ph-ink);text-align:center;padding:0 calc(var(--k) * 4cqw)}',
    '.abp-video-in svg{width:calc(var(--k) * 12cqw);height:calc(var(--k) * 12cqw);opacity:.8}',
    '.abp-video-t{font-size:calc(var(--k) * 5cqw);line-height:1.25;font-weight:600}',

    /* the dialog */
    '.abp-lock{overflow:hidden!important}',
    '.abp-modal{position:fixed;inset:0;z-index:2147481000;display:grid;place-items:center;padding:16px;animation:abp-fade .16s ease-out}',
    '.abp-modal.is-leaving{animation:abp-fade-out .14s ease-in forwards;pointer-events:none}',
    '.abp-backdrop{position:absolute;inset:0;background:rgba(28,26,23,.54)}',
    '.abp-dialog{position:relative;display:flex;flex-direction:column;width:min(680px,100%);max-height:calc(100vh - 32px);max-height:calc(100dvh - 32px);overflow:auto;padding:18px 22px 20px;background:var(--abp-card);color:var(--abp-ink);border:1px solid var(--abp-line);border-radius:16px;box-shadow:0 24px 70px rgba(30,26,20,.3);animation:abp-in .2s ease-out;outline:none}',
    '.abp-dhead{display:flex;align-items:center;justify-content:space-between;gap:12px}',
    '.abp-kind{display:inline-flex;align-items:center;gap:7px;padding:3px 10px 3px 8px;border-radius:999px;background:var(--abp-goldsoft);color:var(--abp-ink70);font:600 11.5px/1.5 var(--abp-sans)}',
    '.abp-kind svg{flex:none;color:var(--abp-accent,var(--abp-gold))}',
    '.abp-x{display:grid;place-items:center;flex:none;width:34px;height:34px;margin:-4px -8px -4px 0;border:1px solid transparent;border-radius:50%;background:none;color:var(--abp-ink70);cursor:pointer;transition:background .15s ease,border-color .15s ease}',
    '.abp-x:hover{background:var(--abp-card2);border-color:var(--abp-line)}',
    '.abp-dtitle{margin:10px 0 12px;font:500 24px/1.25 var(--abp-serif);letter-spacing:.01em;color:var(--abp-ink)}',
    '.abp-stage{--stage-h:min(50vh,340px);container:abpstage/inline-size;display:grid;place-items:center;padding:18px;border:1px solid var(--abp-line);border-radius:12px;background:var(--abp-well)}',
    '.abp-stage.abp-swap,.abp-swap{animation:abp-swap .18s ease-out}',
    '.abp-dnote{margin:9px 0 0;font:italic 12px/1.5 var(--abp-sans);color:var(--abp-ink45)}',
    '.abp-facts{display:grid;gap:0;margin:14px 0 0;padding:0}',
    '.abp-facts>div{display:grid;grid-template-columns:150px minmax(0,1fr);gap:4px 16px;padding:10px 0;border-top:1px solid var(--abp-line2)}',
    '.abp-facts dt{font:700 10.5px/1.6 var(--abp-sans);letter-spacing:.12em;text-transform:uppercase;color:var(--abp-ink45)}',
    '.abp-facts dd{margin:0;font:13px/1.55 var(--abp-sans);color:var(--abp-ink)}',
    '.abp-nav{display:flex;align-items:center;justify-content:space-between;gap:10px;margin-top:6px;padding-top:14px;border-top:1px solid var(--abp-line2)}',
    '.abp-count{flex:1;text-align:center;font:500 12px/1.4 var(--abp-mono,ui-monospace,Menlo,Consolas,monospace);color:var(--abp-ink45)}',
    '.abp-step{display:inline-flex;align-items:center;gap:6px;padding:7px 12px;border:1px solid var(--abp-line);border-radius:9px;background:none;color:var(--abp-ink);font:600 12.5px/1.2 var(--abp-sans);cursor:pointer;transition:background .15s ease}',
    '.abp-step:hover{background:var(--abp-card2)}',
    '.abp-x:focus-visible,.abp-step:focus-visible{outline:2px solid var(--abp-gold);outline-offset:2px}',
    '@media (max-width:560px){.abp-modal{padding:10px}.abp-dialog{padding:14px 16px 16px}.abp-dtitle{font-size:21px}.abp-stage{--stage-h:min(44vh,280px);padding:12px}.abp-facts>div{grid-template-columns:1fr}.abp-step span{display:none}.abp-step{padding:8px 12px}}',
    '@keyframes abp-fade{from{opacity:0}to{opacity:1}}',
    '@keyframes abp-fade-out{to{opacity:0}}',
    '@keyframes abp-in{from{opacity:0;transform:translateY(10px)}to{opacity:1;transform:none}}',
    '@keyframes abp-swap{from{opacity:.25;transform:translateX(6px)}to{opacity:1;transform:none}}',
    '@media (prefers-reduced-motion:reduce){.abp-modal,.abp-modal.is-leaving,.abp-dialog,.abp-swap{animation:none}.abp-well,.abp-zoom,.abp-x,.abp-step{transition:none}.abp-thumb:hover .abp-well{transform:none}}'
  ].join('\n');

  return {
    previewSet: previewSet, thumbHtml: thumbHtml, stripHtml: stripHtml, open: open, close: close, isOpen: isOpen, stepIndex: stepIndex,
    safeImageUrl: safeImageUrl, IMAGE_HOSTS: IMAGE_HOSTS, KINDS: KINDS, NOTE: NOTE, CALLOUTS: CALLOUTS, SITELINK_LINES: SITELINK_LINES, FIXED_SIZES: FIXED.map(function (f) { return f.w + 'x' + f.h; }),
    css: css, injectCss: injectCss, install: install
  };
});
