/* One vocabulary for campaign styles: what each is called, the icon that marks
   it, what a sensible daily budget looks like, and the settings this
   application fixes on the operator's behalf.

   Server and browser read this same file, so a campaign cannot be described one
   way in the console and another in a report. Every autoSettings line states
   something the publishing code actually sets; nothing here is aspirational. */
(function (root) {
  'use strict';

  // 20×20, stroke-based, currentColor — legible at 14px beside a label and in
  // either theme without a second asset.
  const ICONS = {
    pmax: '<circle cx="10" cy="10" r="2.6"/><path d="M10 1.6v3M10 15.4v3M1.6 10h3M15.4 10h3M4.1 4.1l2.1 2.1M13.8 13.8l2.1 2.1M15.9 4.1l-2.1 2.1M6.2 13.8l-2.1 2.1"/>',
    search: '<circle cx="8.6" cy="8.6" r="5.4"/><path d="M12.6 12.6 17.4 17.4"/>',
    fixed_display: '<rect x="2.4" y="4.2" width="15.2" height="11.6" rx="1.4"/><path d="M6.6 8.4h6.8M6.6 11.6h4.2"/>',
    responsive_display: '<rect x="2.4" y="5.4" width="15.2" height="9.2" rx="1.4"/><path d="M5.6 2.8 3 5.4l2.6 2.6M14.4 17.2 17 14.6 14.4 12"/>',
    video: '<rect x="2.2" y="4.4" width="15.6" height="11.2" rx="1.8"/><path d="M8.4 7.9 13 10l-4.6 2.1z"/>',
    demand_gen: '<rect x="3" y="2.6" width="14" height="6.4" rx="1.2"/><rect x="3" y="11" width="14" height="6.4" rx="1.2"/><path d="M8.8 4.3 11.6 5.8 8.8 7.3z"/>',
    shopping: '<path d="M4.2 6.4h11.6l-1 9.2a1.4 1.4 0 0 1-1.4 1.2H6.6a1.4 1.4 0 0 1-1.4-1.2z"/><path d="M7.4 6.4V5a2.6 2.6 0 0 1 5.2 0v1.4"/>',
    unknown: '<circle cx="10" cy="10" r="7.4"/><path d="M10 6.4v4.2M10 13.4v.2"/>'
  };

  // Every style names a Google channel and the settings the publishing code
  // fixes for it, so the operator is never asked to choose something that has
  // one correct answer.
  const STYLES = [
    {
      key: 'pmax', name: 'Performance Max', channel: 'PERFORMANCE_MAX', icon: 'pmax', accent: '#c8922f',
      tagline: 'Sales optimization across Google channels',
      whenToChoose: 'One campaign across Search, Shopping, YouTube, Display, Discover, Gmail and Maps, optimized for sales. The best default for a product with a Merchant Center listing; you give up per-channel control.',
      // Google's published guidance: an average daily budget of at least 3× the
      // cost per conversion, and at least 6 weeks before judging results
      // (support.google.com/google-ads/answer/15864652, /14104997).
      recommendedDaily: 20, minimumSensibleDaily: 10, evaluationDays: 42,
      budgetNote: 'Google advises a daily budget of at least 3× your cost per conversion and 6 weeks before judging results. Smaller budgets still run but learn slowly.',
      autoSettings: [
        'A new campaign is created paused, so nothing spends before you review it. Added to one of your existing Performance Max campaigns, the product joins that campaign’s status, budget and bidding.',
        'A new campaign has its own budget, never shared with another campaign.',
        'A new campaign excludes searches that do not lead to jewellery sales (DIY, wholesale, jobs, digital files); the plan lists each one.',
        'Bids for the most conversion value (sales revenue); the plan shows any target ROAS.',
        'Advertises only this product’s Merchant Center offers, and every click lands on its product page.',
        'Google’s automatic text, image and video enhancements are off. Shopping ads still use your product feed, and without an attached video Google may make one from your images.'
      ]
    },
    {
      key: 'responsive_display', name: 'Responsive Display', channel: 'DISPLAY', icon: 'responsive_display', accent: '#6f8f6a',
      tagline: 'Flexible reach across websites and apps',
      whenToChoose: 'Google fits your photos, text and logo to almost any Display placement, including native formats. More reach than Fixed Display; the final look varies.',
      recommendedDaily: 10, minimumSensibleDaily: 5,
      budgetNote: 'Display clicks cost less but convert less often than Search or Shopping clicks. Keep it a small add-on and judge it by sales, not clicks.',
      autoSettings: [
        'Created paused, so nothing spends before you review it.',
        'Its own budget, never shared with another campaign.',
        'Bids to maximize conversions.',
        'No audience or placement targeting: Google chooses where it shows across the Display Network.',
        'Your approved text and photos plus the official Brites logo; Google’s asset enhancements and auto-generated video are off.'
      ]
    },
    {
      key: 'fixed_display', name: 'Fixed Display', channel: 'DISPLAY', icon: 'fixed_display', accent: '#8a6a9f',
      tagline: 'Your finished design, preserved',
      whenToChoose: 'Shows your exact banner in each supported size. Full visual control, but fewer placements and no text rotation or animation.',
      recommendedDaily: 10, minimumSensibleDaily: 5,
      budgetNote: 'Fixed sizes reach fewer placements than responsive ads, so a larger budget may go unspent. Start small and raise it only if it spends in full.',
      autoSettings: [
        'Created paused, so nothing spends before you review it.',
        'Its own budget, never shared with another campaign.',
        'Bids to maximize conversions.',
        'No audience or placement targeting: Google chooses where it shows across the Display Network.',
        'One image ad per supported banner size, exactly as approved.'
      ]
    }
  ];

  // Campaigns that already exist in the account are described by their Google
  // channel, not by a style this application would have created.
  const CHANNELS = {
    PERFORMANCE_MAX: { name: 'Performance Max', icon: 'pmax', accent: '#c8922f' },
    SEARCH: { name: 'Search · text ads', icon: 'search', accent: '#4a7fb5' },
    DISPLAY: { name: 'Display', icon: 'responsive_display', accent: '#6f8f6a' },
    VIDEO: { name: 'Video', icon: 'video', accent: '#b5654a' },
    DEMAND_GEN: { name: 'Demand Gen', icon: 'demand_gen', accent: '#9a5b7c' },
    SHOPPING: { name: 'Shopping', icon: 'shopping', accent: '#5f8f8f' }
  };

  const byKey = {};
  STYLES.forEach(s => { byKey[s.key] = s; });

  function iconSvg(name, size) {
    const paths = ICONS[name] || ICONS.unknown;
    const px = Number(size) || 14;
    return '<svg class="campaignIcon" viewBox="0 0 20 20" width="' + px + '" height="' + px +
      '" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" focusable="false">' +
      paths + '</svg>';
  }

  // Accepts a style key, a Google channel, or a campaign row. A Display
  // campaign's name records which of the two Display styles produced it; the
  // name is the only place that distinction survives in Google.
  function describe(input) {
    if (!input) return { key: 'unknown', name: 'Campaign type unavailable', icon: 'unknown', accent: '#8a8a8a', known: false };
    if (typeof input === 'string') {
      if (byKey[input]) return { ...byKey[input], known: true };
      const channel = CHANNELS[input.toUpperCase()];
      if (channel) return { key: input.toUpperCase(), ...channel, known: true };
      return { key: 'unknown', name: 'Campaign type unavailable', icon: 'unknown', accent: '#8a8a8a', known: false };
    }
    const channel = String(input.channel || input.advertisingChannelType || '').toUpperCase();
    const name = String(input.name || '');
    if (channel === 'DISPLAY') {
      if (name.includes('· Fixed Display ·')) return { ...byKey.fixed_display, known: true };
      if (name.includes('· Responsive Display ·')) return { ...byKey.responsive_display, known: true };
      return { key: 'DISPLAY', name: 'Display · format not identified', icon: 'responsive_display', accent: '#6f8f6a', known: false };
    }
    const known = describe(channel);
    if (!known.known && channel) return { key: channel, name: channel, icon: 'unknown', accent: '#8a8a8a', known: false };
    return known;
  }

  // The label a person reads, with its icon, wherever a campaign is named.
  function badge(input, options) {
    const d = describe(input), opts = options || {};
    const escape = s => String(s == null ? '' : s).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
    return '<span class="campaignBadge" data-campaign-kind="' + escape(d.key) + '" style="--campaign-accent:' + escape(d.accent) + '"' +
      (opts.title === false ? '' : ' title="' + escape(d.name) + '"') + '>' +
      iconSvg(d.icon, opts.size) + (opts.label === false ? '' : '<span>' + escape(opts.text || d.name) + '</span>') + '</span>';
  }

  // What a recommended campaign is called and explained as wherever an opportunity is shown (the Search and Product
  // ads cards): a plain name, then three short lines for the "What is this?" disclosure. The icon and colour come from
  // the style or channel above, so an opportunity wears the same mark as the campaign it becomes.
  const KIND_WORDS = {
    search: {
      of: 'SEARCH', name: 'Search text ad',
      tagline: 'Words that appear when someone searches Google for what you sell.',
      where: 'On Google search results, above and below the regular listings.',
      suits: 'Shoppers who already know what they want and are typing it in.'
    },
    pmax: {
      of: 'pmax', name: 'Product ad (Performance Max)',
      tagline: 'Your products shown as ads across Google, placed for you.',
      where: 'Google Search, Shopping, YouTube, Gmail, Maps and partner sites.',
      suits: 'Products in your Merchant Center feed that shoppers already buy.'
    },
    responsive_display: {
      of: 'responsive_display', name: 'Responsive banner ad',
      tagline: 'One set of photos and words that Google fits to almost any banner space.',
      where: 'Websites and apps in the Google Display Network.',
      suits: 'Reaching more people cheaply. Judge it by sales, not clicks.'
    },
    fixed_display: {
      of: 'fixed_display', name: 'Fixed banner ad',
      tagline: 'Your finished banner design, shown exactly as made.',
      where: 'Websites and apps in the Google Display Network, in the sizes you supply.',
      suits: 'When the exact look of the ad matters more than reach.'
    }
  };
  const KIND_ALIASES = { product: 'pmax', product_ads: 'pmax', performance_max: 'pmax', display: 'responsive_display' };
  function opportunityKind(kind) {
    const raw = String(kind == null ? '' : kind).trim().toLowerCase().replace(/[\s-]+/g, '_');
    const key = KIND_ALIASES[raw] || raw;
    const words = Object.prototype.hasOwnProperty.call(KIND_WORDS, key) ? KIND_WORDS[key] : null;
    if (!words) return null;
    const d = describe(words.of);
    return { key, name: words.name, tagline: words.tagline, where: words.where, suits: words.suits, icon: d.icon, accent: d.accent };
  }
  // The type mark for an opportunity card: the campaign icon and its plain name, or '' for a type this file does not know.
  function opportunityBadge(kind, options) {
    const k = opportunityKind(kind);
    return k ? badge(KIND_WORDS[k.key].of, Object.assign({ title: false }, options, { text: k.name })) : '';
  }

  // The publisher refuses new campaigns whose budgets, added to those of
  // campaigns already enabled, exceed the total daily budget ceiling in
  // Controls. Browser and server word it the same way, before anything is approved.
  function budgetCeilingMessage(totalDaily, enabledDaily, ceiling, currency) {
    const limit = Number(ceiling), used = Math.max(0, Number(enabledDaily) || 0), want = Number(totalDaily) || 0, free = Math.max(0, limit - used);
    if (!(limit > 0) || want <= free + 0.001) return null;
    const c = currency ? currency + ' ' : '';
    return 'Over your daily ceiling: these budgets total ' + c + want.toFixed(2) + ', but only ' + c + free.toFixed(2) + ' of your ' + c + limit.toFixed(2) + ' ceiling is free (enabled campaigns use ' + c + used.toFixed(2) + '). Lower a budget, or raise the ceiling in Controls.';
  }

  // Adding a product to a Performance Max campaign that is already learning pools budget and
  // conversion data instead of splitting both. A campaign fits when it advertises the product's
  // Merchant Center feed (a campaign without a feed label advertises every feed); it is the
  // default when it also has the same feed label and exactly the same countries.
  function pmaxTargetFits(target, draft) {
    const d = draft || {};
    if (!target || !target.merchantId || (d.merchantId && String(d.merchantId) !== String(target.merchantId))) return false;
    const a = String(target.feedLabel || '').toUpperCase(), b = String(d.feedLabel || '').toUpperCase();
    return !a || a === b;
  }
  function pmaxDefaultTarget(targets, draft) {
    const d = draft || {}, list = v => [...new Set((v || []).map(x => String(x).toUpperCase()))].sort().join(),
      want = String(d.feedLabel || '').toUpperCase(), countries = (d.countries || []).length ? ['countries', list(d.countries)] : ['countryCodes', list(d.countryCodes)];
    if (!countries[1]) return null;
    const hits = (targets || []).filter(t => pmaxTargetFits(t, d) && String(t.feedLabel || '').toUpperCase() === want && list(t[countries[0]]) === countries[1]);
    hits.sort((a, b) => (a.status === 'ENABLED' ? 0 : 1) - (b.status === 'ENABLED' ? 0 : 1) || (Number(b.budget) || 0) - (Number(a.budget) || 0) || String(a.name).localeCompare(String(b.name)));
    return hits[0] || null;
  }
  // The combined budget, in words, when a product joins an existing campaign; add is what the
  // draft adds to that campaign's daily budget (0 keeps it unchanged).
  function pmaxJoinText(target, add, currency) {
    const c = currency ? currency + ' ' : '', now = Number(target.budget) || 0, plus = Math.max(0, Number(add) || 0), groups = (Number(target.assetGroups) || 0) + 1;
    return (plus > 0 ? c + now.toFixed(2) + ' + ' + plus.toFixed(2) + ' = ' + (now + plus).toFixed(2) : c + now.toFixed(2)) + '/day combined, shared by ' + groups + ' product groups' +
      (target.sharedBudget ? ' and every campaign using this shared budget' : '') + '.' + (target.status === 'ENABLED' ? '' : ' The campaign is paused; the product runs once you enable it.');
  }

  const api = { STYLES, CHANNELS, ICONS, byKey, describe, badge, iconSvg, opportunityKind, opportunityBadge, budgetCeilingMessage, pmaxTargetFits, pmaxDefaultTarget, pmaxJoinText };
  if (typeof module === 'object' && module.exports) module.exports = api; else root.BritesCampaignStyles = api;
})(typeof window === 'object' ? window : globalThis);
