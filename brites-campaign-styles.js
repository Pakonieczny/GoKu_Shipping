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
      whenToChoose: 'Google allocates this budget across eligible Search, Shopping, YouTube, Display, Discover, Gmail and Maps inventory. Choose it for broad conversion optimization; you give up exact layout and channel-budget control. Merchant eligibility and video readiness still matter.',
      // Performance Max learns from conversions, and a budget too small to earn
      // several a week keeps it learning indefinitely.
      recommendedDaily: 20, minimumSensibleDaily: 10,
      budgetNote: 'Performance Max optimizes from conversions. Below about 10 a day it rarely gathers enough to leave its learning period, and results stay unrepresentative.',
      autoSettings: [
        'Created paused, so nothing spends before you review it.',
        'Its own budget, never shared with another campaign.',
        'Brand guidelines off, so this ad’s own logo and business name are the ones that serve.',
        'Automatically generated creative turned off, so Google serves only the assets you approved.',
        'Standard delivery, spending evenly rather than front-loading the day.'
      ]
    },
    {
      key: 'responsive_display', name: 'Responsive Display', channel: 'DISPLAY', icon: 'responsive_display', accent: '#6f8f6a',
      tagline: 'Flexible reach across websites and apps',
      whenToChoose: 'Google combines your clean square and landscape photographs with separate text and branding. Choose it for broader Display coverage; the final layout can differ from your preview.',
      recommendedDaily: 10, minimumSensibleDaily: 5,
      budgetNote: 'Display impressions are inexpensive, so a smaller budget still buys meaningful reach. Its job is coverage, not conversion efficiency.',
      autoSettings: [
        'Created paused, so nothing spends before you review it.',
        'Its own budget, never shared with another campaign.',
        'Bidding set to maximize conversions.',
        'Standard delivery, spending evenly rather than front-loading the day.'
      ]
    },
    {
      key: 'fixed_display', name: 'Fixed Display', channel: 'DISPLAY', icon: 'fixed_display', accent: '#8a6a9f',
      tagline: 'Your finished design, preserved',
      whenToChoose: 'Keeps the exact composition in supported banner sizes. Choose it for visual control. It reaches fewer placements and does not rotate text or animate.',
      recommendedDaily: 10, minimumSensibleDaily: 5,
      budgetNote: 'Fixed sizes reach fewer placements than responsive ones, so a large budget will not always be spent. Start modestly and raise it only if it delivers in full.',
      autoSettings: [
        'Created paused, so nothing spends before you review it.',
        'Its own budget, never shared with another campaign.',
        'Bidding set to maximize conversions.',
        'One ad per supported banner size, from the design you approved.'
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
    DEMAND_GEN: { name: 'Demand Gen', icon: 'video', accent: '#b5654a' },
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

  const api = { STYLES, CHANNELS, ICONS, byKey, describe, badge, iconSvg };
  if (typeof module === 'object' && module.exports) module.exports = api; else root.BritesCampaignStyles = api;
})(typeof window === 'object' ? window : globalThis);
