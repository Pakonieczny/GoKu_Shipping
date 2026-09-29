// netlify/functions/_googleAdsCampaignOptions.js
// Small campaign options for the Brites Adwords console. Each becomes a draft in Approvals and
// reaches Google only after Paul approves it: a brand Search campaign, a bidding seasonality
// adjustment for a short sale, the new-customer acquisition goal, and a campaign total budget for
// a fixed-length promotion. Pure helpers (no network, no Firestore): googleAdsAutopilot.js reads
// the account, stores the drafts and publishes approved ones.
//
// Google Ads API v24 facts relied on here (checked 2026-09-29 against the v24 reference, the
// discovery document and the Google Ads Help Center):
// - Date-times: Campaign.start_date_time / end_date_time and BiddingSeasonalityAdjustment
//   .start_date_time / end_date_time are all "yyyy-MM-dd HH:mm:ss" in the account time zone (v24
//   reference for Campaign and BiddingSeasonalityAdjustment, the "Create campaigns" guide, and
//   DateError.INVALID_STRING_DATE_TIME_SECONDS), and GAQL returns that layout. Some of Google's
//   client samples send "yyyyMMdd HH:mm:ss" and it works, but no reference documents it. gadsDateTime
//   below is the one writer; every comparison of two date-times reads through it too.
// - BiddingSeasonalityAdjustment (customers/{cid}/biddingSeasonalityAdjustments:mutate): scope
//   CAMPAIGN with up to 2,000 campaigns; start_date_time is inclusive and end_date_time EXCLUSIVE,
//   both "yyyy-MM-dd HH:mm:ss" in the account time zone; the interval must be within (0, 14 days]
//   and in the future; conversion_rate_modifier 0.1 to 10.0 (1.0 = no change). Google applies it to
//   Search, Shopping and Display campaigns using target ROAS or target CPA, and to Performance Max
//   with any bid strategy (support.google.com/google-ads/answer/10369906). Best for 1 to 7 days.
// - New customer acquisition: in v24 Goal/CampaignGoalConfig carry only CUSTOMER_RETENTION, so the
//   campaign setting is CampaignLifecycleGoalService.ConfigureCampaignLifecycleGoals
//   (customers/{cid}/campaignLifecycleGoal:configureCampaignLifecycleGoals), operation create
//   (campaign set) or update (resource name customers/{cid}/campaignLifecycleGoals/{campaignId}),
//   customer_acquisition_goal_settings.optimization_mode TARGET_ALL_EQUALLY |
//   BID_HIGHER_FOR_NEW_CUSTOMER | TARGET_NEW_CUSTOMER. Search, Performance Max, Shopping and Demand
//   Gen (answer/12080169). "Bid higher" needs value-based bidding and a value (>= 0.01); "only new"
//   also allows target CPA / maximize conversions. Google must know the existing customers first
//   (a customer list of 1,000+ active members, or purchasers from the Google tag), set in Google Ads.
// - Campaign total budgets: CampaignBudget.period CUSTOM_PERIOD with total_amount_micros (exclusive
//   with amount_micros), explicitly_shared false, never shared, period immutable after creation;
//   the campaign needs start and end dates (END_DATE_TIME_REQUIRED_FOR_TOTAL_BUDGET) and may run at
//   most 90 days for Search, Standard Shopping and Performance Max (DURATION_TOO_LONG_FOR_TOTAL_BUDGET,
//   answer/15137812). Search: target ROAS, maximize conversion value, target CPA, maximize
//   conversions, maximize clicks, target impression share, manual CPC. Performance Max: target ROAS,
//   maximize conversion value, target CPA, maximize conversions. Not available for Display.
// - Campaign conversion goals: a new campaign inherits the account's goals. CampaignConversionGoal
//   (customers/{cid}/campaignConversionGoals/{campaignId}~{category}~{origin}) is update-only; an
//   unset biddable inherits the account default, so a goal is kept out of bidding only by an explicit
//   biddable=false with update_mask "biddable". Google's v24 Performance Max example sends these
//   updates in the same GoogleAdsService.Mutate request as the campaign, after it, addressed by the
//   campaign's temporary ID, from the account's customer_conversion_goal rows. The first update moves
//   the campaign to campaign-level goals, which account-level changes no longer alter.
"use strict";

const DAY = 86400000;
const r2 = v => Math.round(Number(v) * 100) / 100;
const toMicros = v => Math.round(Number(v) * 1e6);
const fromMicros = m => (Number(m) || 0) / 1e6;
const ymd = d => d.toISOString().slice(0, 10);
function parseDay(s) {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(s == null ? "" : s).trim()); if (!m) return null;
  const d = new Date(Date.UTC(+m[1], +m[2] - 1, +m[3])); return ymd(d) === m[0] ? d : null;
}
// "2026-11-27 00:00:00", "20261127 23:59:59" or "2026-11-27" -> "2026-11-27".
function dateOnly(s) { const m = String(s || "").match(/(\d{4})-?(\d{2})-?(\d{2})/); return m ? `${m[1]}-${m[2]}-${m[3]}` : null; }
// The one writer of Google Ads date-times, always "yyyy-MM-dd HH:mm:ss". value: a date ("2026-11-27" or
// "20261127", which takes `time`) or a date-time in either layout ("2026-11-27 23:59:59", "20261127 23:59:59",
// "2026-11-27T23:59"; seconds optional). Anything else, a time zone offset included, is null.
function gadsDateTime(value, time = "00:00:00") {
  const m = /^(\d{4})-?(\d{2})-?(\d{2})(?:[ T](\d{1,2}):(\d{2})(?::(\d{2}))?)?$/.exec(String(value == null ? "" : value).trim());
  return m ? `${m[1]}-${m[2]}-${m[3]} ${m[4] == null ? time : `${m[4].padStart(2, "0")}:${m[5]}:${m[6] || "00"}`}` : null;
}
function addDays(day, n) { return ymd(new Date(parseDay(day).getTime() + n * DAY)); }
function daysInclusive(a, b) { return Math.round((parseDay(b) - parseDay(a)) / DAY) + 1; }
function amountIn(v, [min, max], label) {
  const n = typeof v === "string" && !v.trim() ? NaN : Number(v);
  if (!isFinite(n) || n < min || n > max) throw new Error(`${label} must be between ${min} and ${max}.`);
  return r2(n);
}

/* ============================ Brand Search ============================ */
// Same serving settings as the console's Search builder: Google Search only, presence targeting,
// English, and the landing-page tracking suffix the opportunity learner reads.
const SEARCH_URL_SUFFIX = "utm_source=google&utm_medium=paid_search&utm_campaign={campaignid}&utm_content={adgroupid}&utm_term={keyword}";
const BRAND_SEARCH = {
  tag: "brand-search",
  campaignName: "BA · brand-search",
  adGroupName: "Brand · Brites",
  finalUrl: "https://britesjewelry.com/",
  keywords: [["brites", "EXACT"], ["brites", "PHRASE"], ["brites jewelry", "EXACT"], ["brites jewelry", "PHRASE"]].map(([text, matchType]) => ({ text, matchType })),
  // Searches for other "brite" things and for jobs are not shoppers for this store. "free" stays
  // allowed: "brites jewelry free shipping" and "nickel free" are buyers.
  negatives: ["job", "jobs", "hiring", "career", "careers", "salary", "diy", "tutorial", "lite", "teeth", "whitening", "smile", "dental"],
  // Fixed copy made only from the store's vetted brand lines: nothing for paid AI to write or review.
  headlines: ["Brites Jewelry", "Brites Jewelry Official Site", "Shop Brites Jewelry", "Personalized Charm Jewelry", "Handcrafted Jewelry", "Custom-Made Gifts",
    "Made Just For You", "Gifts That Mean More", "Made To Order For You", "Jewelry That Tells A Story", "Little Charms, Big Meaning", "Thoughtful Handmade Gifts"],
  descriptions: ["Shop Brites Jewelry: personalized charm jewelry, handcrafted and made to order.", "Custom-made gifts, personalized just for you.",
    "Discover meaningful jewelry, made to order.", "Designed and handmade to order with meaningful little details."],
  // Sitelinks point to pages other than the ad's own landing page.
  sitelinkPage: { title: "All Jewelry", handle: "all", url: "https://britesjewelry.com/collections/all" },
  relatedHandles: ["bar-engraved", "celestial", "animal-lovers"],
  defaults: { dailyBudget: 5, maxCpc: 0.5 },
  limits: { dailyBudget: [1, 50], maxCpc: [0.05, 5] },
  pairing: "Pairs with the Performance Max brand exclusion: together, people already searching for Brites are bought here at Search brand prices instead of through Performance Max."
};

function buildBrandSearchOps({ cid, dailyBudget, maxCpc, countries, extensionOps, stamp } = {}) {
  if (!/^\d+$/.test(String(cid || ""))) throw new Error("The Google Ads account is not configured.");
  const budget = amountIn(dailyBudget, BRAND_SEARCH.limits.dailyBudget, "Daily budget");
  const cpc = amountIn(maxCpc, BRAND_SEARCH.limits.maxCpc, "Max cost per click");
  if (cpc > budget) throw new Error("The max cost per click can't be more than the daily budget.");
  const geo = [...new Set((countries || []).map(x => String(x).replace(/\D/g, "")).filter(Boolean))];
  if (!geo.length) throw new Error("Choose the account's target countries in Controls first.");
  const bRes = `customers/${cid}/campaignBudgets/-1`, cRes = `customers/${cid}/campaigns/-2`, agRes = `customers/${cid}/adGroups/-3`;
  const ops = [
    { campaignBudgetOperation: { create: { resourceName: bRes, name: `${BRAND_SEARCH.campaignName} · ${stamp || Date.now()}`, amountMicros: toMicros(budget), deliveryMethod: "STANDARD", explicitlyShared: false } } },
    { campaignOperation: { create: { resourceName: cRes, name: BRAND_SEARCH.campaignName, status: "PAUSED", advertisingChannelType: "SEARCH", campaignBudget: bRes,
      containsEuPoliticalAdvertising: "DOES_NOT_CONTAIN_EU_POLITICAL_ADVERTISING", manualCpc: { enhancedCpcEnabled: false },
      networkSettings: { targetGoogleSearch: true, targetSearchNetwork: false, targetPartnerSearchNetwork: false, targetContentNetwork: false },
      finalUrlSuffix: SEARCH_URL_SUFFIX, geoTargetTypeSetting: { positiveGeoTargetType: "PRESENCE" } } } },
    // The ad group's max CPC is the bid cap: manual CPC never bids above it.
    { adGroupOperation: { create: { resourceName: agRes, name: BRAND_SEARCH.adGroupName, campaign: cRes, type: "SEARCH_STANDARD", cpcBidMicros: toMicros(cpc) } } },
    { adGroupAdOperation: { create: { adGroup: agRes, status: "ENABLED", ad: { finalUrls: [BRAND_SEARCH.finalUrl],
      responsiveSearchAd: { headlines: BRAND_SEARCH.headlines.map(text => ({ text })), descriptions: BRAND_SEARCH.descriptions.map(text => ({ text })) } } } } },
    ...BRAND_SEARCH.keywords.map(k => ({ adGroupCriterionOperation: { create: { adGroup: agRes, status: "ENABLED", keyword: { text: k.text, matchType: k.matchType } } } })),
    ...geo.map(id => ({ campaignCriterionOperation: { create: { campaign: cRes, location: { geoTargetConstant: `geoTargetConstants/${id}` } } } })),
    { campaignCriterionOperation: { create: { campaign: cRes, language: { languageConstant: "languageConstants/1000" } } } },
    ...BRAND_SEARCH.negatives.map(text => ({ campaignCriterionOperation: { create: { campaign: cRes, negative: true, keyword: { text, matchType: "BROAD" } } } }))
  ];
  const ext = typeof extensionOps === "function" ? extensionOps(cRes) : { ops: [], summary: null };
  ops.push(...ext.ops);
  return { ops, dailyBudget: budget, maxCpc: cpc, countries: geo, assetSummary: ext.summary, negatives: BRAND_SEARCH.negatives.slice(),
    keywordSummary: { count: BRAND_SEARCH.keywords.length, exact: BRAND_SEARCH.keywords.filter(k => k.matchType === "EXACT").length, measured: 0, researched: false, dropped: [], groups: 1, searchPartners: false },
    adGroupSummary: [{ name: BRAND_SEARCH.adGroupName, finalUrl: BRAND_SEARCH.finalUrl, keywords: BRAND_SEARCH.keywords.map(k => (k.matchType === "EXACT" ? `[${k.text}]` : `"${k.text}"`)) }] };
}

// True only for an unmodified brand Search draft: fixed copy, the brand landing page, the brand
// keywords, and nothing else a creative review would have to check (no images, no other text).
// Anything else keeps the normal creative review.
const BRAND_OP_KINDS = new Set(["campaignBudgetOperation", "campaignOperation", "adGroupOperation", "adGroupAdOperation", "adGroupCriterionOperation", "campaignCriterionOperation", "assetOperation", "campaignAssetOperation"]);
function isBrandSearchDraft(item, cid) {
  if (!item || item.type !== "brand") return false;
  const p = item.payload || {}, ops = p.mutateOperations;
  if (p.designStudioSpec || p.service || p.operations || p.reviewGroups || p.generatedAssets || !Array.isArray(ops) || !ops.length) return false;
  const headlines = new Set(BRAND_SEARCH.headlines), descriptions = new Set(BRAND_SEARCH.descriptions), words = new Set(BRAND_SEARCH.keywords.map(k => k.text));
  const owned = u => typeof u === "string" && /^https:\/\/britesjewelry\.com\//.test(u);
  const onlyText = list => Array.isArray(list) && list.length > 0 && list.every(x => x && Object.keys(x).length === 1 && typeof x.text === "string");
  let ads = 0, campaigns = 0;
  for (const op of ops) {
    const kinds = Object.keys(op || {}); if (kinds.length !== 1 || !BRAND_OP_KINDS.has(kinds[0])) return false;
    const body = op[kinds[0]]; if (!body || !body.create || Object.keys(body).length !== 1) return false;
    const c = body.create;
    if (kinds[0] === "campaignOperation") { campaigns++; if (c.name !== BRAND_SEARCH.campaignName || c.advertisingChannelType !== "SEARCH" || c.status !== "PAUSED" || c.resourceName !== `customers/${cid}/campaigns/-2`) return false; }
    if (kinds[0] === "adGroupAdOperation") {
      ads++; const ad = c.ad || {}, rsa = ad.responsiveSearchAd || {};
      if (Object.keys(ad).some(k => k !== "finalUrls" && k !== "responsiveSearchAd") || Object.keys(rsa).some(k => k !== "headlines" && k !== "descriptions")) return false;
      if (!Array.isArray(ad.finalUrls) || ad.finalUrls.length !== 1 || ad.finalUrls[0] !== BRAND_SEARCH.finalUrl) return false;
      if (!onlyText(rsa.headlines) || !onlyText(rsa.descriptions) || rsa.headlines.some(h => !headlines.has(h.text)) || rsa.descriptions.some(d => !descriptions.has(d.text))) return false;
    }
    if (kinds[0] === "adGroupCriterionOperation" && (!c.keyword || c.negative || !words.has(c.keyword.text))) return false;
    if (kinds[0] === "campaignCriterionOperation" && !(c.location || c.language || (c.negative === true && c.keyword))) return false;
    if (kinds[0] === "assetOperation") {
      const types = Object.keys(c).filter(k => /Asset$/.test(k));
      if (types.length !== 1 || (types[0] !== "sitelinkAsset" && types[0] !== "calloutAsset")) return false;
      if (types[0] === "sitelinkAsset" && !(Array.isArray(c.finalUrls) && c.finalUrls.length)) return false;
      if ((c.finalUrls || []).some(u => !owned(u))) return false;
    }
  }
  return ads === 1 && campaigns === 1;
}

/* ===================== Sale-day bid adjustments (seasonality) ===================== */
// Dated occasions from the opportunity engine's calendar rules (`rule` is the occasion label the
// engine's _occasionRule understands). Default windows: the Black Friday to Cyber Monday weekend,
// and the week before Valentine's Day and Mother's Day, when gift orders still arrive in time.
const SALE_OCCASIONS = [
  { key: "bfcm", label: "Black Friday / Cyber Monday", rule: "cyber monday", defaultPct: 25, window: peak => [addDays(peak, -3), peak] },
  { key: "valentines", label: "Valentine's Day", rule: "valentine's day", defaultPct: 15, window: peak => [addDays(peak, -7), addDays(peak, -1)] },
  { key: "mothers", label: "Mother's Day", rule: "mother's day", defaultPct: 15, window: peak => [addDays(peak, -7), addDays(peak, -1)] }
];
const SEASONALITY_LIMITS = { pct: [-50, 150], maxDays: 14 };
function saleOccasion(key) {
  const o = SALE_OCCASIONS.find(x => x.key === key);
  if (!o) throw new Error("Choose Black Friday / Cyber Monday, Valentine's Day or Mother's Day.");
  return o;
}
// The next window that is still ahead (starting tomorrow at the earliest). peakOf(rule, from) gives
// the occasion's date on or after `from` (YYYY-MM-DD). lastPeak is the same occasion a year earlier,
// so last year's matching dates can be read.
function saleWindow(key, { today, peakOf }) {
  const o = saleOccasion(key), tomorrow = addDays(today, 1);
  let from = tomorrow;
  for (let i = 0; i < 3; i++) {
    const peak = peakOf(o.rule, from); if (!parseDay(peak)) break;
    let [start, end] = o.window(peak); if (start < tomorrow) start = tomorrow;
    if (start <= end) { const lastPeak = peakOf(o.rule, addDays(peak, -400));
      return { key: o.key, label: o.label, start, end, days: daysInclusive(start, end), peak, lastPeak: parseDay(lastPeak) ? lastPeak : null, defaultPct: o.defaultPct }; }
    from = addDays(peak, 1);
  }
  throw new Error(`No upcoming dates were found for ${o.label}.`);
}
function checkSaleDates(start, end, today) {
  if (!parseDay(start) || !parseDay(end)) throw new Error("Choose a start and an end date.");
  if (start <= today) throw new Error("A sale-day adjustment must start after today.");
  if (end < start) throw new Error("The end date is before the start date.");
  const days = daysInclusive(start, end);
  if (days > SEASONALITY_LIMITS.maxDays) throw new Error(`Google allows at most ${SEASONALITY_LIMITS.maxDays} days; 1 to 7 days works best.`);
  return days;
}
// c: { channel, bidding, targetRoas, targetCpaMicros, status, servingStatus, endDate }.
function seasonalityEligible(c, start) {
  if (!c || c.status === "REMOVED") return { ok: false, reason: "removed" };
  if (c.servingStatus === "ENDED" || (c.endDate && start && c.endDate < start)) return { ok: false, reason: "ends before these dates" };
  if (c.channel === "PERFORMANCE_MAX") return { ok: true };
  if (!["SEARCH", "SHOPPING", "DISPLAY"].includes(c.channel)) return { ok: false, reason: "campaign type not supported" };
  const target = c.bidding === "TARGET_ROAS" || c.bidding === "TARGET_CPA" ||
    (c.bidding === "MAXIMIZE_CONVERSION_VALUE" && Number(c.targetRoas) > 0) || (c.bidding === "MAXIMIZE_CONVERSIONS" && Number(c.targetCpaMicros) > 0);
  return target ? { ok: true } : { ok: false, reason: "needs target ROAS or target CPA bidding" };
}
// Expected change from the account's own history: last year's conversion rate on the matching dates
// against the four weeks before them. Too little history: the occasion's starting estimate, said so.
// rows: [{ date: "YYYY-MM-DD", clicks, conversions }] for the whole account.
function conversionRateEstimate(rows, { start, end, shiftDays, defaultPct }) {
  const pct = v => (v * 100).toFixed(1) + "%";
  if (rows == null) return { pct: defaultPct, measured: false, basis: "Starting estimate: last year's results could not be read." };
  if (!(shiftDays > 0)) return { pct: defaultPct, measured: false, basis: "Starting estimate: last year's dates for this occasion are unknown." };
  const ls = addDays(start, -shiftDays), le = addDays(end, -shiftDays), bs = addDays(ls, -28), be = addDays(ls, -1);
  const sum = (a, b) => (rows || []).filter(r => r && r.date >= a && r.date <= b).reduce((t, r) => ({ clicks: t.clicks + (Number(r.clicks) || 0), conv: t.conv + (Number(r.conversions) || 0) }), { clicks: 0, conv: 0 });
  const w = sum(ls, le), base = sum(bs, be);
  if (!(w.clicks >= 100 && w.conv >= 3 && base.clicks >= 300 && base.conv >= 5))
    return { pct: defaultPct, measured: false, lastYear: { start: ls, end: le }, basis: `Starting estimate: too little history for ${ls} to ${le} last year to measure it.` };
  const rate = w.conv / w.clicks, baseRate = base.conv / base.clicks;
  const change = Math.max(SEASONALITY_LIMITS.pct[0], Math.min(SEASONALITY_LIMITS.pct[1], Math.round((rate / baseRate - 1) * 100)));
  return { pct: change, measured: true, lastYear: { start: ls, end: le }, basis: `Estimate from last year: conversion rate ${pct(rate)} on ${ls} to ${le}, against ${pct(baseRate)} in the four weeks before.` };
}
// A sale's days as Google takes them: the start is the first day's midnight, the end EXCLUSIVE, the midnight
// after the last day. The operation and the overlap checks both use this, so their strings always match.
function seasonalityWindow(start, end) {
  return { start: gadsDateTime(start, "00:00:00"), endExclusive: gadsDateTime(addDays(end, 1), "00:00:00") };
}
function seasonalityOperation({ cid, label, start, end, today, campaignIds, changePct, description }) {
  const days = checkSaleDates(start, end, today);
  const pct = typeof changePct === "string" && !changePct.trim() ? NaN : Number(changePct);
  if (!isFinite(pct) || pct < SEASONALITY_LIMITS.pct[0] || pct > SEASONALITY_LIMITS.pct[1]) throw new Error("Set the expected conversion-rate change between -50% and +150%.");
  const modifier = r2(1 + pct / 100);
  if (modifier === 1) throw new Error("An expected change of 0% leaves bidding unchanged, so there is nothing to draft.");
  const ids = [...new Set((campaignIds || []).map(x => String(x).replace(/\D/g, "")).filter(Boolean))];
  if (!ids.length) throw new Error("Choose at least one campaign.");
  if (ids.length > 2000) throw new Error("Google allows at most 2,000 campaigns in one adjustment.");
  const win = seasonalityWindow(start, end);
  return { days, modifier, campaignIds: ids, operation: { create: {
    name: `Brites · ${label} · ${start} to ${end}`.slice(0, 255), description: String(description || "").slice(0, 2048), scope: "CAMPAIGN",
    campaigns: ids.map(id => `customers/${cid}/campaigns/${id}`),
    // Google's end is exclusive: midnight after the last day.
    startDateTime: win.start, endDateTime: win.endExclusive, conversionRateModifier: modifier } } };
}
// Windows overlap when one starts before the other ends. Adjustment times are account-time strings; both
// windows are read as "yyyy-MM-dd HH:mm:ss" first, so one read from Google, one saved in a draft and one
// built here compare in the same layout whichever way they were written (a date alone is its midnight).
function seasonalityOverlaps(a, b) {
  const t = v => gadsDateTime(v) || String(v == null ? "" : v);
  return t(a.start) < t(b.endExclusive) && t(b.start) < t(a.endExclusive);
}

/* ===================== New-customer acquisition goal ===================== */
const ACQUISITION_MODES = {
  TARGET_ALL_EQUALLY: "Off (new and returning customers alike)",
  BID_HIGHER_FOR_NEW_CUSTOMER: "Bid higher for new customers",
  TARGET_NEW_CUSTOMER: "Only bid for new customers"
};
const ACQUISITION_TRADEOFF = "Off by default. Winning first-time buyers costs more: expect a higher cost per sale and lower reported ROAS, and returning customers see fewer of these ads.";
const ACQUISITION_PREREQUISITE = "Google must first know who your existing customers are: in Google Ads, Goals > Customer acquisition, add a customer list (1,000+ active members) or let Google detect purchasers from your tag.";
const ACQUISITION_CHANNELS = ["SEARCH", "PERFORMANCE_MAX", "SHOPPING", "DEMAND_GEN"];
const VALUE_BIDDING = ["MAXIMIZE_CONVERSION_VALUE", "TARGET_ROAS"];
const CONVERSION_BIDDING = ["MAXIMIZE_CONVERSION_VALUE", "TARGET_ROAS", "MAXIMIZE_CONVERSIONS", "TARGET_CPA"];
// c: { status, channel, bidding }, current: { mode, value } | null, purchaseGoal: true | false | null.
function acquisitionEligibility(c, mode, { current = null, purchaseGoal = null } = {}) {
  if (!ACQUISITION_MODES[mode]) return { ok: false, reason: "Choose Off, bid higher for new customers, or only new customers." };
  if (!c || c.status === "REMOVED") return { ok: false, reason: "This campaign was removed." };
  if (!ACQUISITION_CHANNELS.includes(c.channel)) return { ok: false, reason: "Google offers this goal for Search, Performance Max, Shopping and Demand Gen campaigns." };
  const on = current && current.mode && current.mode !== "TARGET_ALL_EQUALLY";
  if (mode === "TARGET_ALL_EQUALLY") return on ? { ok: true } : { ok: false, reason: "Already off for this campaign." };
  if (mode === "BID_HIGHER_FOR_NEW_CUSTOMER" && !VALUE_BIDDING.includes(c.bidding)) return { ok: false, reason: "Bidding higher for new customers needs Maximize conversion value or target ROAS bidding." };
  if (mode === "TARGET_NEW_CUSTOMER" && !CONVERSION_BIDDING.includes(c.bidding)) return { ok: false, reason: "Bidding only for new customers needs conversion-based bidding (target ROAS, target CPA or a Maximize strategy)." };
  if (mode === "BID_HIGHER_FOR_NEW_CUSTOMER" && purchaseGoal === false) return { ok: false, reason: "This campaign doesn't bid toward a purchase conversion goal, which Google requires for this setting." };
  return { ok: true };
}
// The exact request Paul reviews. existing: the campaign's current lifecycle goal ({ mode, value,
// highLifetimeValue }) or null when Google has none for it yet.
function lifecycleGoalRequest({ cid, campaignId, mode, value, existing }) {
  const id = String(campaignId == null ? "" : campaignId).replace(/\D/g, "");
  if (!id || !/^\d+$/.test(String(cid || ""))) throw new Error("Choose a campaign.");
  if (!ACQUISITION_MODES[mode]) throw new Error("Choose Off, bid higher for new customers, or only new customers.");
  const settings = { optimizationMode: mode };
  if (mode === "BID_HIGHER_FOR_NEW_CUSTOMER") {
    const v = typeof value === "string" && !value.trim() ? NaN : Number(value);
    if (!isFinite(v) || v < 0.01 || v > 100000) throw new Error("Enter the extra value of a new customer (at least 0.01).");
    settings.valueSettings = { value: r2(v) };
    // A high lifetime value set in Google Ads stays when it is still above the new value.
    const high = Number(existing && existing.highLifetimeValue);
    if (high > settings.valueSettings.value) settings.valueSettings.highLifetimeValue = high;
  } else if (value != null && String(value).trim() !== "" && Number(value) !== 0) throw new Error("A value applies only when bidding higher for new customers.");
  if (existing) return { operation: { update: { resourceName: `customers/${cid}/campaignLifecycleGoals/${id}`, customerAcquisitionGoalSettings: settings },
    updateMask: "customer_acquisition_goal_settings.optimization_mode,customer_acquisition_goal_settings.value_settings" } };
  return { operation: { create: { campaign: `customers/${cid}/campaigns/${id}`, customerAcquisitionGoalSettings: settings } } };
}
const LIFECYCLE_ERRORS = {
  CUSTOMER_ACQUISITION_MISSING_EXISTING_CUSTOMER_DEFINITION: ACQUISITION_PREREQUISITE,
  CUSTOMER_ACQUISITION_MISSING_HIGH_VALUE_CUSTOMER_DEFINITION: "A high-value customer list (1,000+ active members) must be set up in Google Ads before a high lifetime value can apply.",
  INCOMPATIBLE_BIDDING_STRATEGY: "This campaign's bid strategy doesn't support that setting. Bidding higher for new customers needs Maximize conversion value or target ROAS.",
  MISSING_PURCHASE_GOAL: "This campaign must bid toward a purchase conversion goal first.",
  CUSTOMER_ACQUISITION_UNSUPPORTED_CAMPAIGN_TYPE: "Google doesn't offer the new-customer goal for this campaign type.",
  CUSTOMER_ACQUISITION_VALUE_MISSING: "Bidding higher for new customers needs the extra value of a new customer.",
  CUSTOMER_ACQUISITION_INVALID_VALUE: "The new-customer value is not valid: it must be at least 0.01 and applies only when bidding higher.",
  CUSTOMER_ACQUISITION_INVALID_HIGH_LIFETIME_VALUE: "The high lifetime value set in Google Ads must be above the new-customer value and applies only when bidding higher.",
  CUSTOMER_ACQUISITION_INVALID_OPTIMIZATION_MODE: "Google did not accept that customer acquisition mode.",
  INVALID_CAMPAIGN: "Google couldn't find this campaign.",
  CAMPAIGN_MISSING: "Google couldn't find this campaign."
};
function _errorCodes(data) {
  const out = [], root = (data && data.error) || {};
  (Array.isArray(root.details) ? root.details : []).forEach(d => ((d && (d.errors || (d.googleAdsFailure && d.googleAdsFailure.errors))) || []).forEach(e => Object.values((e && e.errorCode) || {}).forEach(v => out.push(String(v)))));
  return out;
}
// Plain sentence for a refused request; a refused request changes nothing in Google Ads.
function lifecycleErrorText(data, status) {
  const code = _errorCodes(data).find(c => LIFECYCLE_ERRORS[c]);
  const tail = status >= 400 && status < 500 ? " Nothing was changed." : "";
  if (code) return LIFECYCLE_ERRORS[code] + tail;
  const root = (data && data.error) || {};
  return `Google refused the new-customer setting: ${String(root.message || root.status || "HTTP " + status).slice(0, 300)}.${tail}`;
}

/* ===================== Purchase-only goals for new campaigns ===================== */
// Updates that make each new Search or Performance Max campaign bid for purchases only. campaigns:
// the temporary resource names (customers/{cid}/campaigns/-N) of campaigns created in the same
// request, so no existing campaign can be addressed. goals: customer_conversion_goal rows
// { category, origin, biddable } (Google omits a false biddable). Every non-purchase goal is set not
// biddable; every purchase goal the account bids on stays biddable. Without a biddable purchase goal
// nothing is changed: Smart Bidding would be left with nothing to optimize for.
const GOAL_SKIP = new Set(["UNSPECIFIED", "UNKNOWN"]);
function purchaseOnlyGoalOps({ campaigns, goals }) {
  const list = [], seen = new Set();
  (goals || []).forEach(g => { const k = g && g.category + "~" + g.origin;
    if (g && g.category && g.origin && !GOAL_SKIP.has(g.category) && !GOAL_SKIP.has(g.origin) && !seen.has(k)) { seen.add(k); list.push(g); } });
  const refs = [...new Set(campaigns || [])].map(r => /^customers\/(\d+)\/campaigns\/(-\d+)$/.exec(String(r)));
  if (refs.some(m => !m)) throw new Error("Conversion goals are set only for campaigns created in the same request.");
  const purchases = list.filter(g => g.category === "PURCHASE" && g.biddable === true);
  const off = [...new Set(list.filter(g => g.category !== "PURCHASE").map(g => g.category))];
  if (!refs.length) return { ops: [], off: [], kept: [], note: null };
  if (!purchases.length) return { ops: [], off: [], kept: [], note: "The account has no biddable purchase goal, so the new campaign keeps the account's conversion goals." };
  const ops = [];
  for (const [, cid, id] of refs) for (const g of list) {
    if (g.category === "PURCHASE" && g.biddable !== true) continue; // a purchase goal the account keeps out of bidding stays out
    ops.push({ campaignConversionGoalOperation: { update: { resourceName: `customers/${cid}/campaignConversionGoals/${id}~${g.category}~${g.origin}`, biddable: g.category === "PURCHASE" }, updateMask: "biddable" } });
  }
  return { ops, off, kept: purchases.map(g => g.category + "~" + g.origin), note: null };
}

/* ============================ Campaign total budgets ============================ */
const TOTAL_BUDGET_BIDDING = {
  SEARCH: ["manualCpc", "maximizeConversionValue", "maximizeConversions", "targetSpend", "targetImpressionShare", "targetRoas", "targetCpa"],
  PERFORMANCE_MAX: ["maximizeConversionValue", "maximizeConversions", "targetRoas", "targetCpa"]
};
const BIDDING_FIELDS = ["manualCpc", "manualCpm", "manualCpv", "manualCpa", "maximizeConversionValue", "maximizeConversions", "targetSpend", "targetImpressionShare", "targetRoas", "targetCpa", "targetCpm", "targetCpv", "percentCpc", "commission", "fixedCpm", "biddingStrategy"];
const TOTAL_BUDGET_MAX_DAYS = 90;
function isTotalBudget(b) { return !!b && b.period === "CUSTOM_PERIOD"; }
// What a budget can spend per day. A daily budget is its amount. A total budget (fixed dates) is
// what is left of it spread over the days left in its flight: nothing once the flight has ended,
// and all of what is left when its end date is unknown (never less than Google could spend).
function budgetDailyEquivalent(b, { start, end, today, spent = 0 } = {}) {
  if (!isTotalBudget(b)) return fromMicros(b && b.amountMicros);
  const left = Math.max(0, fromMicros(b.totalAmountMicros) - (Number(spent) || 0));
  if (!parseDay(end) || !parseDay(today)) return left;
  const from = parseDay(start) && start > today ? start : today;
  if (end < from) return 0;
  return left / daysInclusive(from, end);
}
// The one new campaign a draft creates and its own budget, if a total budget can apply to it.
function totalBudgetFacts(ops, { today }) {
  const budgets = (ops || []).filter(o => o && o.campaignBudgetOperation && o.campaignBudgetOperation.create).map(o => o.campaignBudgetOperation.create);
  const campaigns = (ops || []).filter(o => o && o.campaignOperation && o.campaignOperation.create).map(o => o.campaignOperation.create);
  if (budgets.length !== 1 || campaigns.length !== 1 || campaigns[0].campaignBudget !== budgets[0].resourceName) return { ok: false, reason: "A total budget applies to one new campaign with its own budget." };
  const b = budgets[0], c = campaigns[0], allowed = TOTAL_BUDGET_BIDDING[c.advertisingChannelType];
  if (!allowed) return { ok: false, reason: "Google offers total budgets for the Search and Performance Max campaigns this console builds, not for this campaign type." };
  const used = BIDDING_FIELDS.filter(k => c[k] !== undefined);
  if (used.length !== 1 || !allowed.includes(used[0])) return { ok: false, reason: "This campaign's bid strategy doesn't support a total budget." };
  const end = dateOnly(c.endDateTime); if (!end) return { ok: false, reason: "Set an end date first: a total budget covers fixed dates." };
  const planned = dateOnly(c.startDateTime), from = planned && planned > today ? planned : today;
  if (end < from) return { ok: false, reason: "The end date has passed. Change the dates first." };
  const days = daysInclusive(from, end);
  if (days > TOTAL_BUDGET_MAX_DAYS) return { ok: false, reason: `Google allows total budgets for up to ${TOTAL_BUDGET_MAX_DAYS} days; these dates run ${days} days.` };
  return { ok: true, budget: b, campaign: c, start: from, end, days, startsOnEnable: !(planned && planned > today) };
}
// Switch a draft's new budget between daily and total for its dates, keeping the same spend: the
// total is the daily amount times the days (and back). prior: the daily amount saved when switching on.
function setTotalBudget(ops, { on, today, prior } = {}) {
  const out = JSON.parse(JSON.stringify(ops || [])), f = totalBudgetFacts(out, { today });
  if (!f.ok) throw new Error(f.reason);
  const b = f.budget;
  if (on) {
    if (isTotalBudget(b)) throw new Error("This draft already uses a total budget.");
    const daily = r2(fromMicros(b.amountMicros)); if (!(daily > 0)) throw new Error("This draft has no daily budget to convert.");
    const total = r2(daily * f.days);
    delete b.amountMicros; b.period = "CUSTOM_PERIOD"; b.totalAmountMicros = toMicros(total); b.explicitlyShared = false;
    return { ops: out, on: true, total, daily, days: f.days, start: f.start, end: f.end, startsOnEnable: f.startsOnEnable };
  }
  if (!isTotalBudget(b)) throw new Error("This draft already uses a daily budget.");
  const total = fromMicros(b.totalAmountMicros), saved = Number(prior && prior.daily);
  const daily = saved > 0 ? r2(saved) : Math.max(0.01, Math.floor(total / f.days * 100) / 100);
  delete b.period; delete b.totalAmountMicros; b.amountMicros = toMicros(daily);
  return { ops: out, on: false, daily, days: f.days, start: f.start, end: f.end };
}
// Daily spend a draft's new budgets add, as the ceiling counts it. A total budget counts its total
// over the days it can still run; its dates must still fit Google's rules.
function draftBudgetDaily(ops, { today }) {
  let sum = 0;
  for (const o of ops || []) {
    const b = o && o.campaignBudgetOperation && o.campaignBudgetOperation.create; if (!b) continue;
    if (!isTotalBudget(b)) { sum += fromMicros(b.amountMicros); continue; }
    const owner = (ops || []).map(x => x && x.campaignOperation && x.campaignOperation.create).find(c => c && c.campaignBudget === b.resourceName) || {};
    const end = dateOnly(owner.endDateTime), planned = dateOnly(owner.startDateTime), from = planned && planned > today ? planned : today;
    if (!end) throw new Error("A total budget needs the campaign's end date. Set the dates or switch back to a daily budget.");
    if (end < from) throw new Error("This draft's end date has passed. Change its dates before publishing.");
    const days = daysInclusive(from, end);
    if (days > TOTAL_BUDGET_MAX_DAYS) throw new Error(`Google allows total budgets for up to ${TOTAL_BUDGET_MAX_DAYS} days; this draft runs ${days} days. Shorten its dates or switch back to a daily budget.`);
    sum += fromMicros(b.totalAmountMicros) / days;
  }
  return sum;
}

module.exports = {
  BRAND_SEARCH, SEARCH_URL_SUFFIX, SALE_OCCASIONS, SEASONALITY_LIMITS, ACQUISITION_MODES, ACQUISITION_TRADEOFF, ACQUISITION_PREREQUISITE,
  ACQUISITION_CHANNELS, TOTAL_BUDGET_BIDDING, TOTAL_BUDGET_MAX_DAYS,
  buildBrandSearchOps, isBrandSearchDraft,
  gadsDateTime, saleOccasion, saleWindow, checkSaleDates, seasonalityEligible, conversionRateEstimate, seasonalityWindow, seasonalityOperation, seasonalityOverlaps,
  acquisitionEligibility, lifecycleGoalRequest, lifecycleErrorText, purchaseOnlyGoalOps,
  isTotalBudget, budgetDailyEquivalent, totalBudgetFacts, setTotalBudget, draftBudgetDaily,
  _dates: { parseDay, dateOnly, addDays, daysInclusive }
};
