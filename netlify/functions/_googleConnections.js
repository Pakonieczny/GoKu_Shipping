'use strict';

// The catalog of every Google surface this application depends on, plus the
// surfaces Google offers for this account model that are not wired up yet.
//
// Nothing here performs I/O. googleConnectionsCheck.js executes the catalog;
// tests/adwords/google-connections.cjs asserts the catalog stays complete as
// the application grows, so a newly queried resource cannot ship unverified.
//
//  used:false  → Google offers it for this account model and it is NOT wired
//                up yet. It is probed anyway so the report says whether the
//                credentials would reach it today.
//  writes:     → every probe in this file is a read. Nothing mutates.

// ── Google Ads: reporting resources ─────────────────────────────────────────
// Each probe is the smallest query that proves the resource is readable with
// the configured developer token, login-customer-id and customer-id pairing.
const RECENT = "segments.date DURING LAST_7_DAYS";

const ADS_RESOURCES = [
  { resource: "customer", used: true, family: "account",
    query: "SELECT customer.id, customer.descriptive_name, customer.currency_code, customer.time_zone FROM customer LIMIT 1",
    why: "Account identity, currency and time zone. Every report date is account-local." },
  { resource: "conversion_action", used: true, family: "conversions",
    query: "SELECT conversion_action.id, conversion_action.name, conversion_action.status, conversion_action.type, conversion_action.category, conversion_action.primary_for_goal FROM conversion_action LIMIT 200",
    why: "The conversion actions Shopify orders are attributed to. Offline upload targets one of these." },
  { resource: "campaign", used: true, family: "structure",
    query: "SELECT campaign.id, campaign.name, campaign.status, campaign.advertising_channel_type, campaign.advertising_channel_sub_type FROM campaign WHERE campaign.status != 'REMOVED' LIMIT 50",
    why: "Every campaign, including paused and zero-impression ones." },
  { resource: "campaign_budget", used: true, family: "structure",
    query: "SELECT campaign_budget.id, campaign_budget.amount_micros FROM campaign_budget LIMIT 1",
    why: "Daily budgets shown and edited in the console." },
  { resource: "ad_group", used: true, family: "structure",
    query: "SELECT ad_group.id, ad_group.name, ad_group.status FROM ad_group LIMIT 1",
    why: "Search ad groups that own image and text assets." },
  { resource: "ad_group_ad", used: true, family: "structure",
    query: "SELECT ad_group_ad.ad.id, ad_group_ad.ad.type, ad_group_ad.status FROM ad_group_ad LIMIT 1",
    why: "Responsive Search ads restored from saved versions." },
  { resource: "ad_group_criterion", used: true, family: "targeting",
    query: "SELECT ad_group_criterion.criterion_id, ad_group_criterion.type FROM ad_group_criterion LIMIT 1",
    why: "Keywords and negatives inside a saved version." },
  { resource: "campaign_criterion", used: true, family: "targeting",
    query: "SELECT campaign_criterion.criterion_id, campaign_criterion.type FROM campaign_criterion LIMIT 1",
    why: "Campaign-level negatives preserved across restores." },
  { resource: "asset", used: true, family: "creative",
    query: "SELECT asset.id, asset.type, asset.image_asset.full_size.width_pixels, asset.image_asset.full_size.height_pixels, asset.image_asset.full_size.url FROM asset WHERE asset.type = 'IMAGE' LIMIT 5",
    why: "THE SHAPE SOURCE. width_pixels/height_pixels are what tie a statistic to an aspect ratio." },
  { resource: "asset_group", used: true, family: "creative",
    query: "SELECT asset_group.id, asset_group.name, asset_group.status, asset_group.final_urls FROM asset_group LIMIT 1",
    why: "Performance Max asset groups and their product destinations." },
  { resource: "asset_group_asset", used: true, family: "creative",
    query: "SELECT asset_group_asset.asset, asset_group_asset.field_type, asset_group_asset.primary_status, asset.image_asset.full_size.width_pixels, asset.image_asset.full_size.height_pixels, metrics.impressions, metrics.clicks, metrics.conversions FROM asset_group_asset WHERE " + RECENT + " LIMIT 1",
    why: "Per-image Performance Max statistics joined to the image's own pixel dimensions." },
  { resource: "ad_group_asset", used: true, family: "creative",
    query: "SELECT ad_group_asset.asset, ad_group_asset.field_type, ad_group_asset.status FROM ad_group_asset WHERE ad_group_asset.field_type = 'IMAGE' LIMIT 1",
    why: "Search image assets that carry their own statistics." },
  { resource: "campaign_asset", used: true, family: "creative",
    query: "SELECT campaign_asset.asset, campaign_asset.field_type, campaign_asset.status FROM campaign_asset LIMIT 1",
    why: "Campaign-level assets such as sitelinks and logos." },
  { resource: "ad_group_ad_asset_view", used: true, family: "creative",
    query: "SELECT ad_group_ad_asset_view.field_type, ad_group_ad_asset_view.performance_label, metrics.impressions, metrics.clicks FROM ad_group_ad_asset_view WHERE " + RECENT + " LIMIT 1",
    why: "Google's own per-asset performance label for Responsive Search assets." },
  { resource: "asset_group_signal", used: true, family: "targeting",
    query: "SELECT asset_group_signal.asset_group, asset_group_signal.audience.audience FROM asset_group_signal LIMIT 1",
    why: "Audience and search-theme signals attached to an asset group." },
  { resource: "asset_group_listing_group_filter", used: true, family: "shopping",
    query: "SELECT asset_group_listing_group_filter.resource_name, asset_group_listing_group_filter.type FROM asset_group_listing_group_filter LIMIT 1",
    why: "The product partition that decides which offers an asset group sells." },
  { resource: "asset_group_product_group_view", used: true, family: "shopping",
    query: "SELECT asset_group_product_group_view.resource_name, metrics.impressions FROM asset_group_product_group_view WHERE " + RECENT + " LIMIT 1",
    why: "Product-group level results inside Performance Max." },
  { resource: "shopping_performance_view", used: true, family: "shopping",
    query: "SELECT segments.product_item_id, segments.product_title, metrics.impressions, metrics.clicks, metrics.conversions_value FROM shopping_performance_view WHERE " + RECENT + " LIMIT 1",
    why: "Paid results per Shopify offer ID — the Ads side of the Merchant join." },
  { resource: "shopping_product", used: true, family: "shopping",
    query: "SELECT shopping_product.item_id, shopping_product.title, shopping_product.availability FROM shopping_product LIMIT 1",
    why: "The offers Google Ads can currently serve, straight from the linked feed." },
  { resource: "search_term_view", used: true, family: "demand",
    query: "SELECT search_term_view.search_term, metrics.impressions FROM search_term_view WHERE " + RECENT + " LIMIT 1",
    why: "Actual queries that triggered Search ads." },
  { resource: "campaign_search_term_view", used: true, family: "demand",
    query: "SELECT campaign_search_term_view.search_term, metrics.impressions FROM campaign_search_term_view WHERE " + RECENT + " LIMIT 1",
    why: "Campaign-scoped search terms, including Performance Max." },
  { resource: "campaign_search_term_insight", used: true, family: "demand",
    query: "SELECT campaign_search_term_insight.category_label, metrics.impressions FROM campaign_search_term_insight WHERE " + RECENT + " LIMIT 1",
    why: "Grouped search themes where individual terms are withheld." },
  { resource: "keyword_view", used: true, family: "demand",
    query: "SELECT ad_group_criterion.keyword.text, metrics.impressions FROM keyword_view WHERE " + RECENT + " LIMIT 1",
    why: "Keyword-level results for Search." },
  { resource: "audience", used: true, family: "targeting",
    query: "SELECT audience.id, audience.name, audience.status FROM audience LIMIT 1",
    why: "Reusable audiences attached as Performance Max signals." },
  { resource: "ad_group_audience_view", used: true, family: "demand",
    query: "SELECT ad_group_audience_view.resource_name, metrics.impressions FROM ad_group_audience_view WHERE " + RECENT + " LIMIT 1",
    why: "Which audiences actually produced impressions." },
  { resource: "geographic_view", used: true, family: "demand",
    query: "SELECT geographic_view.country_criterion_id, metrics.impressions FROM geographic_view WHERE " + RECENT + " LIMIT 1",
    why: "Where the ads served." },
  { resource: "geo_target_constant", used: true, family: "targeting",
    query: "SELECT geo_target_constant.id, geo_target_constant.name FROM geo_target_constant LIMIT 1",
    why: "Names for the geo IDs used in targeting." },
  { resource: "landing_page_view", used: true, family: "demand",
    query: "SELECT landing_page_view.unexpanded_final_url, metrics.impressions FROM landing_page_view WHERE " + RECENT + " LIMIT 1",
    why: "The Shopify URLs Google actually sent traffic to." },
  { resource: "change_event", used: true, family: "audit",
    query: "SELECT change_event.change_date_time, change_event.change_resource_type FROM change_event WHERE change_event.change_date_time DURING LAST_7_DAYS LIMIT 1",
    why: "Who changed what, including changes made outside this app." },
  { resource: "recommendation", used: true, family: "audit",
    query: "SELECT recommendation.type, recommendation.resource_name FROM recommendation LIMIT 1",
    why: "Google's own optimization recommendations." },
  { resource: "you_tube_video_upload", used: true, family: "video",
    query: "SELECT you_tube_video_upload.resource_name, you_tube_video_upload.video_id, you_tube_video_upload.status FROM you_tube_video_upload LIMIT 5",
    why: "Videos this app uploaded to YouTube through Google Ads, and their processing status." },

  // ── Available, not yet wired ──────────────────────────────────────────────
  { resource: "video", used: true, family: "video",
    query: "SELECT video.id, video.title, video.duration_millis, metrics.impressions, metrics.video_views, metrics.video_quartile_p25_rate, metrics.video_quartile_p50_rate, metrics.video_quartile_p75_rate, metrics.video_quartile_p100_rate FROM video WHERE " + RECENT + " LIMIT 10",
    why: "Per-video statistics: views, view rate, watch-through quartiles and duration, read by the campaign analysis." },
  { resource: "asset_field_type_view", used: false, family: "creative",
    query: "SELECT asset_field_type_view.field_type, metrics.impressions, metrics.clicks, metrics.conversions FROM asset_field_type_view WHERE " + RECENT + " LIMIT 5",
    why: "Results grouped by the slot an asset filled (MARKETING_IMAGE, SQUARE_MARKETING_IMAGE, PORTRAIT_MARKETING_IMAGE, YOUTUBE_VIDEO, LOGO) — the closest Google gets to per-shape reporting." },
  { resource: "asset_group_top_combination_view", used: false, family: "creative",
    query: "SELECT asset_group_top_combination_view.asset_group_top_combinations FROM asset_group_top_combination_view WHERE " + RECENT + " LIMIT 1",
    why: "Which asset combinations Google actually assembled and served together." },
  { resource: "detail_placement_view", used: false, family: "demand",
    query: "SELECT detail_placement_view.placement_type, detail_placement_view.display_name, metrics.impressions FROM detail_placement_view WHERE " + RECENT + " LIMIT 1",
    why: "The exact YouTube channels, videos, apps and sites the ads appeared on." },
  { resource: "campaign_asset_set", used: false, family: "shopping",
    query: "SELECT campaign_asset_set.asset_set, campaign_asset_set.status FROM campaign_asset_set LIMIT 1",
    why: "Asset sets (business locations, page feeds) bound to a campaign." },
  { resource: "product_link", used: false, family: "account",
    query: "SELECT product_link.resource_name, product_link.type, product_link.merchant_center.merchant_center_id FROM product_link LIMIT 10",
    why: "THE MERCHANT LINK ITSELF. Proves Google Ads and Merchant Center are joined for this customer." }
];

// ── Google Ads: schema introspection ────────────────────────────────────────
// GoogleAdsFieldService answers, in the live account and the configured API
// version, whether a field exists and what it may be selected beside. It is the
// only way to prove a statistic can be segmented the way the console claims.
const ADS_FIELDS = [
  { field: "segments.device", why: "Mobile versus desktop. selectable_with names every resource that can be split by device." },
  { field: "segments.ad_network_type", why: "Search, Display, YouTube surfaces." },
  { field: "segments.date", why: "Account-local reporting dates." },
  { field: "segments.product_item_id", why: "Joins paid results to a Shopify offer ID." },
  { field: "segments.conversion_action_category", why: "Separates purchases from other conversions." },
  { field: "asset.image_asset.full_size.width_pixels", why: "Image width — one half of the aspect ratio a statistic belongs to." },
  { field: "asset.image_asset.full_size.height_pixels", why: "Image height — the other half." },
  { field: "asset_group_asset.primary_status", why: "Whether Google is actually able to serve this asset." },
  { field: "metrics.impressions", why: "Baseline delivery." },
  { field: "metrics.conversions_value", why: "Attributed revenue." },
  { field: "metrics.video_views", why: "Video views — required before any video statistic can be reported." },
  { field: "metrics.video_quartile_p100_rate", why: "Watch-through rate; proves video reporting depth is available." }
];

// Resources whose statistics the operator expects to split by device.
const DEVICE_SPLIT_WANTED = ["asset_group_asset", "ad_group_asset", "shopping_performance_view", "campaign", "asset_field_type_view", "video"];

// ── Merchant API ────────────────────────────────────────────────────────────
const MERCHANT_PROBES = [
  { key: "account", used: true, method: "GET", path: a => "accounts/v1/accounts/" + a,
    why: "The Merchant Center account itself." },
  { key: "products", used: true, method: "GET", path: a => "products/v1/accounts/" + a + "/products?pageSize=1",
    why: "Processed products Google will serve." },
  { key: "dataSources", used: true, method: "GET", path: a => "datasources/v1/accounts/" + a + "/dataSources?pageSize=25",
    why: "Which feed owns each product. A Shopify-managed source must not be overwritten by this app." },
  { key: "report_product_view", used: true, method: "POST", path: a => "reports/v1/accounts/" + a + "/reports:search",
    body: { query: "SELECT offer_id, id, channel, feed_label, language_code FROM product_view LIMIT 1", pageSize: 1 },
    why: "Resolves a Shopify offer ID to its Merchant product identity." },
  { key: "report_product_performance", used: true, method: "POST", path: a => "reports/v1/accounts/" + a + "/reports:search",
    body: { query: "SELECT offer_id, clicks, impressions, conversions, conversion_value FROM product_performance_view WHERE date DURING LAST_7_DAYS LIMIT 1", pageSize: 1 },
    why: "Free and paid results per offer, the Merchant side of the Shopify join." },
  { key: "accountIssues", used: false, method: "GET", path: a => "accounts/v1/accounts/" + a + "/issues",
    why: "Account-level blockers that stop products serving. Not surfaced in the app today." },
  { key: "productIssues", used: false, method: "POST", path: a => "reports/v1/accounts/" + a + "/reports:search",
    body: { query: "SELECT offer_id, item_issues FROM product_view WHERE item_issues IS NOT NULL LIMIT 5", pageSize: 5 },
    why: "Per-offer disapprovals. A disapproved offer silently earns nothing." },
  { key: "conversionSources", used: false, method: "GET", path: a => "conversions/v1/accounts/" + a + "/conversionSources?pageSize=25",
    why: "THE SHOPIFY CONVERSION LINK. Google's record of which site sends Merchant conversions." },
  { key: "accountRelationships", used: false, method: "GET", path: a => "accounts/v1/accounts/" + a + "/relationships",
    why: "Which Google Ads accounts this Merchant account is joined to." },
  { key: "accountServices", used: false, method: "GET", path: a => "accounts/v1/accounts/" + a + "/services",
    why: "The service agreements behind those links, including campaign management." },
  { key: "promotions", used: false, method: "GET", path: a => "promotions/v1/accounts/" + a + "/promotions?pageSize=1",
    why: "Merchant promotions. Never used, so sale messaging never reaches Shopping surfaces." },
  { key: "onlineReturnPolicies", used: false, method: "GET", path: a => "accounts/v1/accounts/" + a + "/onlineReturnPolicies?pageSize=1",
    why: "Return policy shown beside the offer; affects Shopping eligibility." },
  { key: "shippingSettings", used: false, method: "GET", path: a => "accounts/v1/accounts/" + a + "/shippingSettings",
    why: "Shipping settings that decide whether an offer can serve at all." },
  { key: "autofeedSettings", used: false, method: "GET", path: a => "accounts/v1/accounts/" + a + "/autofeedSettings",
    why: "Whether Google is crawling the Shopify store directly as a feed source." }
];

// Report views that belong to the Merchant API's own query language, not to
// Google Ads. They appear in FROM clauses too, so coverage checks must know them.
const MERCHANT_REPORT_VIEWS = ["product_view", "product_performance_view"];

// ── Other Google hosts ──────────────────────────────────────────────────────
const OTHER_HOSTS = [
  { key: "oauth", host: "oauth2.googleapis.com", used: true, why: "Refresh-token exchange for every Google call." },
  { key: "ads", host: "googleads.googleapis.com", used: true, why: "Google Ads API and the resumable YouTube video upload endpoint." },
  { key: "merchant", host: "merchantapi.googleapis.com", used: true, why: "Merchant Center products and reports." },
  { key: "datamanager", host: "datamanager.googleapis.com", used: true, why: "Offline conversion upload for Shopify orders." },
  { key: "gemini", host: "generativelanguage.googleapis.com", used: true, why: "Image and video generation." },
  { key: "gmail", host: "gmail.googleapis.com", used: true, why: "Order mail, unrelated to advertising." },
  { key: "storage", host: "storage.googleapis.com", used: true, why: "Creative asset storage." },
  { key: "firebasestorage", host: "firebasestorage.googleapis.com", used: true, why: "Saved design and creative bytes." },
  { key: "visionWarehouse", host: "warehouse-visionai.googleapis.com", used: true, why: "Vision AI Warehouse, used by visionWarehouse.js for image indexing and search." },
  { key: "youtube", host: "youtube.googleapis.com", used: false, why: "YouTube Data API. Videos are uploaded through Google Ads but their YouTube-side processing state, thumbnail and public metadata are never read." }
];

// OAuth scopes decide which of the hosts above a refresh token may actually
// reach. A token missing a scope fails at call time, not at consent time, so
// the granted set is checked directly against tokeninfo.
const REQUIRED_SCOPES = [
  { scope: "https://www.googleapis.com/auth/adwords", unlocks: "Google Ads API", required: true },
  { scope: "https://www.googleapis.com/auth/content", unlocks: "Merchant Center", required: false },
  { scope: "https://www.googleapis.com/auth/datamanager", unlocks: "offline conversion upload", required: false },
  { scope: "https://www.googleapis.com/auth/youtube.readonly", unlocks: "YouTube video processing state", required: false },
  { scope: "https://www.googleapis.com/auth/cloud-platform", unlocks: "Vision AI Warehouse and Cloud Storage", required: false }
];

// ── Google's published creative requirements ────────────────────────────────
// Sources are recorded beside each entry in brites-ad-format-policy.js. These
// are the requirements a publication must satisfy, not house style.
// brites-ad-format-policy.js is the single record of Google's published asset
// requirements: the creative engine, the publication gate and this checker all
// read the same numbers, so a report can never disagree with what is enforced.
const REQUIRED_IMAGE_FORMATS = require("../../brites-ad-format-policy").images;

const REQUIRED_VIDEO_FORMATS = [
  { key: "landscape", ratio: 16 / 9, minSeconds: 10, why: "In-stream and in-feed YouTube." },
  { key: "square", ratio: 1, minSeconds: 10, why: "Feed surfaces." },
  { key: "portrait", ratio: 9 / 16, minSeconds: 10, why: "Shorts and vertical feeds. Omitting it removes vertical inventory entirely." }
];

// ── Result shaping ──────────────────────────────────────────────────────────
function row(name, status, detail, extra) {
  return Object.assign({ name: name, status: status, detail: String(detail == null ? "" : detail).slice(0, 600) }, extra || {});
}
const ok = (name, detail, extra) => row(name, "ok", detail, extra);
const fail = (name, detail, extra) => row(name, "FAIL", detail, extra);
const warn = (name, detail, extra) => row(name, "warn", detail, extra);
const skip = (name, detail, extra) => row(name, "skipped", detail, extra);

// A probe that a credential never reached is "skipped", never "ok". Only an
// observed Google response can turn a row green.
function summarize(sections) {
  const all = [].concat.apply([], sections.map(s => s.rows || []));
  const count = status => all.filter(r => r.status === status).length;
  return {
    total: all.length, ok: count("ok"), failed: count("FAIL"), warned: count("warn"), skipped: count("skipped"),
    allGreen: all.length > 0 && all.every(r => r.status === "ok")
  };
}

// Google returns a long error envelope; the first errorCode is what a reader acts on.
function adsErrorCode(data) {
  const details = ((data || {}).error || {}).details || [];
  for (const d of details) for (const e of d.errors || []) {
    const code = e.errorCode && Object.keys(e.errorCode)[0];
    if (code) return e.errorCode[code] + " (" + code + ")";
  }
  return ((data || {}).error || {}).status || null;
}

const REMEDIES = [
  [/DEVELOPER_TOKEN_NOT_APPROVED|DEVELOPER_TOKEN_PROHIBITED/i, "The developer token is not approved for production. Apply for Basic access in the manager account's API Center."],
  [/USER_PERMISSION_DENIED/i, "login-customer-id and customer-id do not pair, or the manager link was never accepted. Check the invitation in Google Ads."],
  [/CUSTOMER_NOT_ENABLED/i, "The Google Ads account is not active — usually unfinished billing."],
  [/NOT_ADS_USER/i, "The OAuth refresh token belongs to a Google account with no access to this Ads account."],
  [/UNRECOGNIZED_FIELD|invalid.*field|Unrecognized/i, "The field does not exist in this API version. Confirm it in GoogleAdsFieldService before querying it."],
  [/CUSTOMER_NOT_ALLOWLISTED/i, "This account is not allowlisted for the feature. Request access through your Google representative."],
  [/PERMISSION_DENIED/i, "The OAuth scope or account grant does not cover this API. Re-consent with the required scope."],
  [/^401|UNAUTHENTICATED/i, "The access token was rejected. The refresh token is revoked or the client secret was rotated."],
  [/^404/i, "The resource or account ID does not exist for these credentials."]
];
function remedyFor(detail) {
  const text = String(detail || "");
  for (const [pattern, advice] of REMEDIES) if (pattern.test(text)) return advice;
  return null;
}

module.exports = {
  ADS_RESOURCES, ADS_FIELDS, DEVICE_SPLIT_WANTED, MERCHANT_PROBES, MERCHANT_REPORT_VIEWS, OTHER_HOSTS, REQUIRED_SCOPES,
  REQUIRED_IMAGE_FORMATS, REQUIRED_VIDEO_FORMATS,
  row, ok, fail, warn, skip, summarize, adsErrorCode, remedyFor, RECENT
};
