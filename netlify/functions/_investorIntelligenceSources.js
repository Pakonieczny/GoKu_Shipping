/* Investor_AI — public company-intelligence source adapters.
 *
 * The browser never supplies a URL. Every network destination is registered
 * here and still passes _investorFetch's DNS/SSRF/content guards. GDELT is a
 * discovery index, never confirmation. Government, company and stakeholder
 * feeds are direct evidence with explicit independence and reliability
 * metadata. Public specialist publications provide context, not automatic
 * authority.
 */
"use strict";

const A = require("./_investorAdmin");
const E = require("./_investorEvidence");
const { fetchPublic, normalizedHash } = require("./_investorFetch");
const { visibleText } = require("./_investorVisibleText");

const DAY_MS = 864e5;
const MAX_FOCUS = 24;

const SOURCE_REGISTRY = {
  "gdelt.discovery": {
    source_id: "gdelt.discovery", name: "GDELT global public-news discovery",
    kind: "gdelt", source_class: "discovery_index", tier: "C", reliability: 0.52,
    independence_group: "gdelt", discovery_only: true, cadenceSeconds: 900,
    retention_rule: "metadata_only", full_text_allowed: false,
    requiredRole: "broad_discovery",
    terms_url: "https://www.gdeltproject.org/about.html",
    notes: "Discovery across public reporting in many languages. A result is a lead, not corroboration; the originating publisher remains the evidence source.",
  },
  "gdelt.sector": {
    source_id: "gdelt.sector", name: "GDELT sector-publication discovery",
    kind: "gdelt_sector", source_class: "discovery_index", tier: "C", reliability: 0.52,
    independence_group: "gdelt", discovery_only: true, cadenceSeconds: 1800,
    retention_rule: "metadata_only", full_text_allowed: false,
    requiredRole: "sector_context",
    notes: "Bounded discovery across the company's automatically selected sector, trade-publication and industry-event domains. Originating publishers, not GDELT, are the evidence sources.",
  },
  "gdelt.relationships": {
    source_id: "gdelt.relationships", name: "GDELT company-relationship discovery",
    kind: "gdelt_relationships", source_class: "discovery_index", tier: "C", reliability: 0.52,
    independence_group: "gdelt", discovery_only: true, cadenceSeconds: 1800,
    retention_rule: "metadata_only", full_text_allowed: false,
    requiredRole: "relationship_context",
    notes: "Queries the bounded, quote-verified customer/supplier/union/regulator/product graph learned from prior documents. It remains discovery only.",
  },
  "company.direct": {
    source_id: "company.direct", name: "SEC-linked company public site",
    kind: "company_direct", source_class: "company_primary", tier: "A", reliability: 0.80,
    independence_group: "company", discovery_only: false, cadenceSeconds: 1800,
    retention_rule: "public_page_snapshot", full_text_allowed: true,
    requiredRole: "company_primary",
    notes: "Fetched only from domains linked by the issuer's SEC filings or explicitly operator-verified. Company claims remain self-interested and do not count as independent corroboration.",
  },
  "federal.register": {
    source_id: "federal.register", name: "Federal Register company search",
    kind: "federal_register", source_class: "government_primary", tier: "A", reliability: 0.97,
    independence_group: "us_federal_register", discovery_only: false, cadenceSeconds: 1800,
    retention_rule: "government_public_domain_retain", full_text_allowed: true,
    requiredRole: "regulatory", terms_url: "https://www.federalregister.gov/developers/documentation/api/v1",
    notes: "Rules, notices and public-inspection material matched to company aliases; legal reliance still points to the official edition.",
  },
  "usaspending.awards": {
    source_id: "usaspending.awards", name: "USAspending federal awards",
    kind: "usaspending", source_class: "government_primary", tier: "A", reliability: 0.98,
    independence_group: "usaspending", discovery_only: false, cadenceSeconds: 21600,
    retention_rule: "government_public_data_retain", full_text_allowed: true,
    requiredRole: "government_awards", terms_url: "https://api.usaspending.gov/",
    notes: "Prime award records reveal federal contract obligations and modifications; award amount is not treated as revenue.",
  },
  "dol.releases": {
    source_id: "dol.releases", name: "U.S. Department of Labor releases",
    kind: "feed", url: "https://www.dol.gov/rss/releases.xml",
    source_class: "government_primary", tier: "A", reliability: 0.95,
    independence_group: "us_dol", discovery_only: false, cadenceSeconds: 1800,
    retention_rule: "government_public_domain_retain", full_text_allowed: true,
    requiredRole: "labor_regulatory", notes: "Official labor, wage, safety and enforcement announcements.",
  },
  "nlrb.releases": {
    source_id: "nlrb.releases", name: "National Labor Relations Board releases",
    kind: "feed", url: "https://www.nlrb.gov/rss/rssPressReleases.xml",
    source_class: "government_primary", tier: "A", reliability: 0.96,
    independence_group: "nlrb", discovery_only: false, cadenceSeconds: 1800,
    retention_rule: "government_public_domain_retain", full_text_allowed: true,
    requiredRole: "labor_regulatory", notes: "Official nationally significant labor-case and Board announcements.",
  },
  "ftc.releases": {
    source_id: "ftc.releases", name: "Federal Trade Commission releases",
    kind: "feed", url: "https://www.ftc.gov/feeds/press-release.xml",
    source_class: "government_primary", tier: "A", reliability: 0.96,
    independence_group: "ftc", discovery_only: false, cadenceSeconds: 1800,
    retention_rule: "government_public_domain_retain", full_text_allowed: true,
    requiredRole: "competition_consumer", notes: "Official competition, consumer-protection and enforcement announcements.",
  },
  "doj.news": {
    source_id: "doj.news", name: "U.S. Department of Justice news and enforcement",
    kind: "feed", url: "https://www.justice.gov/news/rss?m=1",
    source_class: "government_primary", tier: "A", reliability: 0.96,
    independence_group: "us_doj", discovery_only: false, cadenceSeconds: 1800,
    retention_rule: "government_public_domain_retain", full_text_allowed: true,
    requiredRole: "legal_regulatory",
    notes: "Official antitrust, fraud, sanctions, False Claims Act, cybercrime and other enforcement announcements matched to resolved issuer aliases.",
  },
  "nws.alerts": {
    source_id: "nws.alerts", name: "National Weather Service active alerts",
    kind: "nws_alerts", source_class: "government_primary", tier: "A", reliability: 0.98,
    independence_group: "nws", discovery_only: false, cadenceSeconds: 900,
    retention_rule: "government_public_data_retain", full_text_allowed: true,
    requiredRole: null, terms_url: "https://www.weather.gov/documentation/services-web-API",
    notes: "Active alerts are queried only for U.S. states supported by quote-grounded company operating exposure. An alert is a geographic hazard, not proof of company damage.",
  },
  "faa.press": {
    source_id: "faa.press", name: "FAA press releases",
    kind: "feed", url: "https://www.faa.gov/newsroom/press_releases/rss",
    source_class: "regulator_primary", tier: "A", reliability: 0.98,
    independence_group: "faa", discovery_only: false, cadenceSeconds: 900,
    retention_rule: "government_public_domain_retain", full_text_allowed: true,
    requiredRole: "aviation_regulator", notes: "Official U.S. aviation safety, enforcement and certification announcements.",
  },
  "ntsb.press": {
    source_id: "ntsb.press", name: "NTSB press releases",
    kind: "feed",
    url: "https://www.ntsb.gov/_layouts/feed.aspx?page=674e62a9-4f3b-4058-846b-150bc1c21aa0&pageurl=%2FPages%2FRSS-Feed-Page.aspx&web=%2F&wp=5c78a16b-edcb-475c-8a9c-93c00783cd61&xsl=1",
    source_class: "investigator_primary", tier: "A", reliability: 0.98,
    independence_group: "ntsb", discovery_only: false, cadenceSeconds: 900,
    retention_rule: "government_public_domain_retain", full_text_allowed: true,
    requiredRole: "aviation_investigation", notes: "Official independent transportation-safety investigation announcements.",
  },
  "ntsb.investigations": {
    source_id: "ntsb.investigations", name: "NTSB investigations",
    kind: "feed",
    url: "https://www.ntsb.gov/_layouts/feed.aspx?page=674e62a9-4f3b-4058-846b-150bc1c21aa0&pageurl=%2FPages%2FRSS-Feed-Page.aspx&web=%2F&wp=a19255e2-c8e3-41fd-8c99-f8bc0453cb58&xsl=1",
    source_class: "investigator_primary", tier: "A", reliability: 0.98,
    independence_group: "ntsb", discovery_only: false, cadenceSeconds: 1800,
    retention_rule: "government_public_domain_retain", full_text_allowed: true,
    requiredRole: "aviation_investigation", notes: "Investigation openings and updates; shares an independence group with NTSB press releases.",
  },
  "war.contracts": {
    source_id: "war.contracts", name: "U.S. defense contract announcements",
    kind: "feed", url: "https://www.war.gov/DesktopModules/ArticleCS/RSS.ashx?ContentType=400&Site=945&max=10",
    source_class: "government_primary", tier: "A", reliability: 0.97,
    independence_group: "us_defense", discovery_only: false, cadenceSeconds: 1800,
    retention_rule: "government_public_domain_retain", full_text_allowed: true,
    requiredRole: "defense_contracts", notes: "Official daily announcements for material U.S. defense contracts.",
  },
  "aviationweek.context": {
    source_id: "aviationweek.context", name: "Aviation Week public industry context",
    kind: "html_index", url: "https://aviationweek.com/awn-rss/feed",
    source_class: "specialist_publication", tier: "B", reliability: 0.80,
    independence_group: "aviation_week", discovery_only: false, cadenceSeconds: 1800,
    retention_rule: "metadata_and_public_summary_only", full_text_allowed: false,
    requiredRole: "aviation_specialist", notes: "Specialist aviation and defense reporting. Paywalled body text is never bypassed or retained.",
  },
  "farnborough.news": {
    source_id: "farnborough.news", name: "Farnborough International Airshow official news",
    kind: "html_index", url: "https://www.farnboroughairshow.com/resources/news-blogs/",
    source_class: "industry_event_primary", tier: "A", reliability: 0.86,
    independence_group: "farnborough_airshow", discovery_only: false, cadenceSeconds: 1800,
    retention_rule: "public_page_snapshot", full_text_allowed: true,
    requiredRole: "aviation_airshows", notes: "Official airshow announcement channel; deal parties remain the primary sources for contractual terms.",
  },
  "dubai.airshow.news": {
    source_id: "dubai.airshow.news", name: "Dubai Airshow official news",
    kind: "html_index", url: "https://www.dubaiairshow.aero/usefulinfo/info/news/",
    source_class: "industry_event_primary", tier: "A", reliability: 0.86,
    independence_group: "dubai_airshow", discovery_only: false, cadenceSeconds: 1800,
    retention_rule: "public_page_snapshot", full_text_allowed: true,
    requiredRole: "aviation_airshows", notes: "Official event news used to surface public deal announcements; not a substitute for counterparty confirmation.",
  },
  "fda.news": {
    source_id: "fda.news", name: "FDA press announcements and safety actions",
    kind: "html_index", url: "https://www.fda.gov/news-events/fda-newsroom/press-announcements",
    source_class: "regulator_primary", tier: "A", reliability: 0.97,
    independence_group: "fda", discovery_only: false, cadenceSeconds: 1800,
    retention_rule: "government_public_domain_retain", full_text_allowed: true,
    requiredRole: "sector_regulator", notes: "Selected automatically for health, pharmaceutical, biotech, food and medical-device issuers.",
  },
  "clinicaltrials.studies": {
    source_id: "clinicaltrials.studies", name: "ClinicalTrials.gov sponsor records",
    kind: "clinicaltrials", source_class: "public_registry", tier: "B", reliability: 0.86,
    independence_group: "clinicaltrials", discovery_only: false, cadenceSeconds: 21600,
    retention_rule: "government_public_data_retain", full_text_allowed: true,
    requiredRole: "sector_registry", terms_url: "https://clinicaltrials.gov/data-api/about-api",
    notes: "Structured public study records selected for healthcare and biotech. Sponsor-reported status is evidence of the registry record, not independent proof of efficacy or safety.",
  },
  "cisa.news": {
    source_id: "cisa.news", name: "CISA cybersecurity alerts and advisories",
    kind: "feed", url: "https://www.cisa.gov/cybersecurity-advisories/all.xml",
    source_class: "government_primary", tier: "A", reliability: 0.96,
    independence_group: "cisa", discovery_only: false, cadenceSeconds: 1800,
    retention_rule: "government_public_domain_retain", full_text_allowed: true,
    requiredRole: "sector_regulator", notes: "Selected for software, platforms, semiconductors and hardware; relevant vendor mentions are matched to the resolved entity.",
  },
  "epa.news": {
    source_id: "epa.news", name: "EPA news and enforcement releases",
    kind: "html_index", url: "https://www.epa.gov/newsroom/browse-news-releases",
    source_class: "regulator_primary", tier: "A", reliability: 0.96,
    independence_group: "epa", discovery_only: false, cadenceSeconds: 3600,
    retention_rule: "government_public_domain_retain", full_text_allowed: true,
    requiredRole: "sector_regulator", notes: "Selected for energy, power, materials and industrial issuers.",
  },
  "fdic.news": {
    source_id: "fdic.news", name: "FDIC press releases",
    kind: "html_index", url: "https://www.fdic.gov/news/press-releases",
    source_class: "regulator_primary", tier: "A", reliability: 0.97,
    independence_group: "fdic", discovery_only: false, cadenceSeconds: 3600,
    retention_rule: "government_public_domain_retain", full_text_allowed: true,
    requiredRole: "sector_regulator", notes: "Selected for banking and deposit-taking financial issuers.",
  },
  "federalreserve.press": {
    source_id: "federalreserve.press", name: "Federal Reserve press releases",
    kind: "feed", url: "https://www.federalreserve.gov/feeds/press_all.xml",
    source_class: "regulator_primary", tier: "A", reliability: 0.97,
    independence_group: "federal_reserve", discovery_only: false, cadenceSeconds: 3600,
    retention_rule: "government_public_domain_retain", full_text_allowed: true,
    requiredRole: "sector_regulator",
    notes: "Official monetary-policy, supervision, enforcement and banking announcements; provides an independent public financial-regulation lane alongside FDIC.",
  },
  "cpsc.recalls": {
    source_id: "cpsc.recalls", name: "U.S. CPSC recalls",
    kind: "html_index", url: "https://www.cpsc.gov/Recalls",
    source_class: "regulator_primary", tier: "A", reliability: 0.96,
    independence_group: "cpsc", discovery_only: false, cadenceSeconds: 3600,
    retention_rule: "government_public_domain_retain", full_text_allowed: true,
    requiredRole: "sector_regulator", notes: "Selected for consumer-product issuers.",
  },
};

/* Routing is by economic activity, never ticker. Every eligible issuer goes
 * through the same resolver; SIC detail can refine the broad roster sector.
 * Publication/event domains are queried through GDELT as discovery leads and
 * therefore never become automatic corroboration. */
const SECTOR_PACKS = {
  semi: { key: "semiconductors", sourceIds: ["cisa.news"],
    topics: ["fab", "foundry", "wafer", "export controls", "chip equipment", "customer concentration"],
    publicationDomains: ["eetimes.com", "semiengineering.com", "theregister.com"],
    eventDomains: ["semiconwest.org", "ces.tech"] },
  hw: { key: "technology_hardware", sourceIds: ["cisa.news", "cpsc.recalls"],
    topics: ["product launch", "recall", "component shortage", "channel inventory", "cyber vulnerability"],
    publicationDomains: ["eetimes.com", "theregister.com", "tomshardware.com"],
    eventDomains: ["ces.tech", "computextaipei.com.tw"] },
  sw: { key: "software", sourceIds: ["cisa.news"],
    topics: ["cyber vulnerability", "outage", "renewal", "seat growth", "government contract", "antitrust"],
    publicationDomains: ["theregister.com", "darkreading.com", "cyberscoop.com"],
    eventDomains: ["rsaconference.com", "blackhat.com"] },
  plat: { key: "internet_platforms", sourceIds: ["cisa.news", "ftc.releases"],
    topics: ["antitrust", "privacy", "content policy", "advertising demand", "outage", "cyber vulnerability"],
    publicationDomains: ["theregister.com", "adweek.com", "digiday.com"],
    eventDomains: ["ces.tech", "canneslions.com"] },
  health: { key: "healthcare", sourceIds: ["fda.news", "clinicaltrials.studies", "ftc.releases"],
    topics: ["approval", "clinical trial", "recall", "reimbursement", "hospital demand", "safety alert"],
    publicationDomains: ["statnews.com", "fiercehealthcare.com", "medtechdive.com"],
    eventDomains: ["himss.org", "jpmhealthcareconference.com"] },
  pharma: { key: "pharmaceuticals_biotech", sourceIds: ["fda.news", "clinicaltrials.studies", "ftc.releases"],
    topics: ["clinical trial", "FDA approval", "complete response letter", "patent", "recall", "label expansion"],
    publicationDomains: ["statnews.com", "fiercepharma.com", "biopharmadive.com"],
    eventDomains: ["bio.org", "jpmhealthcareconference.com"] },
  energy: { key: "energy", sourceIds: ["epa.news"],
    topics: ["production", "permit", "spill", "refinery outage", "pipeline", "commodity exposure", "sanctions"],
    publicationDomains: ["rigzone.com", "oilprice.com", "energyintel.com"],
    eventDomains: ["ceraweek.com", "adipec.com"] },
  power: { key: "utilities_power", sourceIds: ["epa.news"],
    topics: ["rate case", "grid reliability", "plant outage", "fuel cost", "permit", "capacity auction"],
    publicationDomains: ["utilitydive.com", "powermag.com", "renewableenergyworld.com"],
    eventDomains: ["distributech.com", "ceraweek.com"] },
  mat: { key: "materials", sourceIds: ["epa.news", "dol.releases"],
    topics: ["mine", "mill outage", "permit", "tariff", "commodity price", "labor dispute", "shipment"],
    publicationDomains: ["mining.com", "fastmarkets.com", "recyclingtoday.com"],
    eventDomains: ["minexpo.com", "worldsteel.org"] },
  indus: { key: "industrials", sourceIds: ["epa.news", "dol.releases"],
    topics: ["order", "backlog", "factory", "strike", "supplier", "safety", "government contract"],
    publicationDomains: ["industryweek.com", "manufacturingdive.com", "supplychaindive.com"],
    eventDomains: ["hannovermesse.de", "ces.tech"] },
  fin: { key: "financial_services", sourceIds: ["fdic.news", "federalreserve.press", "ftc.releases"],
    topics: ["capital ratio", "deposit", "credit losses", "liquidity", "enforcement", "rate exposure", "cyber incident"],
    publicationDomains: ["americanbanker.com", "bankingdive.com", "risk.net"],
    eventDomains: ["sibos.com", "money2020.com"] },
  cons: { key: "consumer", sourceIds: ["cpsc.recalls", "ftc.releases"],
    topics: ["recall", "same-store sales", "pricing", "inventory", "consumer demand", "labor", "store closure"],
    publicationDomains: ["retaildive.com", "restaurantbusinessonline.com", "modernretail.co"],
    eventDomains: ["nrfbigshow.nrf.com", "ces.tech"] },
  other: { key: "general", sourceIds: [], topics: ["regulation", "labor", "customer", "supplier", "contract", "safety"],
    publicationDomains: [], eventDomains: [] },
};

const SIC_PACKS = [
  { min: 3720, max: 3769, key: "aerospace_defense",
    sourceIds: ["faa.press", "ntsb.press", "ntsb.investigations", "war.contracts",
      "aviationweek.context", "farnborough.news", "dubai.airshow.news"],
    topics: ["aircraft order", "delivery", "certification", "grounding", "airworthiness", "strike", "defense award"],
    publicationDomains: ["aviationweek.com", "flightglobal.com", "ainonline.com", "defensenews.com"],
    eventDomains: ["farnboroughairshow.com", "dubaiairshow.aero"],
    requiredRoles: ["aviation_regulator", "aviation_investigation", "defense_contracts"] },
];

const CORE_SOURCE_IDS = ["gdelt.discovery", "federal.register", "usaspending.awards",
  "dol.releases", "nlrb.releases", "ftc.releases", "doj.news", "nws.alerts", "company.direct", "gdelt.sector",
  "gdelt.relationships"];

function uniq(values) { return [...new Set((values || []).filter(Boolean))]; }
function cleanSymbol(value) {
  const s = String(value || "").trim().toUpperCase();
  return /^[A-Z][A-Z0-9.-]{0,9}$/.test(s) ? s : null;
}
function cleanText(value, max = 300) {
  return String(value || "").replace(/\s+/g, " ").trim().slice(0, max);
}
function stripTags(value) {
  return visibleText(value);
}
function decodeXml(value) {
  return String(value || "")
    .replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, "$1")
    .replace(/&#(\d+);/g, (_, n) => String.fromCodePoint(Number(n)))
    .replace(/&#x([0-9a-f]+);/gi, (_, n) => String.fromCodePoint(parseInt(n, 16)))
    .replace(/&amp;/gi, "&").replace(/&lt;/gi, "<").replace(/&gt;/gi, ">")
    .replace(/&quot;/gi, "\"").replace(/&apos;/gi, "'").replace(/&nbsp;/gi, " ");
}
function tag(block, names) {
  for (const name of names) {
    const m = String(block).match(new RegExp(`<${name}(?:\\s[^>]*)?>([\\s\\S]*?)<\\/${name}>`, "i"));
    if (m) return stripTags(m[1]);
  }
  return "";
}
function linkFrom(block) {
  const atom = String(block).match(/<link[^>]+(?:href|url)=["']([^"']+)["'][^>]*>/i);
  if (atom) return decodeXml(atom[1]);
  return tag(block, ["link", "guid"]);
}

function parseFeed(xml) {
  const text = String(xml || "");
  const blocks = [...text.matchAll(/<(item|entry)(?:\s[^>]*)?>([\s\S]*?)<\/\1>/gi)]
    .map((m) => m[2]);
  return blocks.map((block) => ({
    title: cleanText(tag(block, ["title"]), 500),
    summary: cleanText(tag(block, ["description", "summary", "content:encoded", "content"]), 6000),
    link: cleanText(linkFrom(block), 1200),
    updated: cleanText(tag(block, ["pubDate", "published", "updated", "dc:date"]), 100),
    accession: cleanText(tag(block, ["guid", "id"]), 500),
  })).filter((x) => x.title && x.link);
}

function profileFor(row = {}) {
  const symbol = cleanSymbol(row.symbol);
  if (!symbol) throw new Error("valid company symbol required");
  const rawName = cleanText(row.company || row.name || symbol, 160);
  const usefulName = rawName && rawName !== symbol ? rawName : null;
  const genericAliases = [usefulName, usefulName && usefulName.replace(/\b(holdings?|group|plc|limited|ltd\.?|incorporated|inc\.?|corporation|corp\.?|company|co\.?)\b/gi, "").trim()];
  if (symbol.length >= 4) genericAliases.push(symbol);
  const formerNames = (row.formerNames || []).map((x) => typeof x === "string" ? x : x && x.name);
  const aliases = uniq([...(row.aliases || []), ...formerNames, ...genericAliases])
    .map((x) => cleanText(x, 100)).filter((x) => x.length >= 3).slice(0, 16);
  const sector = SECTOR_PACKS[row.sector] || SECTOR_PACKS.other;
  const sic = Number(row.sic);
  const sicPack = SIC_PACKS.find((x) => Number.isFinite(sic) && sic >= x.min && sic <= x.max) || null;
  const sourceIds = uniq([...CORE_SOURCE_IDS, ...(sector.sourceIds || []), ...(sicPack && sicPack.sourceIds || [])]);
  const relatedEntities = (row.relatedEntities || []).filter((x) => x && x.name)
    .slice(0, 20).map((x) => ({ name: cleanText(x.name, 120), relationship: x.relationship || "other",
      confidence: Number(x.confidence) || null }));
  return {
    symbol, companyName: usefulName || symbol, sector: row.sector || "other",
    cik: row.cik || null, sic: Number.isFinite(sic) ? sic : null,
    sicDescription: cleanText(row.sicDescription, 200) || null,
    aliases, recipientNames: uniq(row.recipientNames || (usefulName ? [usefulName] : [])),
    officialDomains: uniq(row.officialDomains || []).map((x) => cleanText(x, 200).toLowerCase()),
    verifiedDomains: uniq(row.verifiedDomains || []).map((x) => cleanText(x, 200).toLowerCase()),
    relatedEntities,
    temporalExposures: Array.isArray(row.temporalExposures) ? row.temporalExposures.slice(0, 32) : [],
    sectorPack: sicPack ? sicPack.key : sector.key,
    topics: uniq([...(sector.topics || []), ...(sicPack && sicPack.topics || [])]),
    publicationDomains: uniq([...(sector.publicationDomains || []), ...(sicPack && sicPack.publicationDomains || [])]),
    eventDomains: uniq([...(sector.eventDomains || []), ...(sicPack && sicPack.eventDomains || [])]),
    sourceIds,
    requiredRoles: uniq(["broad_discovery", "sector_context", "regulatory", "company_primary",
      ...(sector.sourceIds && sector.sourceIds.length ? ["sector_regulator"] : []),
      ...(sicPack && sicPack.requiredRoles || [])]),
    curationStatus: usefulName && row.cik ? "sec_resolved" : "identifier_incomplete",
    identitySource: row.identitySource || (row.companySource === "sec_company_tickers" ? "sec_company_tickers" : null),
  };
}

async function resolveIdentity(row = {}) {
  const base = profileFor(row);
  if (!base.cik) return base;
  const cik10 = String(base.cik).padStart(10, "0");
  try {
    const res = await fetchPublic(`https://data.sec.gov/submissions/CIK${cik10}.json`, {
      sourceId: "sec.submissions", accept: ["json"], timeoutMs: 18000, maxBytes: 5 * 1024 * 1024,
    });
    const j = res.json || {};
    return profileFor({ ...row, company: j.name || row.company,
      formerNames: j.formerNames || row.formerNames, sic: j.sic || row.sic,
      sicDescription: j.sicDescription || row.sicDescription,
      aliases: uniq([...(row.aliases || []), ...((j.tickers || []).filter((x) => x !== base.symbol))]),
      identitySource: "sec_submissions_realtime" });
  } catch (error) {
    return { ...base, identityError: String(error.code || error.message).slice(0, 140) };
  }
}

const NON_ISSUER_DOMAINS = new Set(["sec.gov", "www.sec.gov", "w3.org", "www.w3.org",
  "xbrl.org", "www.xbrl.org", "prnewswire.com", "www.prnewswire.com", "businesswire.com",
  "www.businesswire.com", "globenewswire.com", "www.globenewswire.com", "linkedin.com",
  "www.linkedin.com", "facebook.com", "www.facebook.com", "twitter.com", "x.com", "youtube.com",
  "www.youtube.com"]);
const COMMON_PUBLIC_SUFFIX_PREFIXES = new Set(["ac", "co", "com", "edu", "gov", "net", "org"]);
const ISSUER_CONTROLLED_FORMS = /^(?:10-[KQ]|8-K|20-F|40-F|6-K|DEF\s*14A|S-[1348]|F-[134]|424B\d?|POS\s+AM|N-\w+)$/i;
function cleanDomain(value) {
  const host = String(value || "").toLowerCase().replace(/^www\./, "").replace(/\.$/, "");
  const labels = host.split(".");
  const barePublicSuffix = labels.length === 2 && labels[1].length === 2
    && COMMON_PUBLIC_SUFFIX_PREFIXES.has(labels[0]);
  return /^(?:[a-z0-9-]+\.)+[a-z]{2,}$/.test(host) && !barePublicSuffix
    && !NON_ISSUER_DOMAINS.has(host) && !NON_ISSUER_DOMAINS.has("www." + host) ? host : null;
}
function inferOfficialDomains(profile, documents, explicit = []) {
  const scores = new Map(), refs = new Map();
  const companyTokens = String(profile.companyName || "").toLowerCase().split(/[^a-z0-9]+/)
    .filter((x) => x.length >= 3
      && !/^(the|and|are|holdings?|group|company|corporation|inc|plc|limited|global|international|american|software|technolog(?:y|ies)|systems?|industries?|energy|health|financial|services?|products?|resources?)$/.test(x));
  const ticker = String(profile.symbol || "").toLowerCase();
  for (const raw of [...(profile.officialDomains || []), ...(profile.verifiedDomains || []), ...(explicit || [])]) {
    const domain = cleanDomain(raw); if (domain) scores.set(domain, 100);
  }
  for (const doc of documents || []) {
    if (!String(doc.sourceId || "").startsWith("sec.")) continue;
    if (!ISSUER_CONTROLLED_FORMS.test(String(doc.form || ""))) continue;
    const text = String(doc.canonicalText || doc.summary || "");
    const seenInDoc = new Set();
    for (const match of text.matchAll(/https?:\\?\/\\?\/([a-z0-9.-]+)(?:[/:?#"'\\s<]|$)/gi)) {
      const domain = cleanDomain(match[1]);
      if (!domain) continue;
      const labels = domain.split(".").slice(0, -1).flatMap((x) => x.split("-")).filter(Boolean);
      const nameMatch = companyTokens.some((token) => labels.includes(token));
      const tickerMatch = ticker.length >= 2 && labels.includes(ticker);
      if (!nameMatch && !tickerMatch) continue;
      const documentRef = String(doc.documentId || doc.versionId || "");
      if (!documentRef) continue;
      if (!refs.has(domain)) refs.set(domain, new Set());
      const domainRefs = refs.get(domain);
      if (!seenInDoc.has(domain) && !domainRefs.has(documentRef))
        scores.set(domain, (scores.get(domain) || 0) + 1);
      seenInDoc.add(domain);
      domainRefs.add(documentRef);
    }
  }
  return [...scores.entries()].filter(([, score]) => score >= 2 || score >= 100)
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0])).slice(0, 6)
    .map(([domain, score]) => ({ domain, confidence: score >= 100 ? 1 : Math.min(0.94, 0.72 + score * 0.05),
      basis: score >= 100 ? "operator_verified" : "issuer_name_domain_linked_from_multiple_sec_filings",
      documentRefs: [...(refs.get(domain) || [])].slice(0, 6) }));
}
function enrichProfile(profile, documents, explicitDomains = []) {
  const domains = inferOfficialDomains(profile, documents, explicitDomains);
  return { ...profile, officialDomains: domains.map((x) => x.domain), domainResolution: domains,
    curationStatus: profile.curationStatus === "identifier_incomplete" ? profile.curationStatus
      : (domains.length ? "sec_resolved_with_domain" : "sec_resolved_domain_pending") };
}

function aliasRegex(profile) {
  const terms = (profile.aliases || []).filter((x) => x.length >= 4 || /\s/.test(x));
  if (!terms.length) return null;
  return new RegExp(terms.map((x) => x.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"))
    .map((x) => `(?:^|\\b)${x}(?:\\b|$)`).join("|"), "i");
}
function matchesProfile(item, profile, source) {
  if (source.source_class === "company_primary") return true;
  const re = aliasRegex(profile);
  return !!(re && re.test(`${item.title || ""} ${item.summary || ""}`));
}

function sourceMeta(source, item = {}) {
  let domain = item.publisherDomain || null;
  if (!domain && item.link) { try { domain = new URL(item.link).hostname.toLowerCase(); } catch {} }
  return {
    ...source,
    publisher_domain: domain,
    independence_group: item.publisherGroup || source.independence_group || domain || source.source_id,
  };
}

async function readState(sourceId, symbol) {
  const id = `${sourceId}_${symbol}`;
  const snap = await A.col(A.COL.sourceState).doc(id).get();
  return { id, ref: A.col(A.COL.sourceState).doc(id), data: snap.exists ? snap.data() : {} };
}
async function markSuccess(state, source, extra = {}) {
  await state.ref.set({ sourceId: source.source_id, symbol: extra.symbol || null,
    lastSuccessAt: A.FV.serverTimestamp(), lastSuccessAtMs: Date.now(), lastAttemptAt: A.FV.serverTimestamp(),
    consecutiveFailures: 0, lastError: null, ...extra }, { merge: true });
}
async function markFailure(state, source, symbol, error) {
  await state.ref.set({ sourceId: source.source_id, symbol,
    lastAttemptAt: A.FV.serverTimestamp(), consecutiveFailures: Number(state.data.consecutiveFailures || 0) + 1,
    lastError: String(error && (error.code || error.message) || "source_failed").slice(0, 200) }, { merge: true });
}

async function recordItems(profile, source, items, rawSha256) {
  let recorded = 0;
  for (const item of items.slice(0, 100)) {
    if (!matchesProfile(item, profile, source)) continue;
    const link = item.link || source.url;
    const accession = cleanText(item.accession || normalizedHash(link || `${item.title}|${item.updated}`).hash.slice(0, 32), 500);
    const meta = sourceMeta(source, item);
    await E.recordVersion({ symbol: profile.symbol, sourceId: source.source_id,
      entry: { ...item, link, accession, sourceMeta: meta,
        publisherDomain: meta.publisher_domain, publisherGroup: meta.independence_group,
        discoveryOnly: source.discovery_only === true },
      rawSha256: rawSha256 || normalizedHash(`${item.title}|${item.summary}|${item.updated}`).hash,
      body: source.full_text_allowed ? String(item.body || item.summary || "").slice(0, 120000) : "" });
    recorded += 1;
  }
  return recorded;
}

async function pollFeed(profile, source, state) {
  const res = await fetchPublic(source.url, { sourceId: source.source_id,
    etag: state.data.etag, lastModified: state.data.lastModified,
    accept: ["xml", "rss", "atom", "text"], timeoutMs: 18000, maxBytes: 2 * 1024 * 1024 });
  if (res.notModified) {
    await markSuccess(state, source, { symbol: profile.symbol, notModified: true, matched: 0 });
    return { sourceId: source.source_id, healthy: true, notModified: true, matched: 0 };
  }
  const parsed = parseFeed(res.text);
  const matched = await recordItems(profile, source, parsed, res.sha256);
  await markSuccess(state, source, { symbol: profile.symbol, etag: res.etag || null,
    lastModified: res.lastModified || null, parsed: parsed.length, matched, responseSha256: res.sha256 });
  return { sourceId: source.source_id, healthy: true, parsed: parsed.length, matched };
}

async function pollHtmlIndex(profile, source, state) {
  const res = await fetchPublic(source.url, { sourceId: source.source_id,
    etag: state.data.etag, lastModified: state.data.lastModified,
    accept: ["html", "text"], timeoutMs: 18000, maxBytes: 2 * 1024 * 1024 });
  if (res.notModified) {
    await markSuccess(state, source, { symbol: profile.symbol, notModified: true, matched: 0 });
    return { sourceId: source.source_id, healthy: true, notModified: true, matched: 0 };
  }
  const plain = stripTags(res.text).slice(0, 120000);
  /* Never seed the match text with the company name: doing so makes every
     sector index look relevant even when the issuer is absent from the page. */
  const relevant = matchesProfile({ title: source.name, summary: plain }, profile, source);
  const matched = relevant ? await recordItems(profile, source, [{
    title: `${source.name} snapshot for ${profile.companyName}`,
    summary: plain.slice(0, source.full_text_allowed ? 120000 : 6000),
    body: source.full_text_allowed ? plain : "", link: source.url, updated: null,
    accession: normalizedHash(`${source.url}|${res.sha256}`).hash.slice(0, 32),
  }], res.sha256) : 0;
  await markSuccess(state, source, { symbol: profile.symbol, etag: res.etag || null,
    lastModified: res.lastModified || null, matched, responseSha256: res.sha256 });
  return { sourceId: source.source_id, healthy: true, matched };
}

function sameResolvedDomain(host, domains) {
  const h = String(host || "").toLowerCase().replace(/^www\./, "");
  return (domains || []).some((d) => {
    const root = String(d || "").toLowerCase().replace(/^www\./, "");
    return h === root || h.endsWith("." + root);
  });
}
function publicSiteLinks(html, baseUrl, domains) {
  const out = [];
  for (const m of String(html || "").matchAll(/<a\b[^>]*href=["']([^"']+)["'][^>]*>([\s\S]{0,240}?)<\/a>|<link\b[^>]*href=["']([^"']+)["'][^>]*>/gi)) {
    const href = decodeXml(m[1] || m[3] || ""), label = stripTags(m[2] || m[0]);
    if (!/(news|press|media|investor|release|rss|atom|announcement|filing)/i.test(`${href} ${label}`)) continue;
    let u; try { u = new URL(href, baseUrl); } catch { continue; }
    if (u.protocol !== "https:" || !sameResolvedDomain(u.hostname, domains)) continue;
    u.hash = "";
    out.push(u.toString());
  }
  return uniq(out).slice(0, 4);
}
async function pollCompanyDirect(profile, source, state) {
  const domains = uniq(profile.officialDomains || []).map(cleanDomain).filter(Boolean).slice(0, 6);
  if (!domains.length) throw new Error("official company domain not yet resolved from SEC-linked or operator-verified evidence");
  const allowedHosts = uniq(domains.flatMap((d) => [d, `www.${d}`]));
  let matched = 0, fetched = 0, lastError = null;
  for (const domain of domains.slice(0, 3)) {
    const root = `https://${domain}/`;
    let res;
    try {
      res = await fetchPublic(root, { sourceId: source.source_id, allowedHosts,
        accept: ["html", "text", "xml"], timeoutMs: 16000, maxBytes: 2 * 1024 * 1024 });
      fetched += 1;
    } catch (error) { lastError = error; continue; }
    matched += await recordItems(profile, source, [{
      title: `${profile.companyName} official public-site snapshot`,
      summary: stripTags(res.text).slice(0, 12000), body: stripTags(res.text).slice(0, 120000),
      link: res.finalUrl || root, updated: null,
      accession: normalizedHash(`${res.finalUrl || root}|${res.sha256}`).hash.slice(0, 32),
      publisherDomain: domain, publisherGroup: `company:${profile.symbol}`,
    }], res.sha256);
    const links = publicSiteLinks(res.text, res.finalUrl || root, domains);
    for (const link of links.slice(0, 2)) {
      try {
        const lr = await fetchPublic(link, { sourceId: source.source_id,
          allowedHosts: uniq([...allowedHosts, new URL(link).hostname]),
          accept: ["html", "text", "xml", "rss", "atom"], timeoutMs: 16000,
          maxBytes: 2 * 1024 * 1024 });
        fetched += 1;
        const items = /<(rss|feed)\b/i.test(lr.text) ? parseFeed(lr.text) : [{
          title: `${profile.companyName} public news/investor page`,
          summary: stripTags(lr.text).slice(0, 12000), body: stripTags(lr.text).slice(0, 120000),
          link: lr.finalUrl || link, updated: null,
          accession: normalizedHash(`${lr.finalUrl || link}|${lr.sha256}`).hash.slice(0, 32),
          publisherDomain: new URL(lr.finalUrl || link).hostname,
          publisherGroup: `company:${profile.symbol}`,
        }];
        matched += await recordItems(profile, source, items.map((x) => ({ ...x,
          publisherGroup: `company:${profile.symbol}` })), lr.sha256);
      } catch (error) { lastError = error; }
    }
  }
  if (!fetched) throw lastError || new Error("resolved company domains were unreachable");
  await markSuccess(state, source, { symbol: profile.symbol, matched, fetched,
    resolvedDomains: domains, domainResolution: profile.domainResolution || [] });
  return { sourceId: source.source_id, healthy: true, matched, fetched, resolvedDomains: domains };
}

function gdeltQuery(profile, source) {
  const terms = (profile.aliases || []).filter((x) => x.length >= 4 || /\s/.test(x)).slice(0, 6);
  if (!terms.length) return null;
  const company = terms.map((x) => `\"${x.replace(/[\"\\]/g, " ")}\"`).join(" OR ");
  if (source && source.kind === "gdelt_sector") {
    const domains = [...(profile.publicationDomains || []), ...(profile.eventDomains || [])]
      .slice(0, 14).map((x) => `domain:${x}`);
    const topics = (profile.topics || []).slice(0, 8).map((x) => `\"${x.replace(/[\"\\]/g, " ")}\"`);
    const context = [...domains, ...topics].join(" OR ");
    return context ? `(${company}) (${context})` : `(${company})`;
  }
  const relationships = (profile.relatedEntities || []).filter((x) => Number(x.confidence || 0) >= 0.7)
    .slice(0, 6).map((x) => `\"${x.name.replace(/[\"\\]/g, " ")}\"`);
  if (source && source.kind === "gdelt_relationships") {
    return relationships.length ? `(${company}) (${relationships.join(" OR ")})` : company;
  }
  return company;
}
async function pollGdelt(profile, source, state) {
  const query = gdeltQuery(profile, source);
  if (!query) throw new Error("company aliases are insufficient for broad discovery");
  const url = "https://api.gdeltproject.org/api/v2/doc/doc?" + new URLSearchParams({
    query: `(${query})`, mode: "artlist", format: "json", maxrecords: "75",
    timespan: "3d", sort: "datedesc",
  }).toString();
  const res = await fetchPublic(url, { sourceId: source.source_id,
    accept: ["json", "text"], timeoutMs: 20000, maxBytes: 3 * 1024 * 1024 });
  const articles = (res.json && (res.json.articles || res.json.items)) || [];
  const items = articles.map((x) => ({ title: cleanText(x.title, 500),
    summary: cleanText(x.title, 500), link: x.url, updated: x.seendate || x.date || null,
    accession: normalizedHash(x.url || `${x.title}|${x.seendate}`).hash.slice(0, 32),
    publisherDomain: cleanText(x.domain, 200).toLowerCase() || null,
    publisherGroup: cleanText(x.domain, 200).toLowerCase() || null,
    discoveryOnly: true,
  })).filter((x) => x.title && x.link);
  const matched = await recordItems(profile, source, items, res.sha256);
  await markSuccess(state, source, { symbol: profile.symbol, parsed: items.length,
    matched, responseSha256: res.sha256, discoveryWindowDays: 3 });
  return { sourceId: source.source_id, healthy: true, parsed: items.length, matched };
}

function isoDay(ms) { return new Date(ms).toISOString().slice(0, 10); }
async function pollFederalRegister(profile, source, state) {
  const term = (profile.aliases || [])[0];
  if (!term) throw new Error("company name required for Federal Register search");
  const params = new URLSearchParams({ per_page: "100", order: "newest",
    "conditions[term]": term,
    "conditions[publication_date][gte]": isoDay(Date.now() - 180 * DAY_MS) });
  const url = `https://www.federalregister.gov/api/v1/documents.json?${params}`;
  const res = await fetchPublic(url, { sourceId: source.source_id,
    accept: ["json"], timeoutMs: 18000, maxBytes: 3 * 1024 * 1024 });
  const rows = (res.json && res.json.results) || [];
  const items = rows.map((x) => ({ title: cleanText(x.title, 500),
    summary: cleanText(x.abstract || x.excerpts || "", 12000),
    body: cleanText(x.abstract || x.excerpts || "", 12000),
    link: x.html_url || x.pdf_url || x.raw_text_url,
    updated: x.publication_date || x.filing_type || null,
    accession: x.document_number || null,
    publisherDomain: "federalregister.gov", publisherGroup: "us_federal_register",
  })).filter((x) => x.title && x.link);
  const matched = await recordItems(profile, source, items, res.sha256);
  await markSuccess(state, source, { symbol: profile.symbol, parsed: items.length,
    matched, responseSha256: res.sha256, lookbackDays: 180 });
  return { sourceId: source.source_id, healthy: true, parsed: items.length, matched };
}

async function pollUsaspending(profile, source, state) {
  const names = (profile.recipientNames || []).slice(0, 4);
  if (!names.length) throw new Error("recipient identity unavailable for USAspending search");
  const start = isoDay(Date.now() - 180 * DAY_MS), end = isoDay(Date.now());
  const body = {
    filters: { time_period: [{ start_date: start, end_date: end }],
      award_type_codes: ["A", "B", "C", "D"], recipient_search_text: names },
    fields: ["Award ID", "Recipient Name", "Start Date", "End Date", "Award Amount",
      "Awarding Agency", "Award Description", "generated_subaward_id"],
    page: 1, limit: 100, subawards: false,
  };
  const res = await fetchPublic("https://api.usaspending.gov/api/v2/search/spending_by_award/",
    { sourceId: source.source_id, method: "POST", body, accept: ["json"],
      timeoutMs: 22000, maxBytes: 4 * 1024 * 1024, maxRedirects: 0 });
  const rows = (res.json && res.json.results) || [];
  const items = rows.map((x) => {
    const awardId = x["Award ID"] || x.Award_ID || x.generated_unique_award_id;
    const amount = Number(x["Award Amount"] ?? x.Award_Amount);
    const agency = x["Awarding Agency"] || x.Awarding_Agency || "U.S. government";
    const description = cleanText(x["Award Description"] || x.Award_Description || "", 10000);
    return { title: cleanText(`${agency} award ${awardId || "update"} — ${profile.companyName}`, 500),
      summary: cleanText(`${description}${Number.isFinite(amount) ? ` | obligated/award amount: $${amount}` : ""}`, 12000),
      body: description, link: awardId ? `https://www.usaspending.gov/award/${encodeURIComponent(awardId)}/` : "https://www.usaspending.gov/",
      updated: x["Start Date"] || x.Start_Date || null, accession: awardId || null,
      publisherDomain: "usaspending.gov", publisherGroup: "usaspending" };
  }).filter((x) => x.accession);
  const matched = await recordItems(profile, source, items, res.sha256);
  await markSuccess(state, source, { symbol: profile.symbol, parsed: items.length,
    matched, responseSha256: res.sha256, lookbackDays: 180 });
  return { sourceId: source.source_id, healthy: true, parsed: items.length, matched };
}

function clinicalTrialItems(payload = {}) {
  return (Array.isArray(payload.studies) ? payload.studies : []).map((study) => {
    const protocol = study && study.protocolSection || {};
    const identity = protocol.identificationModule || {};
    const status = protocol.statusModule || {};
    const sponsors = protocol.sponsorCollaboratorsModule || {};
    const design = protocol.designModule || {};
    const conditions = protocol.conditionsModule || {};
    const description = protocol.descriptionModule || {};
    const nctId = cleanText(identity.nctId, 32);
    const leadSponsor = cleanText(sponsors.leadSponsor && sponsors.leadSponsor.name, 300);
    const phases = Array.isArray(design.phases) ? design.phases.map((x) => cleanText(x, 60)).filter(Boolean) : [];
    const conditionNames = Array.isArray(conditions.conditions)
      ? conditions.conditions.map((x) => cleanText(x, 120)).filter(Boolean).slice(0, 8) : [];
    const updated = status.lastUpdatePostDateStruct && status.lastUpdatePostDateStruct.date
      || status.studyFirstPostDateStruct && status.studyFirstPostDateStruct.date
      || status.statusVerifiedDate || null;
    const facts = [
      leadSponsor && `Lead sponsor: ${leadSponsor}`,
      status.overallStatus && `Status: ${cleanText(status.overallStatus, 80)}`,
      phases.length && `Phase: ${phases.join(", ")}`,
      conditionNames.length && `Conditions: ${conditionNames.join(", ")}`,
      description.briefSummary && cleanText(description.briefSummary, 8000),
    ].filter(Boolean);
    return {
      title: cleanText(identity.briefTitle || identity.officialTitle || `${nctId} clinical study`, 500),
      summary: cleanText(facts.join(" | "), 12000),
      body: cleanText(facts.join("\n"), 12000),
      link: nctId ? `https://clinicaltrials.gov/study/${encodeURIComponent(nctId)}` : null,
      updated, accession: nctId || null,
      publisherDomain: "clinicaltrials.gov", publisherGroup: "clinicaltrials",
    };
  }).filter((x) => x.accession && x.title && x.link);
}

async function pollClinicalTrials(profile, source, state) {
  const sponsor = (profile.recipientNames || profile.aliases || [])
    .map((x) => cleanText(x, 180)).find((x) => x.length >= 4);
  if (!sponsor) throw new Error("resolved sponsor identity required for ClinicalTrials.gov search");
  const params = new URLSearchParams({ "query.spons": sponsor, pageSize: "100",
    sort: "LastUpdatePostDate:desc", format: "json" });
  const url = `https://clinicaltrials.gov/api/v2/studies?${params}`;
  const res = await fetchPublic(url, { sourceId: source.source_id,
    accept: ["json"], timeoutMs: 22000, maxBytes: 8 * 1024 * 1024 });
  const items = clinicalTrialItems(res.json || {});
  const matched = await recordItems(profile, source, items, res.sha256);
  await markSuccess(state, source, { symbol: profile.symbol, parsed: items.length,
    matched, responseSha256: res.sha256, sponsorQuery: sponsor });
  return { sourceId: source.source_id, healthy: true, parsed: items.length, matched };
}

async function pollNwsAlerts(profile, source, state) {
  const states = uniq([...(profile.operatingStates || []),
    ...((profile.temporalExposures || []).flatMap((x) => x && x.states || []))])
    .map((x) => String(x).trim().toUpperCase()).filter((x) => /^[A-Z]{2}$/.test(x));
  if (!states.length) {
    await markSuccess(state, source, { symbol: profile.symbol, matched: 0, notApplicable: true });
    return { sourceId: source.source_id, healthy: true, matched: 0, notApplicable: true,
      temporalHazards: [], statesQueried: [] };
  }
  const queryStates = states.slice(0, 8), hazards = [], responses = [];
  const queryArea = async (area) => {
    const url = `https://api.weather.gov/alerts/active?${new URLSearchParams({ area })}`;
    const res = await fetchPublic(url, { sourceId: source.source_id, accept: ["json"],
      timeoutMs: 16000, maxBytes: 4 * 1024 * 1024 });
    responses.push({ state: area, responseSha256: res.sha256 || null, fetchedAt: res.fetchedAt || null });
    for (const feature of res.json && res.json.features || []) {
      const p = feature && feature.properties || {};
      hazards.push({ id: feature.id || p.id || null, event: cleanText(p.event, 160),
        headline: cleanText(p.headline || p.description, 500), states: [area],
        severity: cleanText(p.severity, 40) || "Unknown",
        certainty: cleanText(p.certainty, 40) || "Unknown",
        urgency: cleanText(p.urgency, 40) || "Unknown",
        effective: p.effective || p.onset || null, expires: p.expires || p.ends || null,
        areaDesc: cleanText(p.areaDesc, 500),
        responseSha256: res.sha256 || null, fetchedAt: res.fetchedAt || null });
    }
  };
  for (let i = 0; i < queryStates.length; i += 4) {
    await Promise.all(queryStates.slice(i, i + 4).map(queryArea));
  }
  const unique = [...new Map(hazards.map((x) => [`${x.id}|${x.states[0]}`, x])).values()];
  const coverageComplete = queryStates.length === states.length;
  await markSuccess(state, source, { symbol: profile.symbol, matched: unique.length,
    statesQueried: queryStates, coverageComplete, responses });
  return { sourceId: source.source_id, healthy: true, matched: unique.length,
    temporalHazards: unique.slice(0, 100), statesQueried: queryStates,
    truncatedStates: states.slice(queryStates.length), coverageComplete,
    responses: responses.sort((a, b) => a.state.localeCompare(b.state)) };
}

async function pollSource(profile, sourceId) {
  const source = SOURCE_REGISTRY[sourceId];
  if (!source) return { sourceId, healthy: false, error: "unregistered_source" };
  const state = await readState(sourceId, profile.symbol);
  try {
    if (source.kind === "feed") return await pollFeed(profile, source, state);
    if (source.kind === "html_index") return await pollHtmlIndex(profile, source, state);
    if (source.kind === "company_direct") return await pollCompanyDirect(profile, source, state);
    if (source.kind === "gdelt") return await pollGdelt(profile, source, state);
    if (source.kind === "gdelt_sector") return await pollGdelt(profile, source, state);
    if (source.kind === "gdelt_relationships") return await pollGdelt(profile, source, state);
    if (source.kind === "federal_register") return await pollFederalRegister(profile, source, state);
    if (source.kind === "usaspending") return await pollUsaspending(profile, source, state);
    if (source.kind === "clinicaltrials") return await pollClinicalTrials(profile, source, state);
    if (source.kind === "nws_alerts") return await pollNwsAlerts(profile, source, state);
    throw new Error(`unsupported source kind ${source.kind}`);
  } catch (error) {
    await markFailure(state, source, profile.symbol, error);
    return { sourceId, healthy: false, error: String(error.code || error.message).slice(0, 160) };
  }
}

async function pollCompany(profile, { budgetMs = 150000 } = {}) {
  const started = Date.now(), results = [];
  for (const sourceId of profile.sourceIds || []) {
    if (Date.now() - started >= budgetMs) {
      results.push({ sourceId, healthy: false, deferred: true, error: "company_source_budget_exhausted" });
      continue;
    }
    results.push(await pollSource(profile, sourceId));
  }
  return { symbol: profile.symbol, results, elapsedMs: Date.now() - started };
}

function configuredSymbols(ctrl = {}) {
  const fromEnv = String(process.env.INVESTOR_INTELLIGENCE_SYMBOLS || "")
    .split(/[\s,]+/).filter(Boolean);
  const raw = Array.isArray(ctrl.intelligenceSymbols) && ctrl.intelligenceSymbols.length
    ? ctrl.intelligenceSymbols : fromEnv;
  return uniq(raw.map(cleanSymbol)).slice(0, MAX_FOCUS);
}

function focusSymbols({ ctrl = {}, positions = [], researchTier = [], candidates = [], max = MAX_FOCUS } = {}) {
  const rows = [], seen = new Set();
  const add = (value, reason) => {
    const symbol = cleanSymbol(typeof value === "string" ? value : value && value.symbol);
    if (!symbol || seen.has(symbol) || rows.length >= Math.max(1, Math.min(MAX_FOCUS, max))) return;
    seen.add(symbol); rows.push({ symbol, reason });
  };
  /* Held risk outranks research preference. A full 24-name watchlist must not
     push an open position out of the monitored intelligence set. */
  positions.filter((x) => x && x.open).forEach((x) => add(x, "open_position"));
  configuredSymbols(ctrl).forEach((x) => add(x, "operator_watchlist"));
  researchTier.forEach((x) => add(x, "research_roster"));
  [...candidates].sort((a, b) => Number(a.rank ?? 1) - Number(b.rank ?? 1))
    .forEach((x) => add(x, "signal_frontier"));
  return rows;
}

module.exports = {
  DAY_MS, MAX_FOCUS, SOURCE_REGISTRY, SECTOR_PACKS, SIC_PACKS, CORE_SOURCE_IDS,
  decodeXml, stripTags, parseFeed, profileFor, resolveIdentity, inferOfficialDomains,
  enrichProfile, cleanDomain, sameResolvedDomain, matchesProfile, sourceMeta, clinicalTrialItems,
  pollSource, pollCompany, configuredSymbols, focusSymbols, cleanSymbol,
};
