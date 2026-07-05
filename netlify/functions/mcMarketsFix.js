// netlify/functions/mcMarketsFix.js
// ---------------------------------------------------------------------------
// ONE-SHOT diagnostic + fix for the July 2026 Merchant Center outage:
//
//   ROOT CAUSE: the Google & YouTube channel's "Automatically sync countries
//   and languages" is ON and picked up THREE Shopify markets (US, CA, UK).
//   The app submits a SEPARATE product copy per market (shopify_US_*,
//   shopify_CA_*, shopify_GB_*): ~64.6K variants x 3 = ~194K offers.
//     - The UK copies have NO shipping service in Merchant Center (Shopify
//       has no UK rates to sync)  -> "Missing shipping info" on 100%.
//     - The third catalog copy blows the 150,000 Shopping-ads quota
//       -> both "over capacity" cards (Approved pinned at ~150,483).
//
//   FIX: stop targeting the UK on Google. Disabling the UK market in Shopify
//   makes the app's auto-sync drop the GB product copies from Merchant
//   Center: ~194K -> ~129K offers (under quota, full US+CA shipping
//   coverage). All three red cards resolve within 24-72h of reprocessing.
//
//   SAFETY: this tool REFUSES to disable a market that has working shipping
//   rates (that would mean real checkout traffic could exist there). The
//   status action shows exactly what it found; the fix runs only with
//   explicit confirm=1, and only flips `enabled:false` (fully reversible in
//   Settings -> Markets, nothing is deleted).
//
// HTTP (same zero-auth idempotent-diagnostic pattern as googleAttributes):
//   GET ...?action=status
//        -> markets + which countries have deliverable shipping rates +
//           verdict/recommendation. READ-ONLY.
//   GET ...?action=fix&confirm=1
//        -> disables the UK market (only if status says it's safe).
//   GET ...?action=fix&confirm=1&force=1
//        -> disables it even if UK rates exist (NOT recommended).
//   GET ...?action=undo
//        -> re-enables the UK market (if the fix was applied).
//
// Env: SHOPIFY_STORE, SHOPIFY_CLIENT_ID, SHOPIFY_CLIENT_SECRET,
//      SHOPIFY_API_VERSION? (default "2025-10")
// Scopes: needs read_markets/write_markets (and read_shipping). If the token
// lacks them the response says so explicitly - fallback is the 30-second UI
// path: Shopify Settings -> Markets -> United Kingdom -> Disable.
// ---------------------------------------------------------------------------
"use strict";

const API_VERSION = process.env.SHOPIFY_API_VERSION || "2025-10";

let _token = null, _tokenExp = 0;
async function getToken() {
  if (_token && Date.now() < _tokenExp - 60000) return _token;
  const store = process.env.SHOPIFY_STORE;
  const res = await fetch(`https://${store}/admin/oauth/access_token`, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type: "client_credentials",
      client_id: process.env.SHOPIFY_CLIENT_ID,
      client_secret: process.env.SHOPIFY_CLIENT_SECRET
    })
  });
  const text = await res.text();
  if (!res.ok) throw new Error("Token request failed (" + res.status + "): " + text);
  const data = JSON.parse(text);
  _token = data.access_token;
  _tokenExp = Date.now() + (Number(data.expires_in || 3600) * 1000);
  return _token;
}

async function gql(query, variables, _attempt) {
  const store = process.env.SHOPIFY_STORE;
  const token = await getToken();
  const res = await fetch(`https://${store}/admin/api/${API_VERSION}/graphql.json`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "X-Shopify-Access-Token": token },
    body: JSON.stringify({ query, variables: variables || {} })
  });
  const body = await res.json().catch(() => ({}));
  if (res.status === 429 || (body.errors || []).some(e => (e.extensions || {}).code === "THROTTLED")) {
    if ((_attempt || 0) < 4) { await new Promise(r => setTimeout(r, 1500 * ((_attempt || 0) + 1))); return gql(query, variables, (_attempt || 0) + 1); }
  }
  if (body.errors && body.errors.length) throw new Error("GraphQL: " + JSON.stringify(body.errors).slice(0, 500));
  return body.data;
}

/* ------------------------------ reads ------------------------------ */

// Markets with their region country codes.
async function readMarkets() {
  const d = await gql(`
    query {
      markets(first: 20) {
        nodes {
          id name handle enabled primary
          regions(first: 60) { nodes { ... on MarketRegionCountry { code name } } }
        }
      }
    }`);
  return (d.markets.nodes || []).map(m => ({
    id: m.id, name: m.name, handle: m.handle, enabled: !!m.enabled, primary: !!m.primary,
    countries: (m.regions && m.regions.nodes ? m.regions.nodes : []).map(r => r.code).filter(Boolean)
  }));
}

// Delivery profiles -> which country codes actually have shipping RATES.
// A country that appears in a zone with at least one method definition is
// "deliverable"; a market country with no rates anywhere is what produced the
// missing-shipping wall in Merchant Center.
async function readDeliverableCountries() {
  const d = await gql(`
    query {
      deliveryProfiles(first: 10) {
        nodes {
          name default
          profileLocationGroups {
            locationGroupZones(first: 30) {
              nodes {
                zone { name countries { code { countryCode restOfWorld } } }
                methodDefinitions(first: 1) { nodes { id active } }
              }
            }
          }
        }
      }
    }`);
  const deliverable = new Set(); let restOfWorld = false;
  const zones = [];
  (d.deliveryProfiles.nodes || []).forEach(p => {
    (p.profileLocationGroups || []).forEach(g => {
      ((g.locationGroupZones && g.locationGroupZones.nodes) || []).forEach(z => {
        const hasRates = !!(z.methodDefinitions && z.methodDefinitions.nodes && z.methodDefinitions.nodes.length);
        const codes = ((z.zone && z.zone.countries) || []).map(c => (c.code || {}).countryCode).filter(Boolean);
        const row = (z.zone && z.zone.name) || "?";
        zones.push({ profile: p.name, zone: row, hasRates, countries: codes.length ? codes : (((z.zone||{}).countries||[]).some(c=>(c.code||{}).restOfWorld) ? ["Rest of world"] : []) });
        if (!hasRates) return;
        codes.forEach(c => deliverable.add(c));
        if (((z.zone || {}).countries || []).some(c => (c.code || {}).restOfWorld)) restOfWorld = true;
      });
    });
  });
  return { deliverable: [...deliverable], restOfWorld, zones };
}

/* ---------------------------- assemble ----------------------------- */

async function status() {
  const [markets, ship] = await Promise.all([readMarkets(), readDeliverableCountries()]);
  const enabled = markets.filter(m => m.enabled);
  // For every enabled market country: does it have rates?
  const marketReport = enabled.map(m => {
    const missing = m.countries.filter(c => !ship.restOfWorld && !ship.deliverable.includes(c));
    return { name: m.name, handle: m.handle, primary: m.primary, countries: m.countries, countriesWithoutShippingRates: missing };
  });
  const ukMarket = markets.find(m => m.enabled && m.countries.length === 1 && m.countries[0] === "GB")
                || markets.find(m => m.enabled && m.countries.includes("GB") && !m.primary);
  const ukHasRates = ship.restOfWorld || ship.deliverable.includes("GB");
  const enabledCountryCount = new Set(enabled.flatMap(m => m.countries)).size;
  const verdict = {
    enabledMarkets: enabled.map(m => m.name + " [" + m.countries.join(",") + "]"),
    offersMultiplier: enabledCountryCount + " catalog copies are being pushed to Merchant Center (one per synced country)",
    ukMarketFound: !!ukMarket, ukHasShippingRates: ukHasRates,
    recommendation: !ukMarket
      ? "No separate enabled UK market found - if the app still targets GB, turn OFF 'Automatically sync countries' in the Google & YouTube app and keep US+CA."
      : ukHasRates
        ? "UK market HAS shipping rates - disabling it would affect real UK checkout. Instead add a UK shipping service in Merchant Center (or keep UK and request a quota increase after the account is healthy). fix will refuse without force=1."
        : "UK market has NO shipping rates anywhere in your delivery profiles - UK checkout can't complete anyway. Safe to disable: run ?action=fix&confirm=1. Offers drop ~194K -> ~129K (under the 150K quota) and the missing-shipping card clears."
  };
  return { ok: true, markets: marketReport, shippingZones: ship.zones, deliverableCountries: ship.deliverable, restOfWorldRates: ship.restOfWorld, verdict, ukMarketId: ukMarket ? ukMarket.id : null };
}

async function setMarketEnabled(id, enabled) {
  const d = await gql(`
    mutation($input: MarketUpdateInput!, $id: ID!) {
      marketUpdate(id: $id, input: $input) {
        market { id name enabled }
        userErrors { field message }
      }
    }`, { id, input: { enabled } });
  const errs = (d.marketUpdate.userErrors || []);
  if (errs.length) throw new Error("marketUpdate: " + JSON.stringify(errs));
  return d.marketUpdate.market;
}

/* ----------------------------- handler ----------------------------- */

exports.handler = async (event) => {
  const q = (event && event.queryStringParameters) || {};
  const action = q.action || "status";
  const H = { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" };
  const out = (code, body) => ({ statusCode: code, headers: H, body: JSON.stringify(body, null, 2) });
  try {
    if (action === "status") return out(200, await status());

    if (action === "fix") {
      if (q.confirm !== "1") return out(400, { ok: false, error: "add &confirm=1 to apply (run ?action=status first and read the verdict)" });
      const s = await status();
      if (!s.ukMarketId) return out(400, { ok: false, error: "no enabled UK market found - nothing to disable", verdict: s.verdict });
      if (s.verdict.ukHasShippingRates && q.force !== "1")
        return out(409, { ok: false, error: "REFUSING: the UK market has working shipping rates, so disabling it could block real UK checkouts. Read verdict.recommendation; override only with &force=1.", verdict: s.verdict });
      const m = await setMarketEnabled(s.ukMarketId, false);
      return out(200, { ok: true, disabled: m, next: "The Google & YouTube app's auto-sync will drop the GB catalog copies from Merchant Center. Expect the quota and missing-shipping cards to clear within 24-72h as Google reprocesses. Re-enable any time with ?action=undo (or Settings -> Markets)." });
    }

    if (action === "undo") {
      const markets = await readMarkets();
      const uk = markets.find(m => !m.enabled && m.countries.includes("GB"));
      if (!uk) return out(400, { ok: false, error: "no disabled UK market found" });
      const m = await setMarketEnabled(uk.id, true);
      return out(200, { ok: true, reEnabled: m });
    }

    return out(400, { ok: false, error: "unknown action (status | fix | undo)" });
  } catch (e) {
    const msg = (e && e.message) || String(e);
    const scopeHint = /ACCESS_DENIED|access denied|read_markets|write_markets|not approved|403/i.test(msg)
      ? "Token likely lacks read_markets/write_markets scope. Fallback (30 seconds, same effect): Shopify admin -> Settings -> Markets -> United Kingdom -> ... -> Disable."
      : undefined;
    return out(500, { ok: false, error: msg, scopeHint });
  }
};
