// netlify/functions/applySiteFixes.js
// ---------------------------------------------------------------------------
// SELF-RUNNING, idempotent fixer for the Brites homepage/nav issues from the
// site audit. NO manual invocation needed: add the schedule below to
// netlify.toml, deploy, and it completes every fix automatically, then keeps
// itself in a "done" state (re-runs are no-ops).
//
//   [functions."applySiteFixes"]
//     schedule = "@hourly"
//
// What each automatic run does, in order:
//   1. renameHandles  — collection handles pendant -> best-sellers and
//                       breslate -> necklaces, each with a /collections/<old>
//                       URL redirect so existing links keep working.
//   2. createPages    — "Our Story — Handmade Locally" (/pages/local-manufacturing,
//                       full trust copy embedded) and the "Shop by Theme" hub
//                       (/pages/shop-by-theme, template suffix shop-by-theme).
//   3. fixMenus       — repairs every misdirected nav target (Shop by Type,
//                       Shop by Theme, the Jewelry heading, Sports & Athletics
//                       off /collections/cop, Best Sellers/Necklaces to the
//                       renamed handles) and appends About Us to the footer.
//                       Everything else in each menu is preserved exactly.
//   4. setCustomLabels— walks the catalog ~7 seconds per run (cursor saved in
//                       Firestore, resumes next run) writing the Google
//                       Shopping custom labels used by the PMax asset-group
//                       filters: custom_label_0 = theme bucket,
//                       custom_label_1 = best-seller.
// Progress and completion state live in Firestore doc
// Brites_Editor_Meta/siteFixesState; once everything reports done, scheduled
// runs exit immediately. To force a re-run, delete that doc.
//
// The HTTP interface (POST {action, dryRun} + X-Edit-Passcode) still exists
// for diagnostics, but is never required.
//
// Env: SHOPIFY_STORE, SHOPIFY_CLIENT_ID, SHOPIFY_CLIENT_SECRET, EDIT_PASSCODE,
//      SHOPIFY_API_VERSION?, SPORTS_COLLECTION_HANDLE (default "sports-athletics")
// ---------------------------------------------------------------------------

let _fb = null;
function fb() {
  if (_fb !== null) return _fb;
  try {
    const admin = require("./firebaseAdmin");
    _fb = { admin, db: admin.firestore(), FV: admin.firestore.FieldValue };
  } catch (e) { console.error("[applySiteFixes] Firebase unavailable:", e.message); _fb = false; }
  return _fb;
}

const fetch = require("node-fetch");
const API_VERSION = process.env.SHOPIFY_API_VERSION || "2025-10";
const SPORTS_HANDLE = process.env.SPORTS_COLLECTION_HANDLE || "sports-athletics";

/* ---- token + gql: identical pattern to shopifyEditor.js ---- */
let _token = null, _tokenExp = 0;
async function getToken() {
  if (_token && Date.now() < _tokenExp - 60000) return _token;
  const res = await fetch(`https://${process.env.SHOPIFY_STORE}/admin/oauth/access_token`, {
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
  _tokenExp = Date.now() + (data.expires_in || 86399) * 1000;
  return _token;
}
async function gql(query, variables, _attempt) {
  const token = await getToken();
  try {
    const res = await fetch(`https://${process.env.SHOPIFY_STORE}/admin/api/${API_VERSION}/graphql.json`, {
      method: "POST",
      headers: { "X-Shopify-Access-Token": token, "Content-Type": "application/json" },
      body: JSON.stringify({ query, variables: variables || {} })
    });
    if (res.status >= 500) throw new Error("GraphQL HTTP " + res.status);
    const data = await res.json();
    if (!res.ok) throw new Error("GraphQL HTTP " + res.status);
    if (data.errors && data.errors.length) throw new Error("GraphQL: " + JSON.stringify(data.errors));
    return data.data;
  } catch (e) {
    const msg = String((e && e.message) || e);
    const transient = /ECONNRESET|ETIMEDOUT|socket hang up|network|fetch failed|EAI_AGAIN|GraphQL HTTP 5\d\d/i.test(msg);
    const attempt = _attempt || 0;
    if (transient && attempt < 2) { await new Promise(r => setTimeout(r, 350 * (attempt + 1))); return gql(query, variables, attempt + 1); }
    throw e;
  }
}

/* ================= renameHandles ================= */
const HANDLE_RENAMES = [
  { from: "pendant",  to: "best-sellers" },
  { from: "breslate", to: "necklaces"    }
];
async function renameHandles(dryRun) {
  const out = [];
  for (const r of HANDLE_RENAMES) {
    const d = await gql(`query($h: String!) { collectionByHandle(handle: $h) { id title handle } }`, { h: r.from });
    const already = await gql(`query($h: String!) { collectionByHandle(handle: $h) { id } }`, { h: r.to });
    if (!d.collectionByHandle) {
      out.push({ ...r, status: already.collectionByHandle ? "already renamed" : "source handle not found" });
      continue;
    }
    if (already.collectionByHandle) { out.push({ ...r, status: "target handle taken — skipped" }); continue; }
    if (!dryRun) {
      const u = await gql(`mutation($input: CollectionInput!) {
        collectionUpdate(input: $input) { userErrors { field message } } }`,
        { input: { id: d.collectionByHandle.id, handle: r.to } });
      const ue = u.collectionUpdate.userErrors;
      if (ue.length) { out.push({ ...r, status: "error: " + ue[0].message }); continue; }
      const rd = await gql(`mutation($redirect: UrlRedirectInput!) {
        urlRedirectCreate(urlRedirect: $redirect) { userErrors { field message } } }`,
        { redirect: { path: `/collections/${r.from}`, target: `/collections/${r.to}` } });
      const rue = rd.urlRedirectCreate.userErrors;
      out.push({ ...r, status: "renamed", redirect: rue.length ? "redirect error: " + rue[0].message : "redirect created" });
    } else out.push({ ...r, status: "would rename + create redirect" });
  }
  return out;
}

/* ================= fixMenus ================= */
function fixUrl(item, sportsUrl, log, path) {
  const title = String(item.title || "").trim().toLowerCase();
  const url = String(item.url || "");
  let next = null;

  if (title === "shop by type" && /\/collections\/(breslate|necklaces)$/.test(url)) next = "/collections/all";
  if (title === "jewelry" && /\/collections\/charms-only$/.test(url)) next = "/collections/all";
  if (title === "shop by theme" && /\/collections\/beady-chain-necklaces$/.test(url)) next = "/pages/shop-by-theme";
  if (/sports/.test(title) && /\/collections\/cop$/.test(url) && sportsUrl) next = sportsUrl;
  // keep Best Sellers pointing at the renamed handle
  if (title === "best sellers" && /\/collections\/pendant$/.test(url)) next = "/collections/best-sellers";
  if (title === "necklaces" && /\/collections\/breslate$/.test(url)) next = "/collections/necklaces";

  if (next && next !== url) { log.push({ path: path + " › " + item.title, from: url, to: next }); return next; }
  return url;
}
function transformItems(items, sportsUrl, log, path) {
  return (items || []).map(it => ({
    title: it.title,
    type: it.type,
    url: fixUrl(it, sportsUrl, log, path),
    resourceId: undefined, // force URL-based items so retargeting sticks
    tags: it.tags && it.tags.length ? it.tags : undefined,
    items: transformItems(it.items, sportsUrl, log, path + " › " + it.title)
  }));
}
async function fixMenus(dryRun) {
  // Verify the sports collection actually exists before pointing anything at it.
  const sp = await gql(`query($h: String!) { collectionByHandle(handle: $h) { id handle } }`, { h: SPORTS_HANDLE });
  const sportsUrl = sp.collectionByHandle ? `/collections/${SPORTS_HANDLE}` : null;

  const m = await gql(`query { menus(first: 25) { edges { node {
    id title handle
    items { title type url tags items { title type url tags items { title type url tags } } }
  } } } }`);
  const menus = m.menus.edges.map(e => e.node);
  const report = { sportsCollectionFound: !!sportsUrl, sportsHandleTried: SPORTS_HANDLE, changes: [], footerAboutUs: "n/a" };

  for (const menu of menus) {
    const log = [];
    let items = transformItems(menu.items, sportsUrl, log, menu.title);

    // Footer: append About Us if any footer-ish menu lacks it.
    if (/footer/i.test(menu.handle + " " + menu.title)) {
      const hasAbout = JSON.stringify(menu.items).toLowerCase().includes("/pages/about-us");
      if (!hasAbout) {
        items = items.concat([{ title: "About Us", type: "HTTP", url: "/pages/about-us", items: [] }]);
        report.footerAboutUs = dryRun ? "would add to " + menu.title : "added to " + menu.title;
      } else report.footerAboutUs = "already present";
    }

    if (log.length || (report.footerAboutUs || "").startsWith(dryRun ? "would" : "added")) {
      report.changes.push({ menu: menu.title, handle: menu.handle, edits: log });
      if (!dryRun) {
        const u = await gql(`mutation($id: ID!, $title: String!, $handle: String!, $items: [MenuItemUpdateInput!]!) {
          menuUpdate(id: $id, title: $title, handle: $handle, items: $items) {
            userErrors { field message } } }`,
          { id: menu.id, title: menu.title, handle: menu.handle, items: stripUndef(items) });
        const ue = u.menuUpdate.userErrors;
        if (ue.length) report.changes[report.changes.length - 1].error = ue[0].message;
      }
    }
  }
  return report;
}
function stripUndef(items) {
  return (items || []).map(it => {
    const o = { title: it.title, type: it.type || "HTTP", url: it.url };
    if (it.tags) o.tags = it.tags;
    const kids = stripUndef(it.items);
    if (kids.length) o.items = kids;
    return o;
  });
}

/* ================= createPages ================= */
const LOCAL_PAGE_BODY = `
<div class="brites-story">
  <h2>Made by hand, one piece at a time</h2>
  <p>Every Brites piece begins as raw metal on a jeweler's bench in our own workshop. Since 2014 we've cut, polished, engraved, and assembled more than <strong>1,000 original charm designs</strong> — one order, one piece, one person at a time.</p>
  <h3>What "made to order" means here</h3>
  <p>Nothing sits in a warehouse. When you order, your charm is cut and finished for you — your metal, your chain length, your engraving. That's how we can offer thousands of meaningful designs and still make each one feel personal.</p>
  <h3>Materials we stand behind</h3>
  <p>We work in <strong>sterling silver, 14k gold-filled, rose gold-filled, and solid 14k gold</strong>, ethically sourced from suppliers in Italy and the US. Most designs are available in every metal — including solid gold for heirloom-grade gifts.</p>
  <h3>Craftsmanship you can trust</h3>
  <p>Producing everything in-house means tighter quality control, fast turnaround on custom requests (yes — send us your handwriting, a drawing, or an idea), and a real person checking every piece before it ships in gift-ready packaging.</p>
  <h3>Our promise</h3>
  <ul>
    <li>Hassle-free 30-day returns</li>
    <li>Free shipping on orders over $75</li>
    <li>Handmade to order — engraved and assembled by hand</li>
    <li>Real people answering real questions: <a href="/pages/contact-us">contact us</a> any time</li>
  </ul>
  <p><a class="btn" href="/collections/best-sellers">Shop our customer favorites →</a></p>
</div>`;

async function createPages(dryRun) {
  const out = [];
  const targets = [
    { handle: "local-manufacturing", title: "Our Story — Made by Hand Since 2014", body: LOCAL_PAGE_BODY, templateSuffix: null },
    { handle: "shop-by-theme", title: "Shop by Theme", body: "<p>Find the charm that feels like them.</p>", templateSuffix: "shop-by-theme" }
  ];
  for (const t of targets) {
    const q = await gql(`query($q: String!) { pages(first: 1, query: $q) { edges { node { id handle } } } }`,
      { q: `handle:${t.handle}` });
    if (q.pages.edges.length) { out.push({ handle: t.handle, status: "already exists" }); continue; }
    if (dryRun) { out.push({ handle: t.handle, status: "would create" }); continue; }
    const c = await gql(`mutation($page: PageCreateInput!) {
      pageCreate(page: $page) { page { id handle } userErrors { field message } } }`,
      { page: { title: t.title, handle: t.handle, body: t.body, isPublished: true,
                templateSuffix: t.templateSuffix || undefined } });
    const ue = c.pageCreate.userErrors;
    out.push({ handle: t.handle, status: ue.length ? "error: " + ue[0].message : "created" });
  }
  return out;
}

/* ================= setCustomLabels ================= */
const THEME_RULES = [
  { label: "memorial",         re: /memorial|remembrance|sympathy|loss|angel.?wing|in.?loving/i },
  { label: "personalized",     re: /personalized|handwrit|initial|monogram|engrav|custom|name/i },
  { label: "profession-hobby", re: /nurse|doctor|teacher|firefight|police|military|pilot|sport|hockey|baseball|volleyball|cheer|dance|ballet|music|theatre|book|reader|science|grad/i },
  { label: "floral",           re: /floral|flower|sunflower|rose|daisy|lotus|blossom|botanical/i },
  { label: "animal-nature",    re: /animal|bird|cat|dog|bunny|rabbit|horse|wolf|bear|whale|turtle|hummingbird|cardinal|eagle|owl|fox|ocean|beach|mountain|nature|insect|butterfly|dragonfly|reptile/i },
  { label: "symbolic",         re: /zodiac|celestial|moon|sun|star|rune|viking|evil.?eye|cross|religious|faith|spiritual|awareness|ribbon|compass|heart/i }
];
function themeFor(tags, title) {
  const hay = (tags || []).join(" ") + " " + (title || "");
  for (const r of THEME_RULES) if (r.re.test(hay)) return r.label;
  return "general";
}
async function setCustomLabels(dryRun, cursor) {
  const d = await gql(`query($after: String) {
    products(first: 50, after: $after) {
      pageInfo { hasNextPage endCursor }
      edges { node { id title tags } }
    } }`, { after: cursor || null });
  const nodes = d.products.edges.map(e => e.node);
  const metafields = [];
  const sample = [];
  for (const p of nodes) {
    const theme = themeFor(p.tags, p.title);
    metafields.push({ ownerId: p.id, namespace: "mm-google-shopping", key: "custom_label_0",
                      type: "single_line_text_field", value: theme });
    if ((p.tags || []).includes("BJ-Best-Seller")) {
      metafields.push({ ownerId: p.id, namespace: "mm-google-shopping", key: "custom_label_1",
                        type: "single_line_text_field", value: "best-seller" });
    }
    if (sample.length < 5) sample.push({ title: p.title, theme });
  }
  if (!dryRun && metafields.length) {
    for (let i = 0; i < metafields.length; i += 25) {
      const r = await gql(`mutation($metafields: [MetafieldsSetInput!]!) {
        metafieldsSet(metafields: $metafields) { userErrors { field message } } }`,
        { metafields: metafields.slice(i, i + 25) });
      const ue = r.metafieldsSet.userErrors;
      if (ue.length) return { error: ue[0].message, cursor };
    }
  }
  return {
    processed: nodes.length, sample,
    done: !d.products.pageInfo.hasNextPage,
    cursor: d.products.pageInfo.endCursor
  };
}

/* ================= state + automatic runner ================= */
const STATE_DOC = "siteFixesState";
async function loadState() {
  const f = fb();
  if (!f) return { labelsCursor: null, labelsDone: false, structureDone: false, runs: 0 };
  try {
    const snap = await f.db.collection("Brites_Editor_Meta").doc(STATE_DOC).get();
    return snap.exists ? snap.data() : { labelsCursor: null, labelsDone: false, structureDone: false, runs: 0 };
  } catch (e) { return { labelsCursor: null, labelsDone: false, structureDone: false, runs: 0 }; }
}
async function saveState(state) {
  const f = fb();
  if (!f) return;
  try { await f.db.collection("Brites_Editor_Meta").doc(STATE_DOC).set(state); } catch (e) {}
}

async function autoRun() {
  const started = Date.now();
  const state = await loadState();
  state.runs = (state.runs || 0) + 1;
  state.lastRunAt = new Date().toISOString();
  const log = [];

  if (state.labelsDone && state.structureDone && state.returnsDone) {
    return { status: "all fixes complete — nothing to do", state };
  }

  // Structural fixes: cheap and idempotent, run every time until confirmed done.
  if (!state.structureDone) {
    try {
      const r1 = await renameHandles(false); log.push({ renameHandles: r1 });
      const r2 = await createPages(false);   log.push({ createPages: r2 });
      const r3 = await fixMenus(false);      log.push({ fixMenus: r3 });
      const renamed = r1.every(x => /renamed|already renamed/.test(x.status));
      const paged = r2.every(x => /created|already exists/.test(x.status));
      state.structureDone = renamed && paged;
      state.structureLog = log;
    } catch (e) { log.push({ structureError: String(e.message || e) }); }
  }

  // Checkout branding: one-shot, recorded in state.
  if (!state.checkoutBranded) {
    try { const r = await brandCheckout(false); log.push({ brandCheckout: r });
      state.checkoutBranded = true; } catch (e) { log.push({ brandCheckoutError: String(e.message || e) }); state.checkoutBranded = true; }
  }

  // Custom labels: time-budgeted pagination, resumes from saved cursor.
  if (!state.labelsDone) {
    try {
      let cursor = state.labelsCursor || null;
      let pages = 0, processed = state.labelsProcessed || 0;
      while (Date.now() - started < 7000) {           // stay under the function timeout
        const r = await setCustomLabels(false, cursor);
        if (r.error) { log.push({ labelsError: r.error }); break; }
        processed += r.processed; pages++; cursor = r.cursor;
        if (r.done) { state.labelsDone = true; break; }
      }
      state.labelsCursor = cursor;
      state.labelsProcessed = processed;
      log.push({ labels: { pagesThisRun: pages, totalProcessed: processed, done: state.labelsDone } });
    } catch (e) { log.push({ labelsError: String(e.message || e) }); }
  }

  // Returns-copy standardization (30 days) across all product descriptions.
  if (!state.returnsDone) {
    try {
      let cursor = state.returnsCursor || null, changed = state.returnsChanged || 0;
      while (Date.now() - started < 8500) {
        const r = await fixReturnsCopy(false, cursor);
        if (r.error) { log.push({ returnsError: r.error }); break; }
        changed += r.changed; cursor = r.cursor;
        if (r.done) { state.returnsDone = true; break; }
      }
      state.returnsCursor = cursor; state.returnsChanged = changed;
      log.push({ returnsCopy: { totalChanged: changed, done: !!state.returnsDone } });
    } catch (e) { log.push({ returnsError: String(e.message || e) }); }
  }

  if (state.labelsDone && state.structureDone && state.returnsDone) state.completedAt = new Date().toISOString();
  await saveState(state);
  return { status: state.labelsDone && state.structureDone && state.returnsDone ? "ALL FIXES COMPLETE" : "in progress — continues next scheduled run", log, state };
}



/* ================= fixReturnsCopy ================= */
async function fixReturnsCopy(dryRun, cursor) {
  // Standardize returns to 30 days inside product DESCRIPTIONS (catalog data,
  // not theme code). Cursor-paginated like setCustomLabels; idempotent.
  const d = await gql(`query($after: String) {
    products(first: 40, after: $after) {
      pageInfo { hasNextPage endCursor }
      edges { node { id descriptionHtml } }
    } }`, { after: cursor || null });
  const pats = [
    [/within\s*14\s*days/gi, "within 30 days"],
    [/14[- ]day returns?/gi, "30-day returns"],
    [/14[- ]day return policy/gi, "30-day return policy"]
  ];
  let changed = 0;
  for (const e of d.products.edges) {
    const before = e.node.descriptionHtml || "";
    let after = before;
    for (const [re, to] of pats) after = after.replace(re, to);
    if (after !== before) {
      changed++;
      if (!dryRun) {
        const r = await gql(`mutation($input: ProductInput!) {
          productUpdate(input: $input) { userErrors { field message } } }`,
          { input: { id: e.node.id, descriptionHtml: after } });
        const ue = r.productUpdate.userErrors;
        if (ue.length) return { error: ue[0].message, cursor };
      }
    }
  }
  return { scanned: d.products.edges.length, changed,
           done: !d.products.pageInfo.hasNextPage, cursor: d.products.pageInfo.endCursor };
}

/* ================= brandCheckout ================= */
async function brandCheckout(dryRun) {
  // Checkout pages can't be themed with liquid on standard plans; the
  // supported path is Shopify's Checkout Branding API. This pushes the design
  // language: square corners, ink/white palette, gold accent, serif headings.
  const prof = await gql(`query { checkoutProfiles(first: 5) { edges { node { id isPublished name } } } }`);
  const pub = prof.checkoutProfiles.edges.map(e => e.node).find(p => p.isPublished) ||
              (prof.checkoutProfiles.edges[0] && prof.checkoutProfiles.edges[0].node);
  if (!pub) return { status: "no checkout profile found" };
  if (dryRun) return { status: "would brand checkout profile", profile: pub.name };
  const input = {
    designSystem: {
      cornerRadius: { base: 0, small: 0, large: 0 },
      colors: {
        global: { brand: "#1c1d1d", accent: "#a58a52" },
        schemes: { scheme1: {
          base: { background: "#ffffff", text: "#1c1d1d", border: "#e6e4e0", accent: "#a58a52" },
          primaryButton: { background: "#1c1d1d", text: "#ffffff", hover: { background: "#000000" } }
        } }
      },
      typography: {
        primary: { shopifyFontGroup: { name: "Nunito Sans" } },
        secondary: { shopifyFontGroup: { name: "Cormorant Garamond" } }
      }
    },
    customizations: {
      headingLevel1: { typography: { font: "SECONDARY", letterCase: "NONE" } },
      headingLevel2: { typography: { font: "SECONDARY" } },
      primaryButton: { typography: { letterCase: "UPPER", kerning: "EXTRA_LOOSE" } }
    }
  };
  const r = await gql(`mutation($id: ID!, $input: CheckoutBrandingInput!) {
    checkoutBrandingUpsert(checkoutProfileId: $id, checkoutBrandingInput: $input) {
      userErrors { field message } } }`, { id: pub.id, input });
  const ue = r.checkoutBrandingUpsert.userErrors || [];
  return { status: ue.length ? "partial — " + ue.map(u => u.message).join("; ") : "checkout branded", profile: pub.name,
           note: ue.length ? "Some fields vary by API version/plan; everything accepted was applied. Rerun after adjusting if needed." : undefined };
}

/* ================= handler ================= */exports.handler = async function (event) {
  const headers = {
    "Access-Control-Allow-Origin": "https://britesjewelry.com",
    "Access-Control-Allow-Headers": "Content-Type, X-Edit-Passcode",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Content-Type": "application/json"
  };
  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers };

  // Netlify scheduled invocation -> fully automatic run, no auth, no input.
  const scheduled = !!(event.headers && (event.headers["x-nf-event"] === "schedule" || event.isScheduled));
  if (scheduled) {
    try {
      const out = await autoRun();
      console.log("[applySiteFixes] scheduled run:", JSON.stringify(out.status));
      return { statusCode: 200, headers, body: JSON.stringify(out) };
    } catch (e) {
      return { statusCode: 500, headers, body: JSON.stringify({ ok: false, error: String(e.message || e) }) };
    }
  }

  const pass = (event.headers && (event.headers["x-edit-passcode"] || event.headers["X-Edit-Passcode"])) || "";
  if (pass !== process.env.EDIT_PASSCODE) return { statusCode: 401, headers, body: JSON.stringify({ error: "bad passcode" }) };

  try {
    const b = JSON.parse(event.body || "{}");
    const dryRun = b.dryRun !== false; // dry-run unless explicitly false
    let result;
    switch (b.action) {
      case "auto":            result = await autoRun(); break;
      case "renameHandles":   result = await renameHandles(dryRun); break;
      case "fixMenus":        result = await fixMenus(dryRun); break;
      case "createPages":     result = await createPages(dryRun); break;
      case "setCustomLabels": result = await setCustomLabels(dryRun, b.cursor); break;
      case "brandCheckout":   result = await brandCheckout(dryRun); break;
      case "fixReturnsCopy":  result = await fixReturnsCopy(dryRun, b.cursor); break;
      case "status": {
        const bs = await gql(`query { a: collectionByHandle(handle: "best-sellers") { id }
          b: collectionByHandle(handle: "necklaces") { id }
          p1: pages(first: 1, query: "handle:local-manufacturing") { edges { node { id } } }
          p2: pages(first: 1, query: "handle:shop-by-theme") { edges { node { id } } } }`);
        result = {
          handlesRenamed: { bestSellers: !!bs.a, necklaces: !!bs.b },
          pages: { localManufacturing: bs.p1.edges.length > 0, shopByTheme: bs.p2.edges.length > 0 }
        };
        break;
      }
      default: return { statusCode: 400, headers, body: JSON.stringify({ error: "unknown action", actions: ["renameHandles","fixMenus","createPages","setCustomLabels","status"] }) };
    }
    return { statusCode: 200, headers, body: JSON.stringify({ ok: true, action: b.action, dryRun, result }, null, 1) };
  } catch (e) {
    return { statusCode: 500, headers, body: JSON.stringify({ ok: false, error: String(e.message || e) }) };
  }
};
