// Complete-ad end-to-end harness: one fully mocked run of the "complete ad" approval flow.
//
// Drives the real console API (googleAdsAutopilotKick.httpHandler), the real background worker and the
// real engine through every hand-off of a complete ad:
//   send to Approval -> review previews -> edit and save messaging -> choose ad types, budgets, durations
//   and countries -> prepare the plan -> "Check with Google" (validate only) -> Approve ad (publish) ->
//   applyApproval mutates -> the reviewed films upload and attach to the new paused campaign -> verify.
// Only the outside world is fake:
//   * Google Ads REST: a synthetic account with a generic GAQL engine, an atomic googleAds:mutate that
//     enforces the Google rules this flow depends on (text lengths and counts, image shapes and sizes,
//     listing-group trees, responsive and fixed Display ads, temporary-ID order and uniqueness, micros,
//     names, date-times), honours validateOnly, returns real resource names and GoogleAdsFailure
//     errors, and can lose a response or report a partial failure on request; the resumable YouTube
//     upload protocol and you_tube_video_upload processing state
//   * Merchant Center: shopping_product rows (status and availability) read through Google Ads
//   * Firestore and Storage: in memory, rejecting values real Firestore rejects, with serialised
//     transactions (optimistic retry) and a storage bucket with CORS metadata and signed URLs
//   * every other network call is refused: paid AI endpoints are recorded as violations and failed,
//     and real sockets are blocked outright. Nothing generates images or films; fixtures are synthetic.
// Scenarios
//   S1 four products, each in its own workspace (the path that works today): the whole flow per product
//   S2 plan matrix: each ad type alone and all three, budgets, durations, countries, joining an existing
//      Performance Max campaign, and the daily ceiling refused at preparation and at publication
//   S3 dry-run mode: Approve validates only and creates nothing
//   S4 idempotency: double Approve, approving again, re-sending, lost response, partial failure,
//      definite rejection, duplicate campaign name, Merchant eligibility
//   S5 films are refused unless the paused destination isolates exactly the product
//   S6 four products in ONE workspace (Duck selected; Gecko, Bunny and Saturn remembered in
//      w.productDesigns with their jobs in the workspace history): the owner's four waiting ads, sent with
//      the current code (frozen "saved-package" snapshots). A later studio edit of Duck (messaging and a new
//      crop) leaves its review current and unchanged: Approvals publishes exactly what was sent
//   S7 the same four ads as LEGACY reviews, in the format written before commit 19635c8 (no sourceMode, no
//      sourceSetId, no pinned publicationImages; sourceHash = the workspace selection hash when sent), with
//      Duck selected and Gecko's messaging edited in Approvals before the upgrade: previews, the
//      convert-on-prepare path, and publication of every one of them
//   S8 adversity: an Approve interrupted before anything is sent (another publication running) returns the ad to
//      its own card; the generic approve / "Publish again" routes never publish a complete ad (they would skip the
//      Merchant check and the films); Merchant offers ineligible, unreadable or partly eligible at Approve; two tabs
//      saving and preparing one review; saved videos excluded; every action without the passcode; a dry-run join;
//      joining a running campaign (no film promised); and self-tests that the fake rejects a campaign without the
//      EU political-advertising declaration and an image ad without a display URL on its final URL's domain.
//   S9 a Fixed Display plan saved on its approval before image ads carried Google's required display URL (Paul's waiting
//      card): preparing again returns it as saved, and Check with Google and Approve send every image ad with the display
//      URL of its own final URL, while the stored, reviewed plan and its hash stay unchanged.
//   Every scenario also checks no response carries a secret.
// Tags
//   [SCOPE] depends on reviews using their own pinned product and group rather than the workspace's current
//           selection (the review-scope fix, upstream commit 19635c8). Enforced by default; set
//           COMPLETE_AD_SCOPE_STRICT=0 to report them as XFAIL on a checkout without that fix.
//   [KNOWN] a defect found by this harness. Reported XFAIL until enforced with COMPLETE_AD_STRICT=1.
//           A [KNOWN] check that passes prints XPASS: the defect is fixed, drop its tag.
// Run:  node -r ./tests/adwords/suite-guard.cjs tests/adwords/complete-ad-e2e.cjs
//       COMPLETE_AD_STRICT=1 node -r ./tests/adwords/suite-guard.cjs tests/adwords/complete-ad-e2e.cjs  (known defects fail)
//       COMPLETE_AD_ONLY=S6,S7 runs chosen scenarios; COMPLETE_AD_DEBUG=1 prints rejected Google requests.
'use strict';
const assert = require('assert/strict'), path = require('path'), Module = require('module'), crypto = require('crypto');
const ROOT = path.resolve(__dirname, '../..'), FN = path.join(ROOT, 'netlify', 'functions');
const STRICT = process.env.COMPLETE_AD_STRICT === '1', SCOPE_STRICT = STRICT || process.env.COMPLETE_AD_SCOPE_STRICT !== '0';
const DEBUG = !!process.env.COMPLETE_AD_DEBUG, ONLY = process.env.COMPLETE_AD_ONLY ? process.env.COMPLETE_AD_ONLY.split(',') : null;

/* ================================================================ check registry */
const results = []; let scenarioName = 'setup';
function check(ok, name, opts = {}) {
  const tag = opts.tag || null, pass = !!ok, enforced = !tag || (tag === 'scope' ? SCOPE_STRICT : STRICT);
  const status = pass ? (tag === 'known' ? 'XPASS' : 'PASS') : enforced ? 'FAIL' : 'XFAIL';
  results.push({ scenario: scenarioName, name, tag, status, detail: pass || opts.detail == null ? null : String(opts.detail) });
  console.log(status.padEnd(5) + ' ' + (tag ? '[' + tag.toUpperCase() + '] ' : '') + scenarioName + ' · ' + name
    + (!pass && opts.detail != null ? '\n        ' + String(opts.detail).replace(/\s+/g, ' ').slice(0, 600) : ''));
  return pass;
}
// A stage of a flow: once a gating step fails, every later step is recorded as failed ("not reached"),
// so the pass/fail list stays complete and exact. Conditions are thunks so a blocked step never runs.
function stage(tag, label) {
  const st = { blocked: null };
  st.check = (name, fn, detail) => {
    if (st.blocked) return check(false, name, { tag, detail: 'not reached: ' + st.blocked });
    let ok = false, d = null;
    try { ok = !!fn(); } catch (e) { d = 'threw: ' + (e && e.stack ? e.stack.split('\n').slice(0, 2).join(' ') : e); }
    if (!ok && d == null && detail != null) { try { d = typeof detail === 'function' ? detail() : detail; } catch (e) { d = String(e && e.message); } }
    return check(ok, name, { tag, detail: d });
  };
  st.gate = (name, fn, detail) => { const ok = st.check(name, fn, detail); if (!ok && !st.blocked) st.blocked = label + ' — ' + name; return ok; };
  return st;
}
const why = r => r == null ? 'no response' : r.error || r.message || JSON.stringify(r).slice(0, 400);

/* ================================================================ network guard */
const netBlocked = [];
for (const [mod, keys] of [['http', ['request', 'get']], ['https', ['request', 'get']], ['net', ['connect', 'createConnection']], ['tls', ['connect']], ['http2', ['connect']]]) {
  const m = require(mod); for (const k of keys) m[k] = () => { netBlocked.push(mod + '.' + k); throw new Error('complete-ad-e2e: live network is forbidden (' + mod + '.' + k + ')'); };
}
globalThis.fetch = async url => { netBlocked.push('fetch ' + url); throw new Error('complete-ad-e2e: live network is forbidden (global fetch)'); };

/* ================================================================ environment */
const CID = '1234567890', MERCHANT = '5550001', PASS = 'synthetic-pass', SITE = 'https://console.synthetic.test', TZ = 'America/Toronto';
for (const k of ['GADS_CURRENCY', 'GADS_TARGET_ROAS', 'GADS_MAX_DAILY_BUDGET_TOTAL', 'GADS_API_VERSION', 'GADS_GEN_MODEL', 'GMC_REFRESH_TOKEN', 'GMC_CLIENT_ID', 'GMC_CLIENT_SECRET',
  'MERCHANT_CENTER_ID', 'GADS_PMAX_AUDIENCE_RESOURCE', 'GADS_PMAX_AUDIENCE_ID', 'GADS_CONVERSION_ACTION', 'GADS_CONVERSION_UPLOAD_API', 'GADS_NEW_CAMPAIGN_BUDGET', 'GADS_TERM_EXCLUSIONS',
  'GADS_MESSAGING_RULES', 'SITE_NAME', 'KP_BACKOFF_MS']) delete process.env[k];
Object.assign(process.env, {
  GADS_CLIENT_ID: 'synthetic.apps.googleusercontent.com', GADS_CLIENT_SECRET: 'synthetic-client-secret', GADS_REFRESH_TOKEN: 'synthetic-refresh-token', GADS_DEVELOPER_TOKEN: 'synthetic-developer-token',
  GADS_CUSTOMER_ID: CID, GADS_LOGIN_CUSTOMER_ID: CID, GMC_MERCHANT_ID: MERCHANT,
  // Present on purpose: a paid AI request is attempted (and caught by the fake) instead of being skipped silently.
  OPENAI_API_KEY: 'synthetic-never-sent', ANTHROPIC_API_KEY: 'synthetic-never-sent', GEMINI_API_KEY: 'synthetic-never-sent',
  SHOPIFY_STORE: 'synthetic-store.myshopify.com', SHOPIFY_CLIENT_ID: 'synthetic', SHOPIFY_CLIENT_SECRET: 'synthetic-shopify-secret', FIREBASE_PRIVATE_KEY: 'synthetic-firebase-key', URL: SITE, EDIT_PASSCODE: PASS
});
// Every secret the functions can read; none may appear in a console or worker response.
const SECRETS = [PASS, 'synthetic-never-sent', 'synthetic-token', 'synthetic-client-secret', 'synthetic-refresh-token', 'synthetic-developer-token', 'synthetic-shopify-secret', 'synthetic-firebase-key'];
const sha = v => crypto.createHash('sha256').update(typeof v === 'string' ? v : JSON.stringify(v)).digest('hex');
const sha256 = b => crypto.createHash('sha256').update(b).digest('hex');
const creativeHash = v => crypto.createHash('sha256').update(JSON.stringify(v)).digest('hex'); // engine's creativeHash for a string
const clone = v => JSON.parse(JSON.stringify(v));
const len = s => [...String(s == null ? '' : s)].length;
const ymd = (offsetDays = 0) => new Intl.DateTimeFormat('en-CA', { timeZone: TZ, year: 'numeric', month: '2-digit', day: '2-digit' }).format(new Date(Date.now() + offsetDays * 86400000));
const addDays = (day, n) => new Date(Date.parse(day + 'T12:00:00Z') + n * 86400000).toISOString().slice(0, 10);

/* ================================================================ in-memory Firestore and Storage */
class Timestamp {
  constructor(ms) { Object.defineProperty(this, '_ms', { value: ms }); this.seconds = Math.floor(ms / 1000); this.nanoseconds = (ms % 1000) * 1e6; }
  toMillis() { return this._ms; } toDate() { return new Date(this._ms); } valueOf() { return this._ms; }
  static now() { return new Timestamp(Date.now()); } static fromMillis(ms) { return new Timestamp(ms); } static fromDate(d) { return new Timestamp(d.getTime()); }
}
const FieldValue = { serverTimestamp: () => ({ __fv: 'ts' }), increment: n => ({ __fv: 'inc', n }), arrayUnion: (...v) => ({ __fv: 'union', v }), arrayRemove: (...v) => ({ __fv: 'remove', v }), delete: () => ({ __fv: 'del' }) };
function createStore() {
  const docs = new Map(), versions = new Map(), files = new Map(), signed = []; let auto = 0, contention = 0;
  let bucketMeta = { cors: [{ origin: ['https://other-app.synthetic.test'], method: ['GET'], maxAgeSeconds: 60 }], metageneration: '3' };
  const bump = p => versions.set(p, (versions.get(p) || 0) + 1);
  const isFV = v => v && typeof v === 'object' && typeof v.__fv === 'string';
  const isPlain = v => v && typeof v === 'object' && (Object.getPrototypeOf(v) === Object.prototype || Object.getPrototypeOf(v) === null);
  // firebaseAdmin.js sets no ignoreUndefinedProperties: real Firestore rejects these values.
  function validate(v, at, inArray) {
    if (v === undefined) throw new Error('Firestore rejects undefined at ' + at);
    if (typeof v === 'function' || typeof v === 'symbol' || typeof v === 'bigint') throw new Error('Firestore cannot store ' + typeof v + ' at ' + at);
    if (typeof v === 'number' && Number.isNaN(v) && false) return;
    if (v === null || typeof v !== 'object' || v instanceof Timestamp || Buffer.isBuffer(v) || v instanceof Date || isFV(v)) return;
    if (Array.isArray(v)) { if (inArray) throw new Error('Firestore rejects nested arrays at ' + at); v.forEach((x, i) => validate(x, at + '[' + i + ']', true)); return; }
    if (!isPlain(v)) throw new Error('Firestore cannot serialize ' + (v.constructor && v.constructor.name) + ' at ' + at);
    for (const [k, x] of Object.entries(v)) validate(x, at + '.' + k, false);
  }
  const cl = v => v instanceof Timestamp ? v : v instanceof Date ? new Timestamp(v.getTime()) : Buffer.isBuffer(v) ? Buffer.from(v)
    : Array.isArray(v) ? v.map(cl) : v && typeof v === 'object' ? Object.fromEntries(Object.entries(v).map(([k, x]) => [k, cl(x)])) : v;
  function resolve(v, prior) {
    if (isFV(v)) {
      if (v.__fv === 'ts') return new Timestamp(Date.now());
      if (v.__fv === 'inc') return (Number(prior) || 0) + v.n;
      if (v.__fv === 'union') { const out = Array.isArray(prior) ? prior.slice() : []; v.v.forEach(x => { if (!out.some(y => JSON.stringify(y) === JSON.stringify(x))) out.push(cl(x)); }); return out; }
      if (v.__fv === 'remove') return (Array.isArray(prior) ? prior : []).filter(y => !v.v.some(x => JSON.stringify(x) === JSON.stringify(y)));
    }
    if (isPlain(v)) return Object.fromEntries(Object.entries(v).filter(([, x]) => !(isFV(x) && x.__fv === 'del')).map(([k, x]) => [k, resolve(x, prior && prior[k])]));
    return cl(v);
  }
  function merge(target, patch) {
    for (const [k, v] of Object.entries(patch)) {
      if (isFV(v) && v.__fv === 'del') { delete target[k]; continue; }
      if (isPlain(v) && !isFV(v)) { target[k] = isPlain(target[k]) ? target[k] : {}; merge(target[k], v); } else target[k] = resolve(v, target[k]);
    }
    return target;
  }
  const sizeCheck = (p, data) => { if (Buffer.byteLength(JSON.stringify(data)) > 1048487) throw new Error('Firestore document ' + p + ' exceeds the maximum allowed size'); };
  const getField = (o, f) => String(f).split('.').reduce((a, k) => (a == null ? undefined : a[k]), o);
  const cmp = (a, b) => { a = a instanceof Timestamp ? a.toMillis() : a; b = b instanceof Timestamp ? b.toMillis() : b; return a < b ? -1 : a > b ? 1 : 0; };
  const snap = (p, ref) => { const d = docs.get(p); return { id: p.split('/').pop(), ref, exists: d !== undefined, data: () => (d === undefined ? undefined : cl(d)), get: f => cl(getField(d, f)) }; };
  // Writes validate synchronously, as the Admin SDK does; transactions and batches apply them atomically on commit.
  function writer(p) {
    return {
      set: (data, opts) => { validate(data, p); return () => { const next = opts && opts.merge ? merge(cl(docs.get(p) || {}), data) : resolve(data, docs.get(p)); sizeCheck(p, next); docs.set(p, next); bump(p); }; },
      update: data => { validate(data, p); return () => {
        if (!docs.has(p)) throw Object.assign(new Error('5 NOT_FOUND: No document to update: ' + p), { code: 5 });
        const next = cl(docs.get(p));
        for (const [key, v] of Object.entries(data)) {
          const parts = key.split('.'); let at = next;
          for (const part of parts.slice(0, -1)) at = isPlain(at[part]) ? at[part] : (at[part] = {});
          const last = parts[parts.length - 1];
          if (isFV(v) && v.__fv === 'del') delete at[last]; else at[last] = resolve(v, at[last]);
        }
        sizeCheck(p, next); docs.set(p, next); bump(p); }; },
      create: data => { validate(data, p); return () => { if (docs.has(p)) throw Object.assign(new Error('6 ALREADY_EXISTS: ' + p), { code: 6 }); docs.set(p, resolve(data)); bump(p); }; },
      delete: () => () => { docs.delete(p); bump(p); }
    };
  }
  function commit(ops) {
    const backup = new Map(); ops.forEach(({ p }) => { if (!backup.has(p)) backup.set(p, docs.has(p) ? docs.get(p) : undefined); });
    try { ops.forEach(({ fn }) => fn()); } catch (e) { for (const [p, d] of backup) { if (d === undefined) docs.delete(p); else docs.set(p, d); } throw e; }
  }
  function docRef(p) {
    const w = writer(p), ref = {
      id: p.split('/').pop(), path: p, _doc: true,
      get parent() { return collRef(p.split('/').slice(0, -1).join('/')); },
      get: async () => snap(p, ref),
      set: async (data, opts) => commit([{ p, fn: w.set(data, opts) }]), update: async data => commit([{ p, fn: w.update(data) }]),
      create: async data => commit([{ p, fn: w.create(data) }]), delete: async () => commit([{ p, fn: w.delete() }]),
      collection: n => collRef(p + '/' + n), _w: w
    };
    return ref;
  }
  function query(p, filters, order, lim, after) {
    const q = {
      where: (f, op, v) => query(p, filters.concat([[f, op, v]]), order, lim, after),
      orderBy: (f, dir = 'asc') => query(p, filters, order.concat([[f, dir]]), lim, after),
      limit: n => query(p, filters, order, n, after), startAfter: v => query(p, filters, order, lim, v), select: () => q,
      get: async () => {
        let rows = [...docs.keys()].filter(k => k.startsWith(p + '/') && !k.slice(p.length + 1).includes('/')).map(k => ({ k, d: docs.get(k) }));
        for (const [f, op, v] of filters) rows = rows.filter(({ d }) => { const x = getField(d, f);
          if (op === '==') return x !== undefined && cmp(x, v) === 0; if (op === '!=') return x !== undefined && cmp(x, v) !== 0;
          if (op === 'in') return v.some(y => cmp(x, y) === 0); if (op === 'not-in') return x !== undefined && !v.some(y => cmp(x, y) === 0);
          if (op === 'array-contains') return Array.isArray(x) && x.some(y => cmp(y, v) === 0);
          if (x === undefined) return false; const c = cmp(x, v); return op === '<' ? c < 0 : op === '<=' ? c <= 0 : op === '>' ? c > 0 : op === '>=' ? c >= 0 : false; });
        for (const [f] of order) rows = rows.filter(({ d }) => getField(d, f) !== undefined);
        if (order.length) rows.sort((a, b) => { for (const [f, dir] of order) { const c = cmp(getField(a.d, f), getField(b.d, f)); if (c) return dir === 'desc' ? -c : c; } return 0; });
        if (after != null) { const i = rows.findIndex(r => r.k.split('/').pop() === (after.id || after)); if (i >= 0) rows = rows.slice(i + 1); }
        if (lim != null) rows = rows.slice(0, lim);
        const list = rows.map(({ k }) => snap(k, docRef(k)));
        return { docs: list, size: list.length, empty: !list.length, forEach: fn => list.forEach(fn) };
      }
    };
    return q;
  }
  function collRef(p) { return Object.assign(query(p, [], [], null, null), { id: p.split('/').pop(), path: p, doc: id => docRef(p + '/' + (id || 'auto' + String(++auto).padStart(6, '0'))),
    add: async data => { const ref = docRef(p + '/auto' + String(++auto).padStart(6, '0')); await ref.set(data); return ref; } }); }
  const db = {
    collection: collRef, doc: docRef, getAll: async (...refs) => Promise.all(refs.map(r => r.get())),
    // Serialised like the Admin SDK: a transaction whose reads changed before its commit runs again.
    runTransaction: async fn => {
      for (let attempt = 0; attempt < 12; attempt++) {
        const reads = new Map(), ops = [];
        const tx = {
          get: async r => { if (r && r._doc) { if (!reads.has(r.path)) reads.set(r.path, versions.get(r.path) || 0); } return r.get(); },
          set: (r, v, o) => { ops.push({ p: r.path, fn: r._w.set(v, o) }); return tx; }, update: (r, v) => { ops.push({ p: r.path, fn: r._w.update(v) }); return tx; },
          delete: r => { ops.push({ p: r.path, fn: r._w.delete() }); return tx; }, create: (r, v) => { ops.push({ p: r.path, fn: r._w.create(v) }); return tx; }
        };
        const out = await fn(tx);
        if ([...reads].some(([p, v]) => (versions.get(p) || 0) !== v)) { contention++; continue; }
        commit(ops); return out;
      }
      throw Object.assign(new Error('10 ABORTED: Too much contention on these documents.'), { code: 10 });
    },
    batch: () => { const ops = [], b = { set: (r, v, o) => { ops.push({ p: r.path, fn: r._w.set(v, o) }); return b; }, update: (r, v) => { ops.push({ p: r.path, fn: r._w.update(v) }); return b; },
      delete: r => { ops.push({ p: r.path, fn: r._w.delete() }); return b; }, create: (r, v) => { ops.push({ p: r.path, fn: r._w.create(v) }); return b; }, commit: async () => commit(ops) }; return b; }
  };
  const notFound = p => Object.assign(new Error('No such object: ' + p), { code: 404 });
  const bucket = {
    name: 'synthetic-creative-bucket',
    getMetadata: async () => [clone(bucketMeta)],
    setMetadata: async (patch, opts = {}) => {
      if (opts.ifMetagenerationMatch != null && String(opts.ifMetagenerationMatch) !== String(bucketMeta.metageneration)) throw Object.assign(new Error('412 Precondition Failed'), { code: 412 });
      bucketMeta = { ...bucketMeta, ...clone(patch), metageneration: String(Number(bucketMeta.metageneration) + 1) }; return [clone(bucketMeta)];
    },
    file: p => ({ name: p,
      save: async (b, opts = {}) => { if (opts.preconditionOpts && opts.preconditionOpts.ifGenerationMatch === 0 && files.has(p)) throw Object.assign(new Error('412 Precondition Failed'), { code: 412 }); files.set(p, Buffer.from(b)); },
      download: async () => { if (!files.has(p)) throw notFound(p); return [Buffer.from(files.get(p))]; },
      exists: async () => [files.has(p)],
      delete: async (o = {}) => { if (!files.has(p) && !o.ignoreNotFound) throw notFound(p); files.delete(p); },
      getSignedUrl: async () => { signed.push(p); return ['https://storage.synthetic.test/' + encodeURIComponent(p) + '?X-Goog-Signature=synthetic']; },
      getMetadata: async () => { if (!files.has(p)) throw notFound(p); return [{ name: p, size: String(files.get(p).length), metadata: {} }]; },
      setMetadata: async m => [m] })
  };
  return { docs, files, signed, db, bucket, get contention() { return contention; }, get bucketMeta() { return bucketMeta; } };
}

/* ================================================================ synthetic Google Ads account (+ Merchant data) */
const FIXED_SIZES = new Set(['200x200', '240x400', '250x250', '250x360', '300x250', '336x280', '580x400', '120x600', '160x600', '300x600', '300x1050', '468x60', '728x90', '930x180', '970x90', '970x250', '980x120', '300x50', '320x50', '320x100']);
const IMAGE_SPEC = { MARKETING_IMAGE: [1.91, 600, 314], SQUARE_MARKETING_IMAGE: [1, 300, 300], PORTRAIT_MARKETING_IMAGE: [0.8, 480, 600], LOGO: [1, 128, 128], LANDSCAPE_LOGO: [4, 512, 128] };
const TEXT_SPEC = { HEADLINE: 30, LONG_HEADLINE: 90, DESCRIPTION: 90, BUSINESS_NAME: 25 };
// The v24 Campaign fields this flow may send at creation (REST rejects any other name).
const CAMPAIGN_FIELDS = new Set(['resourceName', 'name', 'status', 'advertisingChannelType', 'advertisingChannelSubType', 'campaignBudget', 'brandGuidelinesEnabled', 'containsEuPoliticalAdvertising', 'shoppingSetting', 'assetAutomationSettings', 'geoTargetTypeSetting',
  'networkSettings', 'finalUrlSuffix', 'trackingUrlTemplate', 'startDateTime', 'endDateTime', 'maximizeConversionValue', 'maximizeConversions', 'manualCpc', 'targetSpend', 'targetRoas', 'targetCpa', 'biddingStrategy']);
const GEO = { 2840: ['United States', 'US'], 2124: ['Canada', 'CA'], 2826: ['United Kingdom', 'GB'], 2036: ['Australia', 'AU'] };
const OP_TYPES = {
  campaignBudgetOperation: ['campaignBudgets', 'campaignBudgetResult'], campaignOperation: ['campaigns', 'campaignResult'], campaignCriterionOperation: ['campaignCriteria', 'campaignCriterionResult'],
  assetOperation: ['assets', 'assetResult'], assetGroupOperation: ['assetGroups', 'assetGroupResult'], assetGroupAssetOperation: ['assetGroupAssets', 'assetGroupAssetResult'],
  assetGroupListingGroupFilterOperation: ['assetGroupListingGroupFilters', 'assetGroupListingGroupFilterResult'], assetGroupSignalOperation: ['assetGroupSignals', 'assetGroupSignalResult'],
  campaignAssetOperation: ['campaignAssets', 'campaignAssetResult'], adGroupOperation: ['adGroups', 'adGroupResult'], adGroupAdOperation: ['adGroupAds', 'adGroupAdResult'],
  campaignConversionGoalOperation: ['campaignConversionGoals', 'campaignConversionGoalResult']
};
function createGoogle() {
  const g = { nextId: 7100000, customer: { resourceName: 'customers/' + CID, id: CID, timeZone: TZ, currencyCode: 'CAD', descriptiveName: 'Synthetic Brites account' },
    campaigns: new Map(), budgets: new Map(), assets: new Map(), assetGroups: new Map(), links: [], filters: [], signals: [], criteria: [], campaignAssets: [], adGroups: new Map(), ads: [],
    goals: [], goalUpdates: [], products: [], uploads: new Map(), sessions: new Map(), requests: [], unknownGaql: new Set(), faults: [], linkSeq: 0 };
  g.id = () => String(++g.nextId);
  return g;
}
const RN = (coll, id) => `customers/${CID}/${coll}/${id}`;
function seedGoogle(g) {
  const budget = (id, amount, status = 'ENABLED') => { const rn = RN('campaignBudgets', id); g.budgets.set(rn, { resourceName: rn, id, name: 'Synthetic budget ' + id, amountMicros: String(amount * 1e6), deliveryMethod: 'STANDARD', status, period: 'DAILY' }); return rn; };
  const campaign = c => { const rn = RN('campaigns', c.id); g.campaigns.set(rn, { resourceName: rn, servingStatus: 'SERVING', primaryStatus: c.status === 'ENABLED' ? 'ELIGIBLE' : 'PAUSED', ...c }); return rn; };
  // An enabled Search campaign spends 70 of the 100 daily ceiling: 30 remains for a new ad.
  campaign({ id: '9100', name: 'Brites · Everyday Search', status: 'ENABLED', advertisingChannelType: 'SEARCH', biddingStrategyType: 'MANUAL_CPC', campaignBudget: budget('91000', 70) });
  // A paused retail Performance Max campaign (brand guidelines on) that a product may join.
  const pmax = campaign({ id: '9001', name: 'Brites · Animal Charms PMax', status: 'PAUSED', advertisingChannelType: 'PERFORMANCE_MAX', biddingStrategyType: 'MAXIMIZE_CONVERSION_VALUE', maximizeConversionValue: {},
    brandGuidelinesEnabled: true, shoppingSetting: { merchantId: MERCHANT, feedLabel: 'US' }, campaignBudget: budget('90010', 25) });
  const fox = RN('assetGroups', '9002');
  g.assetGroups.set(fox, { resourceName: fox, id: '9002', name: 'AG · Fox Charm Necklace', campaign: pmax, finalUrls: ['https://britesjewelry.com/products/fox-charm-necklace'], status: 'ENABLED', primaryStatus: 'PAUSED' });
  const root = RN('assetGroupListingGroupFilters', '9002~1');
  g.filters.push({ resourceName: root, assetGroup: fox, type: 'SUBDIVISION', listingSource: 'SHOPPING' },
    { resourceName: RN('assetGroupListingGroupFilters', '9002~2'), assetGroup: fox, parentListingGroupFilter: root, type: 'UNIT_INCLUDED', listingSource: 'SHOPPING', caseValue: { productItemId: { value: 'shopify_us_8199_44991' } } },
    { resourceName: RN('assetGroupListingGroupFilters', '9002~3'), assetGroup: fox, parentListingGroupFilter: root, type: 'UNIT_EXCLUDED', listingSource: 'SHOPPING', caseValue: { productItemId: {} } });
  for (const geo of ['2840', '2124']) g.criteria.push({ resourceName: RN('campaignCriteria', '9001~' + geo), campaign: pmax, criterionId: geo, type: 'LOCATION', status: 'ENABLED', location: { geoTargetConstant: 'geoTargetConstants/' + geo } });
  g.goals.push({ category: 'PURCHASE', origin: 'WEBSITE', biddable: true }, { category: 'ADD_TO_CART', origin: 'WEBSITE', biddable: true }, { category: 'BEGIN_CHECKOUT', origin: 'WEBSITE', biddable: true });
  for (const p of [...PRODUCTS, { title: 'Fox Charm Necklace', offers: ['shopify_us_8199_44991'] }]) for (const offer of p.offers)
    g.products.push({ resourceName: `customers/${CID}/shoppingProducts/${MERCHANT}~US~en~${offer}`, merchantCenterId: MERCHANT, itemId: offer, title: p.title, status: 'ELIGIBLE', availability: 'IN_STOCK', feedLabel: 'US', languageCode: 'en', targetCountries: ['US', 'CA'], priceMicros: '58000000', currencyCode: 'CAD' });
}

/* ---------------- generic GAQL engine */
const camel = s => s.replace(/_([a-z0-9])/g, (_, c) => c.toUpperCase());
function splitTop(s, re) { const out = []; let depth = 0, quote = null, cur = ''; for (let i = 0; i < s.length; i++) { const ch = s[i];
  if (quote) { cur += ch; if (ch === '\\') { cur += s[++i] || ''; continue; } if (ch === quote) quote = null; continue; }
  if (ch === "'" || ch === '"') { quote = ch; cur += ch; continue; } if (ch === '(') depth++; if (ch === ')') depth--;
  if (!depth) { const m = s.slice(i).match(re); if (m && m.index === 0) { out.push(cur); cur = ''; i += m[0].length - 1; continue; } } cur += ch; }
  out.push(cur); return out.map(x => x.trim()).filter(Boolean); }
function scalar(s) { s = s.trim(); if (s[0] === "'" || s[0] === '"') { const q = s[0]; let out = ''; for (let i = 1; i < s.length; i++) { const ch = s[i]; if (ch === '\\') { out += s[++i]; continue; } if (ch === q) break; out += ch; } return out; }
  if (/^(TRUE|FALSE)$/i.test(s)) return /^TRUE$/i.test(s); return s; }
function gaqlRows(g, from) {
  const camp = rn => g.campaigns.get(rn) || {}, group = rn => g.assetGroups.get(rn) || {};
  const live = [...g.campaigns.values()];
  switch (from) {
    case 'customer': return [{ customer: g.customer }];
    case 'campaign': return live.map(c => ({ campaign: c, campaignBudget: g.budgets.get(c.campaignBudget) || {} }));
    case 'campaign_budget': return [...g.budgets.values()].map(b => ({ campaignBudget: b }));
    case 'asset_group': return [...g.assetGroups.values()].map(a => ({ campaign: camp(a.campaign), assetGroup: a }));
    case 'asset_group_listing_group_filter': return g.filters.map(f => ({ campaign: camp(group(f.assetGroup).campaign), assetGroup: group(f.assetGroup), assetGroupListingGroupFilter: f }));
    case 'asset_group_asset': return g.links.map(l => ({ campaign: camp(group(l.assetGroup).campaign), assetGroup: group(l.assetGroup), assetGroupAsset: l, asset: g.assets.get(l.asset) || {} }));
    case 'asset_group_signal': return g.signals.map(s => ({ campaign: camp(group(s.assetGroup).campaign), assetGroup: group(s.assetGroup), assetGroupSignal: s }));
    case 'campaign_criterion': return g.criteria.map(c => ({ campaign: camp(c.campaign), campaignCriterion: c }));
    case 'campaign_asset': return g.campaignAssets.map(c => ({ campaign: camp(c.campaign), campaignAsset: c, asset: g.assets.get(c.asset) || {} }));
    case 'ad_group': return [...g.adGroups.values()].map(a => ({ campaign: camp(a.campaign), adGroup: a }));
    case 'ad_group_ad': return g.ads.map(a => ({ campaign: camp((g.adGroups.get(a.adGroup) || {}).campaign), adGroup: g.adGroups.get(a.adGroup) || {}, adGroupAd: a }));
    case 'asset': return [...g.assets.values()].map(a => ({ asset: a }));
    case 'customer_conversion_goal': return g.goals.map(x => ({ customerConversionGoal: x }));
    case 'shopping_product': return g.products.map(p => ({ shoppingProduct: p }));
    case 'you_tube_video_upload': return [...g.uploads.values()].map(u => ({ youTubeVideoUpload: u }));
    case 'geo_target_constant': return Object.entries(GEO).map(([id, [name, code]]) => ({ geoTargetConstant: { resourceName: 'geoTargetConstants/' + id, id, name, countryCode: code, targetType: 'Country', status: 'ENABLED' } }));
    case 'product_link': return [{ productLink: { resourceName: `customers/${CID}/productLinks/1`, type: 'MERCHANT_CENTER', merchantCenter: { merchantCenterId: MERCHANT } } }];
    // Each Performance Max or Search campaign inherits the account goals; a campaign-level update overrides biddable.
    case 'campaign_conversion_goal': return live.filter(c => ['PERFORMANCE_MAX', 'SEARCH'].includes(c.advertisingChannelType)).flatMap(c => g.goals.map(x => {
      const rn = `customers/${CID}/campaignConversionGoals/${c.id}~${x.category}~${x.origin}`, u = g.goalUpdates.filter(y => y.resourceName === rn).pop();
      return { campaign: c, campaignConversionGoal: { resourceName: rn, campaign: c.resourceName, category: x.category, origin: x.origin, biddable: u ? u.biddable : x.biddable } }; }));
    case 'shared_set': case 'audience': case 'campaign_shared_set': case 'user_list': return [];
    default: return null;
  }
}
const pathGet = (row, field) => field.split('.').map(camel).reduce((a, k) => (a == null ? undefined : a[k]), row);
function clauseMatch(row, clause) {
  const m = clause.match(/^([a-z0-9_.]+)\s+(NOT IN|IN|IS NOT NULL|IS NULL|NOT LIKE|LIKE|NOT REGEXP_MATCH|REGEXP_MATCH|CONTAINS ANY|CONTAINS ALL|CONTAINS NONE|!=|>=|<=|=|>|<)\s*([\s\S]*)$/i);
  if (!m) throw new Error('unsupported GAQL condition: ' + clause);
  const x = pathGet(row, m[1]), op = m[2].toUpperCase(), raw = m[3].trim();
  const list = () => splitTop(raw.replace(/^\(|\)$/g, ''), /^,/).map(scalar);
  const eq = (a, b) => typeof b === 'boolean' ? (a === true) === b : a !== undefined && a !== null && String(a) === String(b);
  switch (op) {
    case '=': return eq(x, scalar(raw)); case '!=': return !eq(x, scalar(raw));
    case 'IN': return list().some(v => eq(x, v)); case 'NOT IN': return !list().some(v => eq(x, v));
    case 'IS NULL': return x == null; case 'IS NOT NULL': return x != null;
    case 'LIKE': case 'NOT LIKE': { const re = new RegExp('^' + scalar(raw).replace(/[.*+?^${}()|[\]\\]/g, '\\$&').replace(/%/g, '.*') + '$', 'i'); return (op === 'LIKE') === re.test(String(x == null ? '' : x)); }
    case 'REGEXP_MATCH': case 'NOT REGEXP_MATCH': { let p = scalar(raw), flags = ''; if (p.startsWith('(?i)')) { p = p.slice(4); flags = 'i'; } return (op === 'REGEXP_MATCH') === new RegExp(p, flags).test(String(x == null ? '' : x)); }
    case '>': return Number(x) > Number(scalar(raw)); case '<': return Number(x) < Number(scalar(raw)); case '>=': return Number(x) >= Number(scalar(raw)); case '<=': return Number(x) <= Number(scalar(raw));
    default: throw new Error('unsupported GAQL operator ' + op);
  }
}
function runGaql(g, raw) {
  const q = String(raw).replace(/\s+/g, ' ').trim();
  const m = q.match(/^SELECT (.+?) FROM ([a-z_]+)(?: WHERE (.+?))?(?: ORDER BY (.+?))?(?: LIMIT (\d+))?(?: PARAMETERS .*)?$/i);
  if (!m) { g.unknownGaql.add('unparsed: ' + q.slice(0, 160)); return []; }
  const fields = m[1].split(',').map(s => s.trim()), from = m[2].toLowerCase(), where = m[3] ? splitTop(m[3], /^\s+AND\s+/i) : [];
  // The synthetic account has no traffic: any metrics or date segment returns no rows.
  if (/\b(metrics|segments)\./.test(q)) return [];
  let rows = gaqlRows(g, from);
  if (!rows) { g.unknownGaql.add('FROM ' + from); return []; }
  rows = rows.filter(r => where.every(c => clauseMatch(r, c)));
  if (m[5]) rows = rows.slice(0, Number(m[5]));
  // REST omits unset and default values (false booleans included); int64 values are strings.
  return rows.map(r => { const out = {};
    const put = (field, v) => { const keys = field.split('.').map(camel); let at = out; keys.slice(0, -1).forEach(k => { at = at[k] = at[k] || {}; }); at[keys[keys.length - 1]] = clone(v); };
    for (const f of fields) { const v = pathGet(r, f); if (v !== undefined && v !== null && v !== false && v !== '') put(f, v); }
    for (const top of new Set(fields.map(f => f.split('.')[0]))) { const rn = pathGet(r, top + '.resource_name'); if (rn) put(top + '.resource_name', rn); }
    return out; });
}

/* ---------------- atomic googleAds:mutate */
const TEMP = /^-\d+$/;
function parseRn(s) { const m = /^customers\/(\d+)\/([A-Za-z]+)\/(.+)$/.exec(String(s)); return m ? { cid: m[1], coll: m[2], id: m[3], parts: m[3].split('~') } : null; }
function refsIn(v, out = []) { if (typeof v === 'string') { if (parseRn(v)) out.push(v); } else if (Array.isArray(v)) v.forEach(x => refsIn(x, out)); else if (v && typeof v === 'object') Object.values(v).forEach(x => refsIn(x, out)); return out; }
function existsInWorld(g, rn) { const p = parseRn(rn); if (!p) return false;
  switch (p.coll) { case 'campaigns': return g.campaigns.has(rn); case 'campaignBudgets': return g.budgets.has(rn); case 'assets': return g.assets.has(rn); case 'assetGroups': return g.assetGroups.has(rn);
    case 'adGroups': return g.adGroups.has(rn); case 'assetGroupListingGroupFilters': return g.filters.some(f => f.resourceName === rn); case 'youTubeVideoUploads': return g.uploads.has(rn);
    case 'campaignConversionGoals': return g.campaigns.has(RN('campaigns', p.parts[0])); default: return true; } }
const failure = (errors, message = 'Request contains an invalid argument.') => ({ error: { code: 400, message, status: 'INVALID_ARGUMENT', details: [{ '@type': 'type.googleapis.com/google.ads.googleads.v24.errors.GoogleAdsFailure',
  errors: errors.map(e => ({ errorCode: { [e.family || 'requestError']: e.code }, message: e.message, location: { fieldPathElements: [{ fieldName: 'mutate_operations', index: e.i }, ...(e.field ? [{ fieldName: e.field }] : [])] } })),
  requestId: 'synthetic-' + crypto.randomBytes(4).toString('hex') }] } });
async function mutate(g, body, call) {
  const sharp = require('sharp'), ops = body && body.mutateOperations, errors = [], err = (i, code, message, field, family) => errors.push({ i, code, message, field, family });
  const rec = { step: call.step, validateOnly: !!(body && body.validateOnly), at: g.requests.length, ops: [], assetInfo: new Map(), errors, ok: false, created: [], responses: [] };
  g.requests.push(rec); call.mutate = rec;
  if (!Array.isArray(ops) || !ops.length) { err(0, 'REQUIRED', 'mutate_operations is required'); return reply(400, failure(errors)); }
  rec.ops = clone(ops).map(o => { const a = o.assetOperation && o.assetOperation.create; if (a && a.imageAsset && a.imageAsset.data) a.imageAsset = { dataSha: creativeHash(a.imageAsset.data), dataBytes: Buffer.from(a.imageAsset.data, 'base64').length }; return o; });
  // Decode every uploaded image first: shape rules need its pixels.
  for (const o of ops) { const a = o && o.assetOperation && o.assetOperation.create; if (!a) continue;
    const info = { kind: null };
    if (a.textAsset) Object.assign(info, { kind: 'text', text: a.textAsset.text });
    else if (a.imageAsset) { const bytes = Buffer.from(String(a.imageAsset.data || ''), 'base64'); let meta = {}; try { meta = await sharp(bytes).metadata(); } catch (_) {}
      Object.assign(info, { kind: 'image', width: meta.width, height: meta.height, bytes: bytes.length, format: meta.format, contentHash: creativeHash(String(a.imageAsset.data || '')) }); }
    else if (a.youtubeVideoAsset) Object.assign(info, { kind: 'video', videoId: a.youtubeVideoAsset.youtubeVideoId });
    else if (a.callToActionAsset) Object.assign(info, { kind: 'cta', cta: a.callToActionAsset.callToAction });
    else if (a.calloutAsset) Object.assign(info, { kind: 'callout', text: a.calloutAsset.calloutText });
    else if (a.sitelinkAsset) Object.assign(info, { kind: 'sitelink' });
    else if (a.structuredSnippetAsset) Object.assign(info, { kind: 'snippet' });
    if (a.resourceName) rec.assetInfo.set(a.resourceName, info); }
  const worldInfo = rn => { const a = g.assets.get(rn); if (!a) return null; return a._info; };
  const infoOf = rn => rec.assetInfo.get(rn) || worldInfo(rn);
  const created = new Map(), tempIds = new Map(), campaignsIn = new Map(), groupsIn = new Map(), names = new Set(), budgetNames = new Set();
  const campaignOf = rn => campaignsIn.get(rn) || g.campaigns.get(rn) || null;
  const groupOf = rn => groupsIn.get(rn) || g.assetGroups.get(rn) || null;
  const https = u => { try { const x = new URL(u); return x.protocol === 'https:' && !!x.hostname; } catch (_) { return false; } };
  // A URL's domain as the Destination mismatch policy compares a display URL with the final URL: its host, lower-cased, without "www.".
  const domainOf = u => { try { return new URL(/^[a-z][a-z0-9+.-]*:\/\//i.test(String(u)) ? String(u) : 'http://' + u).hostname.toLowerCase().replace(/^www\./, ''); } catch (_) { return null; } };
  const imageOk = (info, spec) => info && info.kind === 'image' && info.width && Math.abs(info.width / info.height - spec[0]) <= spec[0] * 0.01 && info.width >= spec[1] && info.height >= spec[2] && info.bytes <= 5120 * 1024;
  const groupLinks = new Map(), groupFilters = new Map(), adGroupsIn = new Map();
  for (let i = 0; i < ops.length; i++) {
    const o = ops[i] || {}, keys = Object.keys(o);
    if (keys.length !== 1 || !OP_TYPES[keys[0]]) { err(i, 'UNSUPPORTED_OPERATION', 'Unsupported operation ' + keys.join(',')); continue; }
    const type = keys[0], op = o[type], action = op.create ? 'create' : op.update ? 'update' : op.remove ? 'remove' : null;
    if (!action) { err(i, 'REQUIRED', 'operation has no create, update or remove'); continue; }
    const obj = op.create || op.update || {}, own = action === 'create' ? obj.resourceName : action === 'update' ? obj.resourceName : op.remove;
    // References: a temporary name must be created by an EARLIER operation; a real one must exist.
    for (const ref of refsIn(action === 'remove' ? [] : obj)) {
      if (ref === own && action === 'create') continue;
      const p = parseRn(ref); if (p.cid !== CID) { err(i, 'INVALID_CUSTOMER_ID', 'Resource of another customer: ' + ref); continue; }
      if (p.parts.some(x => TEMP.test(x))) {
        const target = p.coll === 'campaignConversionGoals' ? RN('campaigns', p.parts[0]) : ref;
        if (!created.has(target)) err(i, 'INVALID_TEMPORARY_RESOURCE_NAME', 'Temporary resource ' + target + ' is referenced before it is created', null, 'mutateError');
      } else if (!existsInWorld(g, ref) && ref !== own) err(i, 'RESOURCE_NOT_FOUND', 'Resource not found: ' + ref, null, 'mutateError');
    }
    if (action === 'update' && own) { const p = parseRn(own); if (!p) err(i, 'INVALID_RESOURCE_NAME', 'Invalid resource name ' + own); else if (!p.parts.some(x => TEMP.test(x)) && !existsInWorld(g, own)) err(i, 'RESOURCE_NOT_FOUND', 'Resource not found: ' + own, null, 'mutateError'); }
    if (action === 'create' && own) {
      const p = parseRn(own); if (!p || p.coll !== OP_TYPES[type][0]) err(i, 'INVALID_RESOURCE_NAME', 'Resource name ' + own + ' does not match ' + type);
      else { const tid = p.parts[p.parts.length - 1]; if (TEMP.test(tid)) { if (tempIds.has(tid)) err(i, 'DUPLICATE_TEMP_IDS', 'Temporary ID ' + tid + ' is used by ' + tempIds.get(tid) + ' and ' + own, null, 'mutateError'); else tempIds.set(tid, own); } }
      created.set(own, { i, type });
    }
    const c = obj;
    if (type === 'campaignBudgetOperation') {
      const micros = Number(c.amountMicros);
      if (action === 'update' && !/(^|,)\s*amount_micros\s*(,|$)/.test(String(op.updateMask || ''))) err(i, 'FIELD_MASK_MISSING', 'Budget update without amount_micros in update_mask', 'update_mask', 'fieldMaskError');
      if (!Number.isInteger(micros) || micros <= 0 || micros % 10000) err(i, 'MONEY_AMOUNT_LESS_THAN_CURRENCY_MINIMUM_CPC', 'Budget amount ' + c.amountMicros + ' micros is not a positive multiple of the currency unit', 'amount_micros', 'campaignBudgetError');
      if (action === 'create') { if (!c.name || [...g.budgets.values()].some(b => b.name === c.name && b.status !== 'REMOVED') || budgetNames.has(c.name)) err(i, 'DUPLICATE_NAME', 'Budget name missing or already used: ' + c.name, 'name', 'campaignBudgetError'); budgetNames.add(c.name); if (c.explicitlyShared !== false) err(i, 'INVALID_FIELD', 'New campaign budgets must not be shared', 'explicitly_shared'); }
    }
    if (type === 'campaignOperation') {
      if (action === 'create') {
        if (!c.name || len(c.name) > 255) err(i, 'INVALID_NAME', 'Campaign name missing or too long', 'name', 'campaignError');
        if ([...g.campaigns.values()].some(x => x.name === c.name && x.status !== 'REMOVED') || names.has(c.name)) err(i, 'DUPLICATE_CAMPAIGN_NAME', 'A campaign named "' + c.name + '" already exists', 'name', 'campaignError');
        names.add(c.name);
        if (!['PAUSED', 'ENABLED'].includes(c.status)) err(i, 'INVALID_ENUM_VALUE', 'Campaign status ' + c.status, 'status');
        if (!['PERFORMANCE_MAX', 'DISPLAY', 'SEARCH'].includes(c.advertisingChannelType)) err(i, 'INVALID_ENUM_VALUE', 'Channel ' + c.advertisingChannelType, 'advertising_channel_type');
        // v24: every new campaign declares whether it contains EU political advertising; REST rejects unknown (removed) fields such as url_expansion_opt_out.
        if (!['DOES_NOT_CONTAIN_EU_POLITICAL_ADVERTISING', 'CONTAINS_EU_POLITICAL_ADVERTISING'].includes(c.containsEuPoliticalAdvertising)) err(i, 'REQUIRED', 'contains_eu_political_advertising must be declared for a new campaign', 'contains_eu_political_advertising', 'fieldError');
        const unknownFields = Object.keys(c).filter(k => !CAMPAIGN_FIELDS.has(k)); if (unknownFields.length) err(i, 'UNKNOWN_FIELD', 'Invalid JSON payload received. Unknown name ' + unknownFields.join(', ') + ' at campaign', unknownFields[0], 'requestError');
        if (c.advertisingChannelType === 'PERFORMANCE_MAX' && !(c.maximizeConversionValue || c.maximizeConversions)) err(i, 'INVALID_BIDDING_STRATEGY_TYPE', 'Performance Max bids with maximize conversion value or maximize conversions', 'campaign_bidding_strategy', 'biddingError');
        if (!c.campaignBudget) err(i, 'REQUIRED', 'campaign_budget is required', 'campaign_budget');
        if (c.advertisingChannelType === 'PERFORMANCE_MAX') {
          if (!c.shoppingSetting || String(c.shoppingSetting.merchantId) !== MERCHANT) err(i, 'MERCHANT_NOT_LINKED', 'Performance Max retail campaigns must use the linked Merchant Center account', 'shopping_setting.merchant_id', 'shoppingSettingError');
          if (typeof c.brandGuidelinesEnabled !== 'boolean') err(i, 'REQUIRED', 'brand_guidelines_enabled must be set explicitly at creation', 'brand_guidelines_enabled');
        }
        const bidding = ['maximizeConversionValue', 'maximizeConversions', 'manualCpc', 'targetSpend', 'targetRoas', 'targetCpa'].filter(k => c[k] !== undefined);
        if (bidding.length !== 1) err(i, 'BIDDING_STRATEGY_REQUIRED', 'Exactly one bidding strategy is required', 'campaign_bidding_strategy', 'biddingError');
        campaignsIn.set(own, { ...c, _new: true });
      }
      for (const f of ['startDateTime', 'endDateTime']) if (c[f] !== undefined && !/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(String(c[f]))) err(i, 'INVALID_DATE_FORMAT', f + ' ' + c[f], f, 'dateError');
      if (c.endDateTime && String(c.endDateTime).slice(0, 10) < ymd(0)) err(i, 'END_DATE_TIME_IS_IN_THE_PAST', 'end date ' + c.endDateTime, 'end_date_time', 'dateError');
    }
    if (type === 'campaignCriterionOperation' && action === 'create') {
      const kinds = ['location', 'language', 'keyword', 'brandList'].filter(k => c[k]);
      if (kinds.length !== 1) err(i, 'INVALID_CRITERION', 'Exactly one criterion is required');
      if (c.location && !(/^geoTargetConstants\/(\d+)$/.test(c.location.geoTargetConstant) && GEO[c.location.geoTargetConstant.split('/')[1]])) err(i, 'INVALID_GEO_TARGET', 'Unknown location ' + c.location.geoTargetConstant, 'location', 'criterionError');
      if (c.language && c.language.languageConstant !== 'languageConstants/1000') err(i, 'INVALID_LANGUAGE', 'Unknown language', 'language', 'criterionError');
      if (c.keyword && (!c.keyword.text || len(c.keyword.text) > 80 || !['EXACT', 'PHRASE', 'BROAD'].includes(c.keyword.matchType))) err(i, 'INVALID_KEYWORD', 'Invalid keyword ' + JSON.stringify(c.keyword), 'keyword', 'criterionError');
    }
    if (type === 'assetOperation' && action === 'create') {
      const info = rec.assetInfo.get(c.resourceName) || {}, kinds = ['textAsset', 'imageAsset', 'youtubeVideoAsset', 'callToActionAsset', 'calloutAsset', 'sitelinkAsset', 'structuredSnippetAsset'].filter(k => c[k]);
      if (kinds.length !== 1) err(i, 'INVALID_ASSET', 'An asset needs exactly one content type');
      if (info.kind === 'text' && (!String(info.text || '').trim() || len(info.text) > 90)) err(i, 'TEXT_TOO_LONG', 'Text asset "' + info.text + '"', 'text_asset.text', 'assetError');
      if (info.kind === 'image' && (!info.width || info.bytes > 5120 * 1024 || !['jpeg', 'png', 'gif'].includes(info.format))) err(i, 'IMAGE_ERROR', 'Image asset is unreadable, unsupported or over 5120 KB (' + info.bytes + ' bytes)', 'image_asset.data', 'imageError');
      if (info.kind === 'video' && !(/^[A-Za-z0-9_-]{11}$/.test(info.videoId || '') && [...g.uploads.values()].some(u => u.videoId === info.videoId && u.state === 'PROCESSED'))) err(i, 'YOUTUBE_VIDEO_NOT_FOUND', 'YouTube video ' + info.videoId + ' is not available', 'youtube_video_asset', 'assetError');
      if (info.kind === 'cta' && !['SHOP_NOW', 'LEARN_MORE', 'BUY_NOW', 'ORDER_NOW'].includes(info.cta)) err(i, 'INVALID_CALL_TO_ACTION', 'Call to action ' + info.cta, 'call_to_action_asset', 'assetError');
      if (info.kind === 'callout' && (!info.text || len(info.text) > 25)) err(i, 'TOO_LONG', 'Callout "' + info.text + '" exceeds 25 characters', 'callout_asset', 'stringLengthError');
      if (c.sitelinkAsset && (len(c.sitelinkAsset.linkText) > 25 || len(c.sitelinkAsset.description1) > 35 || len(c.sitelinkAsset.description2) > 35 || !(c.finalUrls || []).length || !(c.finalUrls || []).every(https))) err(i, 'INVALID_SITELINK', 'Sitelink text too long or URL invalid', 'sitelink_asset', 'assetError');
      if (c.structuredSnippetAsset) { const s = c.structuredSnippetAsset; if (!['Types', 'Brands', 'Styles', 'Models', 'Collections'].includes(s.header) || !Array.isArray(s.values) || s.values.length < 3 || s.values.length > 10 || s.values.some(v => !v || len(v) > 25)) err(i, 'INVALID_STRUCTURED_SNIPPET', 'Structured snippet header or values invalid', 'structured_snippet_asset', 'assetError'); }
    }
    if (type === 'assetGroupOperation' && action === 'create') {
      const camp = campaignOf(c.campaign);
      if (!camp || camp.advertisingChannelType !== 'PERFORMANCE_MAX') err(i, 'NOT_PERFORMANCE_MAX', 'Asset groups belong to Performance Max campaigns', 'campaign', 'assetGroupError');
      if (!c.name || len(c.name) > 128) err(i, 'INVALID_NAME', 'Asset group name', 'name', 'assetGroupError');
      const sameCampaign = [...g.assetGroups.values(), ...groupsIn.values()].filter(x => x.campaign === c.campaign && x.status !== 'REMOVED');
      if (sameCampaign.some(x => String(x.name).toLowerCase() === String(c.name).toLowerCase())) err(i, 'DUPLICATE_NAME', 'Asset group name "' + c.name + '" is already used in this campaign', 'name', 'assetGroupError');
      if (!Array.isArray(c.finalUrls) || c.finalUrls.length !== 1 || !c.finalUrls.every(https)) err(i, 'INVALID_FINAL_URL', 'Asset group needs one https final URL', 'final_urls', 'assetGroupError');
      groupsIn.set(own, { ...c, _new: true });
    }
    if (type === 'assetGroupAssetOperation' && action === 'create') {
      const grp = groupOf(c.assetGroup), info = infoOf(c.asset), ft = c.fieldType, camp = grp && campaignOf(grp.campaign);
      if (!grp) err(i, 'RESOURCE_NOT_FOUND', 'Asset group ' + c.assetGroup, 'asset_group', 'mutateError');
      if (!info) err(i, 'RESOURCE_NOT_FOUND', 'Asset ' + c.asset, 'asset', 'mutateError');
      else if (TEXT_SPEC[ft]) { if (info.kind !== 'text' || !String(info.text || '').trim() || len(info.text) > TEXT_SPEC[ft]) err(i, 'TOO_LONG', ft + ' "' + info.text + '" exceeds ' + TEXT_SPEC[ft] + ' characters', 'field_type', 'assetLinkError'); }
      else if (IMAGE_SPEC[ft]) { if (!imageOk(info, IMAGE_SPEC[ft])) err(i, 'ASPECT_RATIO_NOT_ALLOWED', ft + ' image ' + info.width + 'x' + info.height + ' does not meet ' + IMAGE_SPEC[ft].join('/'), 'field_type', 'mediaUploadError'); }
      else if (ft === 'YOUTUBE_VIDEO') { if (info.kind !== 'video') err(i, 'INVALID_ASSET_TYPE', 'YOUTUBE_VIDEO needs a YouTube asset', 'field_type', 'assetLinkError'); }
      else if (ft === 'CALL_TO_ACTION_SELECTION') { if (info.kind !== 'cta') err(i, 'INVALID_ASSET_TYPE', 'CALL_TO_ACTION_SELECTION needs a call-to-action asset', 'field_type', 'assetLinkError'); }
      else err(i, 'INVALID_FIELD_TYPE', 'Unsupported asset group field ' + ft, 'field_type', 'assetLinkError');
      if (camp && camp.brandGuidelinesEnabled === true && ['BUSINESS_NAME', 'LOGO', 'LANDSCAPE_LOGO'].includes(ft)) err(i, 'BRAND_ASSETS_NOT_ALLOWED_AT_ASSET_GROUP', ft + ' belongs to the campaign when brand guidelines are enabled', 'field_type', 'assetGroupAssetError');
      const list = groupLinks.get(c.assetGroup) || []; if (list.some(l => l.asset === c.asset && l.fieldType === ft)) err(i, 'DUPLICATE_RESOURCE', 'Duplicate asset link', null, 'mutateError'); list.push({ asset: c.asset, fieldType: ft, info, i }); groupLinks.set(c.assetGroup, list);
    }
    if (type === 'assetGroupListingGroupFilterOperation' && action === 'create') {
      const grp = groupOf(c.assetGroup), p = parseRn(own || '');
      if (!grp) err(i, 'RESOURCE_NOT_FOUND', 'Asset group ' + c.assetGroup, 'asset_group', 'mutateError');
      if (!p || p.parts.length !== 2 || RN('assetGroups', p.parts[0]) !== c.assetGroup) err(i, 'INVALID_RESOURCE_NAME', 'Listing group filter name must start with its asset group ID', 'resource_name');
      if (!['SUBDIVISION', 'UNIT_INCLUDED', 'UNIT_EXCLUDED'].includes(c.type) || c.listingSource !== 'SHOPPING') err(i, 'INVALID_LISTING_GROUP', 'type ' + c.type + ' source ' + c.listingSource, 'type', 'assetGroupListingGroupFilterError');
      const list = groupFilters.get(c.assetGroup) || []; list.push({ ...c, resourceName: own, i }); groupFilters.set(c.assetGroup, list);
    }
    if (type === 'assetGroupSignalOperation' && action === 'create') {
      if (!groupOf(c.assetGroup)) err(i, 'RESOURCE_NOT_FOUND', 'Asset group ' + c.assetGroup, 'asset_group', 'mutateError');
      if (c.searchTheme && (!c.searchTheme.text || len(c.searchTheme.text) > 80)) err(i, 'TOO_LONG', 'Search theme "' + c.searchTheme.text + '"', 'search_theme', 'stringLengthError');
    }
    if (type === 'adGroupOperation' && action === 'create') {
      const camp = campaignOf(c.campaign); if (!camp || camp.advertisingChannelType !== 'DISPLAY' || c.type !== 'DISPLAY_STANDARD') err(i, 'INVALID_AD_GROUP_TYPE', 'Display ad groups need a Display campaign and DISPLAY_STANDARD', 'type', 'adGroupError');
      adGroupsIn.set(own, c);
    }
    if (type === 'adGroupAdOperation' && action === 'create') {
      const ad = c.ad || {}, urls = ad.finalUrls || [];
      if (!(adGroupsIn.has(c.adGroup) || g.adGroups.has(c.adGroup))) err(i, 'RESOURCE_NOT_FOUND', 'Ad group ' + c.adGroup, 'ad_group', 'mutateError');
      if (!urls.length || !urls.every(https)) err(i, 'INVALID_FINAL_URL', 'Ads need an https final URL', 'ad.final_urls', 'adError');
      if (ad.responsiveDisplayAd) {
        const r = ad.responsiveDisplayAd, texts = (list, min, max, chars, field) => { if (!Array.isArray(list) || list.length < min || list.length > max || list.some(t => !t || !String(t.text || '').trim() || len(t.text) > chars)) err(i, 'TOO_LONG', field + ' must have ' + min + '-' + max + ' items of at most ' + chars + ' characters', 'ad.responsive_display_ad.' + field, 'adError'); };
        texts(r.headlines, 1, 5, 30, 'headlines'); texts(r.descriptions, 1, 5, 90, 'descriptions'); texts(r.longHeadline ? [r.longHeadline] : [], 1, 1, 90, 'long_headline');
        if (!r.businessName || len(r.businessName) > 25) err(i, 'TOO_LONG', 'business_name', 'ad.responsive_display_ad.business_name', 'adError');
        const images = (list, min, max, spec, field) => { if (!Array.isArray(list || []) || (list || []).length < min || (list || []).length > max || (list || []).some(x => !imageOk(infoOf(x.asset), spec))) err(i, 'ASPECT_RATIO_NOT_ALLOWED', field + ' must have ' + min + '-' + max + ' images of ' + spec.join('/'), 'ad.responsive_display_ad.' + field, 'mediaUploadError'); };
        images(r.marketingImages, 1, 15, IMAGE_SPEC.MARKETING_IMAGE, 'marketing_images'); images(r.squareMarketingImages, 1, 15, IMAGE_SPEC.SQUARE_MARKETING_IMAGE, 'square_marketing_images');
        images(r.squareLogoImages, 0, 5, IMAGE_SPEC.LOGO, 'square_logo_images'); images(r.logoImages, 0, 5, IMAGE_SPEC.LANDSCAPE_LOGO, 'logo_images');
        // Marketing images (landscape + square) share one limit of 15, logos (square + landscape) one of 5.
        if ((r.marketingImages || []).length + (r.squareMarketingImages || []).length > 15 || (r.squareLogoImages || []).length + (r.logoImages || []).length > 5) err(i, 'TOO_MANY_IMAGES', 'Too many marketing images or logos', 'ad.responsive_display_ad', 'adError');
        if ((r.youtubeVideos || []).length > 5 || (r.youtubeVideos || []).some(v => (infoOf(v.asset) || {}).kind !== 'video')) err(i, 'INVALID_VIDEO', 'youtube_videos', 'ad.responsive_display_ad.youtube_videos', 'adError');
        if (ad.name !== undefined) err(i, 'FIELD_NOT_SUPPORTED', 'Ad.name is not supported for responsive display ads', 'ad.name', 'adError');
      } else if (ad.imageAd) {
        const info = infoOf(ad.imageAd.imageAsset && ad.imageAd.imageAsset.asset);
        if (!info || info.kind !== 'image' || !FIXED_SIZES.has(info.width + 'x' + info.height)) err(i, 'IMAGE_SIZE_NOT_SUPPORTED', 'Image ad size ' + (info ? info.width + 'x' + info.height : 'unknown') + ' is not a supported Display size', 'ad.image_ad', 'imageError');
        else if (info.bytes > 150 * 1024) err(i, 'FILE_TOO_LARGE', 'Image ads are limited to 150 KB', 'ad.image_ad', 'imageError');
        if (!ad.name) err(i, 'REQUIRED', 'Image ads need a name', 'ad.name', 'adError');
        // v24 requires Ad.display_url on image ads (FieldError REQUIRED, as Google answered the first Fixed Display plan), and the
        // Destination mismatch policy refuses a display URL whose domain is not the final URL's.
        if (!ad.displayUrl) err(i, 'REQUIRED', 'The required field was not present.', 'ad_group_ad_operation.create.ad.display_url', 'fieldError');
        else if (urls.some(u => domainOf(u) !== domainOf(ad.displayUrl))) err(i, 'POLICY_FINDING', 'Destination mismatch: display URL ' + ad.displayUrl + ' is not on the final URL\'s domain', 'ad_group_ad_operation.create.ad.display_url', 'policyFindingError');
      } else err(i, 'INVALID_AD_TYPE', 'Only responsive display and image ads are expected here', 'ad', 'adError');
    }
    if (type === 'campaignConversionGoalOperation') {
      const p = parseRn(own || ''), goal = p && g.goals.find(x => x.category === p.parts[1] && x.origin === p.parts[2]);
      if (action !== 'update' || !p || p.parts.length !== 3 || !goal || op.updateMask !== 'biddable' || typeof c.biddable !== 'boolean') err(i, 'INVALID_CONVERSION_GOAL', 'Invalid campaign conversion goal update ' + own, 'resource_name', 'conversionGoalError');
      const camp = p && campaignOf(RN('campaigns', p.parts[0])); if (camp && !['PERFORMANCE_MAX', 'SEARCH'].includes(camp.advertisingChannelType)) err(i, 'INVALID_CAMPAIGN', 'Goals of a ' + camp.advertisingChannelType + ' campaign', 'resource_name', 'conversionGoalError');
    }
    if (type === 'campaignAssetOperation' && action === 'create' && (!campaignOf(c.campaign) || !infoOf(c.asset))) err(i, 'RESOURCE_NOT_FOUND', 'Campaign asset link', null, 'mutateError');
  }
  // Composite rules per asset group: every new group must be complete; existing groups stay within limits.
  for (const [rn, grp] of [...groupsIn, ...[...groupLinks.keys()].filter(k => !groupsIn.has(k)).map(k => [k, g.assetGroups.get(k)])]) {
    if (!grp) continue;
    const camp = campaignOf(grp.campaign) || {}, brand = camp.brandGuidelinesEnabled === true, i = (groupLinks.get(rn) || [{ i: 0 }])[0].i;
    const existing = g.links.filter(l => l.assetGroup === rn && l.status !== 'REMOVED').map(l => ({ fieldType: l.fieldType, info: (g.assets.get(l.asset) || {})._info }));
    const all = [...existing, ...(groupLinks.get(rn) || [])], n = ft => all.filter(l => l.fieldType === ft).length, texts = ft => all.filter(l => l.fieldType === ft).map(l => (l.info || {}).text || '');
    const limits = { HEADLINE: [3, 15], LONG_HEADLINE: [1, 5], DESCRIPTION: [2, 5], BUSINESS_NAME: brand ? [0, 0] : [1, 1], LOGO: brand ? [0, 0] : [1, 5], LANDSCAPE_LOGO: [0, brand ? 0 : 5], MARKETING_IMAGE: [1, 20], SQUARE_MARKETING_IMAGE: [1, 20], PORTRAIT_MARKETING_IMAGE: [0, 20], YOUTUBE_VIDEO: [0, 15], CALL_TO_ACTION_SELECTION: [0, 1] };
    for (const [ft, [min, max]] of Object.entries(limits)) if ((grp._new && n(ft) < min) || n(ft) > max) err(i, grp._new && n(ft) < min ? 'NOT_ENOUGH_' + ft + '_ASSET' : 'TOO_MANY_' + ft + '_ASSETS', 'Asset group ' + grp.name + ' has ' + n(ft) + ' ' + ft + ' (allowed ' + min + '-' + max + ')', 'asset_group', 'assetGroupError');
    if (grp._new && !texts('HEADLINE').some(t => len(t) <= 15)) err(i, 'SHORT_HEADLINE_REQUIRED', 'Asset group ' + grp.name + ' needs a headline of 15 characters or fewer', 'asset_group', 'assetGroupError');
    if (grp._new && !texts('DESCRIPTION').some(t => len(t) <= 60)) err(i, 'SHORT_DESCRIPTION_REQUIRED', 'Asset group ' + grp.name + ' needs a description of 60 characters or fewer', 'asset_group', 'assetGroupError');
    if (grp._new && new Set(texts('HEADLINE').map(t => t.toLowerCase())).size !== n('HEADLINE')) err(i, 'DUPLICATE_ASSETS_WITH_DIFFERENT_FIELD_VALUE', 'Duplicate headlines in ' + grp.name, 'asset_group', 'assetGroupError');
    // Google deduplicates identical text assets, so a repeated long headline or description links one asset twice.
    for (const ft of ['LONG_HEADLINE', 'DESCRIPTION']) if (grp._new && new Set(texts(ft)).size !== n(ft)) err(i, 'DUPLICATE_RESOURCE', 'Duplicate ' + ft + ' text in ' + grp.name, 'asset_group', 'assetGroupAssetError');
  }
  // Listing-group trees: one SUBDIVISION root, unit children of one dimension, one "everything else" node.
  for (const [rn, list] of groupFilters) {
    const i = list[0].i, roots = list.filter(f => !f.parentListingGroupFilter), existing = g.filters.filter(f => f.assetGroup === rn);
    if (existing.length) { err(i, 'LISTING_GROUP_ALREADY_EXISTS', 'Asset group already has a product tree', 'asset_group', 'assetGroupListingGroupFilterError'); continue; }
    if (roots.length !== 1) { err(i, 'MULTIPLE_ROOTS', 'Exactly one root listing group is required', 'parent_listing_group_filter', 'assetGroupListingGroupFilterError'); continue; }
    const root = roots[0], children = list.filter(f => f.parentListingGroupFilter);
    if (root.type === 'SUBDIVISION') {
      if (children.some(f => f.parentListingGroupFilter !== root.resourceName)) err(i, 'INVALID_PARENT', 'Children must reference the root', 'parent_listing_group_filter', 'assetGroupListingGroupFilterError');
      if (children.some(f => f.type === 'SUBDIVISION' || !f.caseValue || !f.caseValue.productItemId)) err(i, 'INVALID_DIMENSION', 'Children must be units of the product item ID dimension', 'case_value', 'assetGroupListingGroupFilterError');
      const others = children.filter(f => f.caseValue && f.caseValue.productItemId && !f.caseValue.productItemId.value), values = children.map(f => f.caseValue && f.caseValue.productItemId && f.caseValue.productItemId.value).filter(Boolean);
      if (others.length !== 1) err(i, 'SUBDIVISION_REQUIRES_OTHERS_CASE', 'A subdivision needs exactly one "everything else" node', 'case_value', 'assetGroupListingGroupFilterError');
      if (new Set(values.map(v => v.toLowerCase())).size !== values.length) err(i, 'DUPLICATE_CASE_VALUE', 'Duplicate product item IDs', 'case_value', 'assetGroupListingGroupFilterError');
    } else if (root.type !== 'UNIT_INCLUDED' || children.length) err(i, 'INVALID_ROOT', 'A single root must be UNIT_INCLUDED', 'type', 'assetGroupListingGroupFilterError');
  }
  for (const [rn, camp] of campaignsIn) if (camp.advertisingChannelType === 'PERFORMANCE_MAX' && ![...groupsIn.values()].some(x => x.campaign === rn)) err(created.get(rn).i, 'MISSING_ASSET_GROUP', 'A new Performance Max campaign needs an asset group in the same request', 'campaign', 'campaignError');
  if (errors.length) { if (DEBUG) console.log('DEBUG rejected mutate', call.step, JSON.stringify(errors).slice(0, 2000)); return reply(400, failure(errors)); }
  if (rec.validateOnly) { rec.ok = true; return reply(200, {}); }
  const fault = g.faults.find(f => !f.used && f.when(rec)); if (fault) { fault.used = true; rec.fault = fault.kind; }
  if (fault && fault.kind === 'definite') { rec.errors.push({ i: 0, code: 'RESOURCE_TEMPORARILY_EXHAUSTED', message: 'Synthetic definite rejection' }); return reply(400, failure([{ i: 0, code: 'CONCURRENT_MODIFICATION', message: 'Synthetic definite rejection', family: 'databaseError' }])); }
  if (fault && fault.kind === 'partial') { rec.ok = false; return reply(200, { partialFailureError: { code: 3, message: 'Synthetic partial failure' }, mutateOperationResponses: [] }); }
  // Apply atomically: temporary names become real ones in every later reference.
  const idMap = new Map(), real = v => typeof v === 'string' ? (idMap.get(v) || v.replace(/^(customers\/\d+\/campaignConversionGoals\/)(-\d+)(~.*)$/, (m, a, b, c2) => { const r = idMap.get(RN('campaigns', b)); return r ? a + r.split('/').pop() + c2 : m; }))
    : Array.isArray(v) ? v.map(real) : v && typeof v === 'object' ? Object.fromEntries(Object.entries(v).map(([k, x]) => [k, real(x)])) : v;
  const idOf = rn => String(rn).split('/').pop();
  for (const o of ops) {
    const type = Object.keys(o)[0], op = o[type], coll = OP_TYPES[type][0], result = OP_TYPES[type][1];
    if (op.update) {
      const u = real(op.update), rn = u.resourceName;
      if (type === 'campaignBudgetOperation') Object.assign(g.budgets.get(rn), { amountMicros: String(u.amountMicros) });
      else if (type === 'campaignOperation') Object.assign(g.campaigns.get(rn), u);
      else if (type === 'campaignConversionGoalOperation') g.goalUpdates.push({ ...u, updateMask: op.updateMask });
      rec.responses.push({ [result]: { resourceName: rn } }); continue;
    }
    const c = real(op.create), temp = op.create.resourceName; let rn;
    const n = g.id();
    if (type === 'assetGroupListingGroupFilterOperation') rn = RN(coll, idOf(c.assetGroup) + '~' + n);
    else if (type === 'assetGroupAssetOperation') rn = RN(coll, idOf(c.assetGroup) + '~' + idOf(c.asset) + '~' + c.fieldType);
    else if (type === 'assetGroupSignalOperation') rn = RN(coll, idOf(c.assetGroup) + '~' + n);
    else if (type === 'campaignCriterionOperation') rn = RN(coll, idOf(c.campaign) + '~' + n);
    else if (type === 'campaignAssetOperation') rn = RN(coll, idOf(c.campaign) + '~' + idOf(c.asset) + '~' + c.fieldType);
    else if (type === 'adGroupAdOperation') rn = RN(coll, idOf(c.adGroup) + '~' + n);
    else rn = RN(coll, n);
    if (temp) idMap.set(temp, rn);
    const obj = { ...c, resourceName: rn, id: n };
    if (type === 'campaignBudgetOperation') g.budgets.set(rn, { ...obj, amountMicros: String(c.amountMicros), status: 'ENABLED', period: 'DAILY' });
    if (type === 'campaignOperation') g.campaigns.set(rn, { ...obj, servingStatus: 'SERVING', primaryStatus: c.status === 'ENABLED' ? 'ELIGIBLE' : 'PAUSED', biddingStrategyType: c.maximizeConversionValue ? 'MAXIMIZE_CONVERSION_VALUE' : c.maximizeConversions ? 'MAXIMIZE_CONVERSIONS' : 'MANUAL_CPC', ...(c.shoppingSetting ? { shoppingSetting: { ...c.shoppingSetting, merchantId: String(c.shoppingSetting.merchantId) } } : {}) });
    if (type === 'assetOperation') { const info = rec.assetInfo.get(temp) || {}, a = { ...obj, _info: info, type: { text: 'TEXT', image: 'IMAGE', video: 'YOUTUBE_VIDEO', cta: 'CALL_TO_ACTION', callout: 'CALLOUT', sitelink: 'SITELINK', snippet: 'STRUCTURED_SNIPPET' }[info.kind] };
      if (info.kind === 'image') a.imageAsset = { mimeType: 'IMAGE_JPEG', fileSize: String(info.bytes), fullSize: { widthPixels: String(info.width), heightPixels: String(info.height), url: 'https://tpc.googlesyndication.com/simgad/' + n } };
      g.assets.set(rn, a); }
    if (type === 'assetGroupOperation') g.assetGroups.set(rn, { ...obj, primaryStatus: 'PAUSED' });
    if (type === 'assetGroupAssetOperation') g.links.push({ ...obj, status: 'ENABLED', primaryStatus: 'PENDING', policySummary: { approvalStatus: 'UNDER_REVIEW', reviewStatus: 'REVIEW_IN_PROGRESS' } });
    if (type === 'assetGroupListingGroupFilterOperation') g.filters.push(obj);
    if (type === 'assetGroupSignalOperation') g.signals.push(obj);
    if (type === 'campaignCriterionOperation') g.criteria.push({ ...obj, criterionId: n, status: 'ENABLED', type: c.location ? 'LOCATION' : c.language ? 'LANGUAGE' : c.keyword ? 'KEYWORD' : 'BRAND_LIST' });
    if (type === 'campaignAssetOperation') g.campaignAssets.push({ ...obj, status: 'ENABLED' });
    if (type === 'adGroupOperation') g.adGroups.set(rn, obj);
    if (type === 'adGroupAdOperation') g.ads.push({ ...obj, ad: { ...c.ad, id: n, resourceName: RN('ads', n) } });
    rec.created.push(rn); rec.responses.push({ [result]: { resourceName: rn } });
  }
  rec.ok = true; rec.idMap = idMap;
  if (fault && fault.kind === 'lost') throw Object.assign(new Error('socket hang up'), { code: 'ECONNRESET', type: 'system' });
  return reply(200, { mutateOperationResponses: rec.responses });
}

/* ---------------- resumable YouTube uploads */
const UPLOAD_START = `https://googleads.googleapis.com/resumable/upload/v24/customers/${CID}/youTubeVideoUploads:create`;
function upload(g, url, method, headers, body, call) {
  if (url === UPLOAD_START) {
    if (method !== 'POST' || headers['x-goog-upload-command'] !== 'start' || headers['x-goog-upload-protocol'] !== 'resumable') return reply(400, { error: { message: 'Invalid resumable start' } });
    const sid = String(g.sessions.size + 1), meta = (body && body.you_tube_video_upload) || {};
    g.sessions.set(sid, { sid, total: Number(headers['x-goog-upload-header-content-length']), received: 0, title: meta.video_title, privacy: meta.video_privacy, step: call.step, bytes: null, resourceName: null });
    call.upload = 'start';
    return reply(200, '', { 'x-goog-upload-url': 'https://googleads.googleapis.com/resumable/upload/session/' + sid, 'x-goog-upload-status': 'active' });
  }
  const m = url.match(/^https:\/\/googleads\.googleapis\.com\/resumable\/upload\/session\/(\d+)$/), s = m && g.sessions.get(m[1]);
  if (!s) return reply(404, { error: { message: 'No such upload session' } });
  if (headers['x-goog-upload-command'] === 'query') { call.upload = 'query'; return reply(200, s.resourceName ? { resourceName: s.resourceName } : '', { 'x-goog-upload-size-received': String(s.received), 'x-goog-upload-status': s.resourceName ? 'final' : 'active' }); }
  if (method === 'PUT' && /finalize/.test(headers['x-goog-upload-command'] || '')) {
    const bytes = Buffer.isBuffer(body) ? body : Buffer.from(body || '');
    if (Number(headers['x-goog-upload-offset']) !== s.received || s.received + bytes.length !== s.total) return reply(400, { error: { message: 'Upload offset or length mismatch' } });
    s.received += bytes.length; s.bytes = bytes; call.upload = 'finalize';
    const n = g.id(), resourceName = `customers/${CID}/youTubeVideoUploads/${n}`, videoId = crypto.createHash('sha256').update(bytes).digest('base64url').replace(/[^A-Za-z0-9_-]/g, '').slice(0, 11);
    s.resourceName = resourceName; g.uploads.set(resourceName, { resourceName, videoId, state: 'PROCESSED', videoPrivacy: s.privacy || 'UNSPECIFIED', videoTitle: s.title, _sha256: sha256(bytes), _sid: s.sid });
    return reply(200, { resourceName });
  }
  return reply(400, { error: { message: 'Unsupported upload command' } });
}

/* ================================================================ fake fetch: the only door to the outside */
let ctx = null; // the current scenario
const reply = (status, body, headers = {}) => { const h = Object.fromEntries(Object.entries(headers).map(([k, v]) => [k.toLowerCase(), v]));
  return { ok: status < 400, status, statusText: String(status), headers: { get: k => h[String(k).toLowerCase()] ?? null, raw: () => h, has: k => String(k).toLowerCase() in h },
    json: async () => (typeof body === 'string' ? JSON.parse(body) : body), text: async () => (typeof body === 'string' ? body : JSON.stringify(body)),
    buffer: async () => Buffer.from(typeof body === 'string' ? body : JSON.stringify(body)), arrayBuffer: async () => Buffer.from(typeof body === 'string' ? body : JSON.stringify(body)) }; };
async function fakeFetch(url, opts = {}) {
  url = String(url); const method = String(opts.method || 'GET').toUpperCase(), headers = Object.fromEntries(Object.entries(opts.headers || {}).map(([k, v]) => [k.toLowerCase(), String(v)]));
  let body = null; const raw = opts.body;
  if (Buffer.isBuffer(raw) || raw instanceof Uint8Array) body = Buffer.from(raw); else if (raw != null) { const s = String(raw); try { body = JSON.parse(s); } catch (_) { body = s; } }
  const call = { step: ctx.step, url, method, at: ctx.calls.length }; ctx.calls.push(call);
  if (url === SITE + '/.netlify/functions/googleAdsAutopilot-background') {
    // Netlify acknowledges a background function with 202 at once and runs it afterwards.
    // Approve ad's own publication (publishSubmission) keeps the Approve step, so its Google requests and the film upload it
    // queues read as that step's, as they did when the request published by itself; call.started tells them apart.
    const task = ((body && body.tasks) || []).join(','), base = ctx.step; call.task = task;
    if (ctx.dispatchFail === task) { call.refused = true; return reply(500, 'Internal Error'); }
    if (ctx.beforeWorker) await ctx.beforeWorker(body);
    call.done = new Promise(done => setImmediate(done)).then(() => { call.started = true; ctx.step = task === 'publishSubmission' ? base : base + ' · ' + task; return ctx.worker.handler({ httpMethod: 'POST', headers: {}, body: String(raw) }); })
      .then(res => { call.worker = JSON.parse(res.body || '{}'); call.workerStatus = res.statusCode; }).catch(e => { call.workerError = e.message; });
    ctx.background.push(call.done);
    return reply(202, '');
  }
  if (url === 'https://oauth2.googleapis.com/token') return reply(200, { access_token: 'synthetic-token', expires_in: 3600, token_type: 'Bearer' });
  if (/^https:\/\/googleads\.googleapis\.com\/resumable\//.test(url)) return upload(ctx.google, url, method, headers, body, call);
  const ads = url.match(/^https:\/\/googleads\.googleapis\.com\/(v\d+)\/customers\/(\d+)\/googleAds:(search|mutate)$/);
  if (ads) {
    if (ads[1] !== 'v24' || ads[2] !== CID) return reply(400, { error: { message: 'Unexpected Google Ads version or customer ' + ads[1] + '/' + ads[2] } });
    if (ads[3] === 'search') { call.gaql = body && body.query; if (ctx.google.searchFault && ctx.google.searchFault.test(call.gaql || '')) return reply(400, { error: { code: 400, status: 'INVALID_ARGUMENT', message: 'Synthetic read failure' } });
      return reply(200, { results: runGaql(ctx.google, body && body.query) }); }
    return mutate(ctx.google, body, call);
  }
  if (/^https:\/\/(api\.openai\.com|api\.anthropic\.com|generativelanguage\.googleapis\.com|[a-z0-9-]+-aiplatform\.googleapis\.com)\//.test(url)) { call.violation = 'paid-ai'; return reply(500, { error: { message: 'complete-ad-e2e: paid AI requests are forbidden' } }); }
  call.unhandled = true; return reply(404, { error: { message: 'complete-ad-e2e: no synthetic endpoint for ' + url.split('?')[0] } });
}

/* ================================================================ load the real modules with the fakes */
const adminFacade = { apps: [{}], firestore: Object.assign(() => ctx.store.db, { FieldValue, Timestamp }), storage: () => ({ bucket: () => ctx.store.bucket }), credential: { cert: () => ({}) } };
const realResolve = Module._resolveFilename;
Module._resolveFilename = function (request, parent, ...rest) {
  if (request === 'node-fetch') return 'SYNTHETIC:node-fetch';
  if (/(^|\/)firebaseAdmin(\.js)?$/.test(request)) return 'SYNTHETIC:firebaseAdmin';
  return realResolve.call(this, request, parent, ...rest);
};
require.cache['SYNTHETIC:node-fetch'] = { id: 'SYNTHETIC:node-fetch', filename: 'SYNTHETIC:node-fetch', loaded: true, exports: fakeFetch };
require.cache['SYNTHETIC:firebaseAdmin'] = { id: 'SYNTHETIC:firebaseAdmin', filename: 'SYNTHETIC:firebaseAdmin', loaded: true, exports: adminFacade };
// A fresh boot per scenario: new module state (caches, leases, memoised tokens), store and Google account.
async function boot(name, control = {}) {
  scenarioName = name;
  for (const k of Object.keys(require.cache)) if (k.startsWith(FN + path.sep) || (k.startsWith(ROOT + path.sep) && /[\\/]brites-[^\\/]+\.js$/.test(k) && !k.includes('node_modules'))) delete require.cache[k];
  ctx = { name, step: 'setup', calls: [], background: [], responses: [], store: createStore(), google: createGoogle(), beforeWorker: null };
  seedGoogle(ctx.google);
  ctx.store.docs.set('Brites_GAds_Control/control', { enabled: false, dryRun: false, maxDailyBudgetTotal: 100, ...control });
  ctx.kick = require(path.join(FN, 'googleAdsAutopilotKick.js'));
  ctx.worker = require(path.join(FN, 'googleAdsAutopilot-background.js'));
  ctx.E = require(path.join(FN, 'googleAdsAutopilot.js'));
  return ctx;
}
async function api(action, data = {}) {
  const res = await ctx.kick.httpHandler({ httpMethod: 'POST', headers: { 'x-edit-passcode': PASS }, body: JSON.stringify({ action, ...data }) });
  while (ctx.background.length) await ctx.background.shift(); // dispatched worker tasks finish before the next console request
  ctx.responses.push(String(res.body || ''));
  let json = {}; try { json = JSON.parse(res.body || '{}'); } catch (_) { json = { error: 'unparseable response ' + String(res.body).slice(0, 200) }; }
  return { http: res.statusCode, ...json };
}
// Approve ad, as its card drives it: the request approves the reviewed plan and hands the publication to the background worker
// (publishSubmission); the worker publishes and saves the outcome on the approval; the card reads it with approvalStatus.
// Returns the answer the request gave before publication moved to the worker: the worker's result with the saved outcome, or
// {ok:false,error} with the reason the card shows. meta: the request's own answer, the googleAds:mutate calls made before it
// answered, and the worker run.
async function approve(ap, planHash = ap && ap.planHash) {
  const from = ctx.google.requests.length, firstCall = ctx.calls.length;
  const res = await ctx.kick.httpHandler({ httpMethod: 'POST', headers: { 'x-edit-passcode': PASS }, body: JSON.stringify({ action: 'publishAdDesignSubmission', id: ap.id, hash: ap.reviewHash, planHash, confirmed: true }) });
  const job = ctx.calls.slice(firstCall).find(c => c.task === 'publishSubmission'), meta = { sentDuringRequest: ctx.google.requests.slice(from).length, workerStartedBeforeAnswer: !!(job && job.started), job, outcomeAtAnswer: approval(ap.id) ? approval(ap.id).publishOutcome : undefined };
  if (job && job.done) await job.done;
  while (ctx.background.length) await ctx.background.shift();
  ctx.responses.push(String(res.body || ''));
  let request = {}; try { request = JSON.parse(res.body || '{}'); } catch (_) { request = { error: 'unparseable response ' + String(res.body).slice(0, 200) }; }
  meta.request = request;
  if (!request.queued) return { http: res.statusCode, ...request, meta };
  const card = await api('approvalStatus', { id: ap.id }), o = card.publishOutcome && card.publishOutcome.at >= request.requestedAt ? card.publishOutcome : null;
  const worker = job && job.worker && job.worker.result && job.worker.result.publishSubmission || null; meta.card = card; meta.worker = worker;
  if (o && (o.status === 'APPLIED' || o.status === 'VALIDATED')) return { ...(worker || {}), ok: true, status: o.status, message: o.message, meta };
  if (card.status === 'APPLY_UNKNOWN') return { ok: false, error: o && o.status === 'APPLY_UNKNOWN' ? o.message : card.error, meta };
  if (card.status === 'PENDING' && card.error && card.lastErrorAt >= request.requestedAt) return { ok: false, error: card.error, meta };
  return { ok: false, error: 'no outcome yet: ' + JSON.stringify({ status: card.status, error: card.error, lastErrorAt: card.lastErrorAt, publishOutcome: card.publishOutcome }), meta };
}
// The approval request itself sent nothing to Google and answered before the worker started; the worker then ran.
const queuedOnly = r => !!(r && r.meta && r.meta.request.ok === true && r.meta.request.queued === true && r.meta.sentDuringRequest === 0 && !r.meta.workerStartedBeforeAnswer && r.meta.job && r.meta.job.workerStatus === 200);
const queuedDetail = r => r && r.meta ? JSON.stringify({ request: r.meta.request, sentDuringRequest: r.meta.sentDuringRequest, workerStartedBeforeAnswer: r.meta.workerStartedBeforeAnswer, worker: r.meta.job && (r.meta.job.workerStatus || r.meta.job.workerError) }).slice(0, 500) : why(r);
// Lets applyApproval's lease wait (5-second steps, up to 8 minutes) pass at once: the clock moves forward by each wait.
let clockSkew = 0; const realNow = Date.now; Date.now = () => realNow() + clockSkew;
async function fastWaits(fn) { const wait = global.setTimeout; global.setTimeout = (cb, ms, ...a) => { if (ms >= 5000) { clockSkew += ms; ms = 0; } return wait(cb, ms, ...a); }; try { return await fn(); } finally { global.setTimeout = wait; } }
const step = s => { ctx.step = s; };
const reqsIn = s => ctx.google.requests.filter(r => r.step === s);
const callsIn = s => ctx.calls.filter(c => c.step === s);
const approval = id => ctx.store.docs.get('Brites_GAds_Approvals/' + id);
const wsPath = id => 'Brites_GAds_State/adDesign/workspaces/' + id;
const counts = () => { const g = ctx.google; return JSON.stringify([g.campaigns.size, g.budgets.size, g.assets.size, g.assetGroups.size, g.links.length, g.filters.length, g.criteria.length, g.adGroups.size, g.ads.length, g.uploads.size, g.goalUpdates.length]); };

/* ================================================================ fixtures */
const GROUP = { ref: 'opportunity:animal-charms', channel: 'pmax', name: 'Animal charms', url: 'https://britesjewelry.com/collections/animal-charms', productIds: ['8101', '8102', '8103', '8104'] };
const product = (key, short, id, title, handle, variants, color) => ({ key, short, id, gid: 'gid://shopify/Product/' + id, title, handle, url: 'https://britesjewelry.com/products/' + handle, offers: variants.map(v => `shopify_us_${id}_${v}`), color });
const PRODUCTS = [
  product('duck', 'Duck', '8101', 'Duck Silhouette Charm Necklace', 'duck-silhouette-charm-necklace', ['44001', '44002'], '#f2c94c'),
  product('gecko', 'Gecko', '8102', 'Gecko Necklace', 'gecko-necklace', ['44011'], '#6fcf97'),
  product('bunny', 'Bunny', '8103', 'Bunny Pendant Necklace', 'bunny-pendant-necklace', ['44021', '44022'], '#f5b7c8'),
  product('saturn', 'Saturn', '8104', 'Planet Saturn Pendant Necklace', 'planet-saturn-pendant-necklace', ['44031'], '#9b8cf2')];
const P = Object.fromEntries(PRODUCTS.map(p => [p.key, p]));
const copyFor = (noun, title) => ({
  headlines: [`${noun} Necklace`, `${noun} Charm Necklace`, `Handcrafted ${noun} Pendant`, `Gold Filled ${noun} Charm`, `Sterling Silver ${noun} Charm`, `Playful ${noun} Gift Idea`, `Hand-Engrave the ${noun} Charm`,
    `Gift-Ready ${noun} Necklace`, `Shop the ${noun} Necklace`, 'Handcrafted to Order', `Order Your ${noun} Charm`, `Shop ${noun} Jewelry`, `Rose Gold Filled ${noun} Charm`, `A Little ${noun} Every Day`, `Personalized ${noun} Pendant`],
  longHeadlines: [`${title}, handcrafted to order in your choice of metal`, `A playful ${noun.toLowerCase()} charm necklace made for everyday wear`, `Add hand engraving to your ${noun.toLowerCase()} charm and make it personal`,
    `Gift-ready ${noun.toLowerCase()} necklace, ready to shop at Brites Jewelry`, `Meet the ${noun.toLowerCase()} pendant, a small charm with a big personality`],
  descriptions: ['Choose gold filled, sterling silver or rose gold filled.', `A ${noun.toLowerCase()} pendant for a fun, lighthearted everyday look.`, `Gift-ready packaging included. A playful present for ${noun.toLowerCase()} lovers.`,
    'Add hand engraving to the charm. Tap to customize.', 'Pick a 14, 16 or 18 inch chain and wear it with everything.'] });
const COPY = {
  // The validated Duck messaging (tests/adwords/design-publication.cjs): 15 headlines, 5 long headlines, 5 descriptions.
  duck: { headlines: ['Duck Charm Necklace', 'Rubber Ducky Pendant Necklace', 'Tiny 7mm Duck Charm', 'Gold Filled Duck Necklace', 'Sterling Silver Duck Charm', 'Playful Duck Gift Idea', 'Hand-Engrave the Duck Charm', 'Gift-Ready Duck Necklace', 'Shop the Duck Necklace', 'Handcrafted to Order', 'Order Your Duck Charm', 'Shop Duck Jewelry', 'Duck Necklace', 'Rose Gold Filled Duck Charm', 'A Quacktastic Little Gift'],
    longHeadlines: ['Duck Silhouette Charm Necklace with a 7mm rubber ducky pendant', 'A lighthearted duck charm necklace, handcrafted to order in your choice of metal', 'Looking for a playful gift? Meet the rubber ducky charm necklace', 'Add hand engraving to your duck charm and make it personal', 'Gift-ready duck charm necklace, ready to shop at Brites Jewelry'],
    descriptions: ['A 7mm rubber ducky pendant for a fun, lighthearted everyday look.', 'Choose gold filled, sterling silver or rose gold filled. Order yours today.', 'Gift-ready packaging included. A playful present for duck lovers.', 'Add hand engraving to the charm. Tap to customize.', 'Pick a 14, 16 or 18 inch chain and wear a tiny duck charm with everything.'] },
  gecko: copyFor('Gecko', 'Gecko Necklace'), bunny: copyFor('Bunny', 'Bunny Pendant Necklace'), saturn: copyFor('Saturn', 'Planet Saturn Pendant Necklace') };
// The operator's edit in Approvals: one headline and one description reworded.
const EDITED_DUCK = { ...clone(COPY.duck), headlines: COPY.duck.headlines.map(h => h === 'Rose Gold Filled Duck Charm' ? 'Rose Gold Duck Charm' : h), descriptions: COPY.duck.descriptions.map(d => d.startsWith('Pick a 14') ? 'Pick a chain length and wear a tiny duck charm every day.' : d) };
// Gecko's edit, saved in Approvals before the upgrade (S7).
const EDITED_GECKO = { ...clone(COPY.gecko), headlines: COPY.gecko.headlines.map(h => h === 'Shop Gecko Jewelry' ? 'Gecko Charm Gift' : h), descriptions: COPY.gecko.descriptions.map(d => d.startsWith('Pick a 14') ? 'Pick a chain length and wear your gecko charm every day.' : d) };
const FORMATS = { landscape: [1200, 628], square: [1200, 1200], portrait: [960, 1200] };
const PROOFS = [[300, 250], [728, 90], [160, 600], [320, 50], [1000, 1000]]; // the last is not a Google Display size
const FILMS = [['mobile_portrait', 'portrait', 1080, 1350], ['mobile_square', 'square', 1080, 1080], ['desktop_landscape', 'landscape', 1920, 1080]];
const bytesCache = new Map();
async function jpeg(key, width, height, color) {
  const k = key + width + 'x' + height; if (bytesCache.has(k)) return bytesCache.get(k);
  const sharp = require('sharp'), stripe = await sharp({ create: { width: Math.max(8, Math.round(width / 3)), height: Math.max(8, Math.round(height / 3)), channels: 3, background: '#ffffff' } }).png().toBuffer();
  const out = await sharp({ create: { width, height, channels: 3, background: color } }).composite([{ input: stripe, gravity: 'center' }]).jpeg({ quality: 88 }).toBuffer();
  bytesCache.set(k, out); return out;
}
function filmBytes(key, variant) { const parts = []; let h = Buffer.from(key + ':' + variant); for (let i = 0; i < 96; i++) { h = crypto.createHash('sha256').update(h).digest(); parts.push(h); } return Buffer.concat(parts); }
// Seeds one design workspace. selectedKey is the current selection; the other products are remembered in
// productDesigns with their design jobs in the workspace history, exactly as googleAdsAdDesign save() keeps them.
async function seedWorkspace({ id, products, selectedKey, scoped = false }) {
  const D = require(path.join(FN, 'googleAdsAdDesign.js')), R = require(path.join(FN, 'googleAdsMotionReferences.js')), H = ctx.E.creativeHash, docs = ctx.store.docs, files = ctx.store.files, now = Date.now(), ws = wsPath(id), setId = 'set_' + id;
  const save = (dir, name, bytes, ext = 'jpg', meta = {}) => { const hash = H(bytes.toString('base64')), p = `${dir}/${id}/${name}-${hash}.${ext}`; files.set(p, Buffer.from(bytes)); return { path: p, hash, bytes: bytes.length, ...meta }; };
  const productDocs = products.map((p, i) => ({ id: p.gid, title: p.title, url: p.url, handle: p.handle, itemId: p.offers[0], offerIds: p.offers, images: [{ id: 'img_' + p.id + '_1', url: 'https://cdn.shopify.synthetic.test/' + p.handle + '.jpg', altText: p.title }],
    eligibleGroupRefs: [GROUP.ref], creativeGroupRefs: [GROUP.ref], adProduct: true, position: i, language: 'en' }));
  productDocs.forEach(doc => docs.set(`${ws}/sourceSets/${setId}/products/${sha(String(doc.id)).slice(0, 32)}`, clone(doc)));
  const context = { handle: 'animal-charms', title: 'Animal charms', feedLabel: 'US', itemIds: products.flatMap(p => p.offers), dailyBudget: 12, days: 30, countries: ['2840', '2124'], groups: [clone(GROUP)],
    scopeProductId: scoped ? products[0].gid : null, warnings: [], gallerySchema: 2, gallerySources: [] };
  const fx = {}, placements = [];
  for (const p of products) {
    const f = fx[p.key] = { product: p, images: {}, proofs: [], films: [] };
    for (const [format, [w, h]] of Object.entries(FORMATS)) { f.images[format] = save('Brites_GAds_Creative', 'crop_' + p.key + '_' + format, await jpeg(p.key, w, h, p.color), 'jpg', { width: w, height: h, kind: 'crop', mimeType: 'image/jpeg' });
      placements.push({ id: 'pl_' + p.key + '_' + format, groupRef: GROUP.ref, productId: p.gid, productIds: [p.gid], device: 'desktop', format, kind: 'crop', imageId: 'crop_' + p.key + '_' + format, asset: f.images[format] }); }
    for (const [w, h] of PROOFS) f.proofs.push({ key: w + 'x' + h, width: w, height: h, supported: FIXED_SIZES.has(w + 'x' + h), asset: save('Brites_GAds_Creative', 'proof_' + p.key + '_' + w + 'x' + h, await jpeg(p.key + 'proof', w, h, p.color), 'jpg', { width: w, height: h, kind: 'ad proof', mimeType: 'image/jpeg' }) });
    f.settings = D.settingsFor({ productId: p.gid, groupRef: GROUP.ref }, productDocs, context.groups, [], D.FORMATS, {});
    f.messaging = { copy: clone(COPY[p.key]), productId: p.gid, groupRef: GROUP.ref, savedDesignId: null, edited: true, researchedAt: now - 9e6, evidenceHash: sha('evidence' + p.key), updatedAt: now - 8e6 };
    f.job = { id: 'job_' + sha(id + p.key).slice(0, 24), mode: 'design', phase: 'ready', createdAt: now - 7.2e6, updatedAt: now - 7e6, completedAt: now - 7e6, leaseUntil: 0, inFlight: null, sourceVersion: null, snapshotHash: null,
      settingsHash: sha(f.settings), progress: { pct: 100, label: 'Design ready' }, result: { copy: clone(COPY[p.key]), assets: clone(f.images), productIds: [p.gid], brief: { rationale: 'Synthetic product photographs', hypothesis: 'Product clarity', successMetric: 'qualified_clicks' } } };
    // The saved all-size review (fixed Display proofs) of this product.
    const eid = 'eai_' + sha('eai' + id + p.key).slice(0, 24); f.editorJobId = eid;
    docs.set(`${ws}/editorAIJobs/${eid}`, { id: eid, scope: { productId: p.gid, groupRef: GROUP.ref }, phase: 'ready', designKey: 'synthetic_' + p.key, requestId: 'req_' + eid, inputHash: sha(eid), createdAt: now - 3.6e6, updatedAt: now - 3.5e6, progress: { pct: 100, label: 'Saved review ready' } });
    docs.set(`${ws}/editorAIJobs/${eid}/data/ad_quality_v2`, { rubric: 'synthetic', score: 96, pass: true, productFaithful: true, mobileReadable: true, claimsSupported: true, issues: [] });
    docs.set(`${ws}/editorAIJobs/${eid}/data/ad_proofs_v2`, { images: f.proofs.map(x => ({ key: x.key, width: x.width, height: x.height, asset: x.asset })), proofHash: sha('proof' + eid), candidateHash: sha('candidate' + eid) });
    // Three finished reference-guided films (portrait, square, landscape), ready for publication.
    const mid = 'motion_' + sha('motion' + id + p.key).slice(0, 40), referenceHash = sha('reference' + p.key); f.motionJobId = mid;
    for (const [key, format, w, h] of FILMS) { const bytes = filmBytes(p.key, key), asset = save('Brites_GAds_Motion', p.key + '_' + key, bytes, 'mp4', { mimeType: 'video/mp4', width: w, height: h });
      const frames = []; for (let i = 0; i < 6; i++) frames.push(save('Brites_GAds_Motion', p.key + '_' + key + '_frame' + i, await jpeg(p.key + 'frame' + i, 64, 64, p.color), 'jpg', { mimeType: 'image/jpeg', width: 64, height: 64 }));
      f.films.push({ key, format, device: key.startsWith('mobile') ? 'mobile' : 'desktop', width: w, height: h, seconds: 10, asset, frames, fidelity: { policy: R.POLICY, referenceHash, assetHash: asset.hash, videoHash: sha256(bytes) } }); }
    docs.set(`${ws}/motionJobs/${mid}`, { id: mid, workspaceId: id, productId: p.gid, groupRef: GROUP.ref, destination: p.url, title: p.title, phase: 'ready', quality: { score: 94, pass: true, issues: [] }, completedAt: now - 1.8e6, createdAt: now - 3e6, updatedAt: now - 1.8e6,
      leaseUntil: 0, inFlight: null, owner: null, motionMode: R.MODE, referencePolicy: R.POLICY, referenceHash, pipelineVersion: 2, renderVersion: 10, plan: { copy: { headline: COPY[p.key].headlines[0] }, nativeCopy: clone(COPY[p.key]) },
      variants: clone(f.films), originalSources: [{ asset: clone(f.images.square), productId: p.gid, selection: 'design-primary-photo' }], progress: { pct: 100, label: 'Films ready' }, publication: null });
  }
  const sel = fx[selectedKey], productDesigns = {};
  for (const p of products) if (p.key !== selectedKey) { const f = fx[p.key]; productDesigns[sha([GROUP.ref, p.gid]).slice(0, 32)] = { settings: clone(f.settings), messaging: clone(f.messaging), jobId: f.job.id }; docs.set(`${ws}/history/${f.job.id}`, clone(f.job)); }
  docs.set(ws, clone({ schema: 1, workspaceId: id, context, sourceVersion: null, snapshotHash: null, sourceSnapshot: null, sourceSetId: setId, productsIds: productDocs.map(d => d.id), settings: sel.settings, references: [], productDesigns,
    messaging: sel.messaging, refreshWarnings: [], placements, job: sel.job, editorAI: null, revision: 4, createdAt: now - 8.64e7, updatedAt: now - 3.6e6 }));
  return { id, fx };
}
const motionJob = (wsId, f) => ctx.store.docs.get(`${wsPath(wsId)}/motionJobs/${f.motionJobId}`);

/* ================================================================ Google-side audits of a publication */
// The operations of one recorded request, resolved into campaigns, groups, links, filters, ads and criteria.
function audit(rec) {
  const ops = rec.ops, info = rn => rec.assetInfo.get(rn) || ((ctx.google.assets.get(rn) || {})._info) || {}, creates = t => ops.filter(o => o[t] && o[t].create).map(o => o[t].create);
  const campaigns = creates('campaignOperation'), budgets = creates('campaignBudgetOperation'), groups = creates('assetGroupOperation'), links = creates('assetGroupAssetOperation'), filters = creates('assetGroupListingGroupFilterOperation');
  const ads = creates('adGroupAdOperation'), adGroups = creates('adGroupOperation'), criteria = creates('campaignCriterionOperation'), updates = ops.filter(o => Object.values(o)[0].update).map(o => ({ type: Object.keys(o)[0], ...o[Object.keys(o)[0]] }));
  return { ops, campaigns, budgets, groups, links, filters, ads, adGroups, criteria, updates, info,
    linksOf: (grp, ft) => links.filter(l => l.assetGroup === grp && l.fieldType === ft).map(l => ({ ...l, info: info(l.asset) })),
    filtersOf: grp => filters.filter(f => f.assetGroup === grp) };
}
// No campaign is created or switched on as ENABLED; an ENABLED child (ad group, ad, asset group) only
// sits under a campaign that is created PAUSED in the same request or already exists PAUSED.
function enabledProblems(recs) {
  const out = [];
  for (const rec of recs) { const a = audit(rec), paused = new Set(a.campaigns.filter(c => c.status === 'PAUSED').map(c => c.resourceName));
    a.campaigns.filter(c => c.status !== 'PAUSED').forEach(c => out.push('campaign created ' + c.status + ': ' + c.name));
    a.updates.filter(u => u.type === 'campaignOperation' && u.update.status === 'ENABLED').forEach(u => out.push('campaign enabled: ' + u.update.resourceName));
    const parentPaused = rn => paused.has(rn) || (ctx.google.campaigns.get(rn) || {}).status === 'PAUSED' || (rec.idMap && [...rec.idMap].some(([t, r]) => r === rn && paused.has(t)));
    a.groups.filter(x => x.status === 'ENABLED' && !parentPaused(x.campaign)).forEach(x => out.push('enabled asset group under a non-paused campaign: ' + x.name));
    a.adGroups.filter(x => x.status === 'ENABLED' && !parentPaused(x.campaign)).forEach(x => out.push('enabled ad group under a non-paused campaign: ' + x.name)); }
  return out;
}
// The exact product isolation Performance Max needs: one SUBDIVISION, the product's own UNIT_INCLUDED
// offers, and one catch-all UNIT_EXCLUDED.
function isolationProblem(filters, p) {
  const sub = filters.filter(f => f.type === 'SUBDIVISION'), inc = filters.filter(f => f.type === 'UNIT_INCLUDED'), exc = filters.filter(f => f.type === 'UNIT_EXCLUDED');
  const values = inc.map(f => f.caseValue && f.caseValue.productItemId && f.caseValue.productItemId.value).sort();
  if (sub.length !== 1) return sub.length + ' SUBDIVISION nodes';
  if (JSON.stringify(values) !== JSON.stringify(p.offers.slice().sort())) return 'included ' + JSON.stringify(values) + ' instead of ' + JSON.stringify(p.offers);
  if (exc.length !== 1 || (exc[0].caseValue && exc[0].caseValue.productItemId && exc[0].caseValue.productItemId.value)) return exc.length + ' UNIT_EXCLUDED nodes / not a catch-all';
  if (filters.length !== inc.length + 2) return 'unexpected extra filters';
  return null;
}

/* ================================================================ the complete-ad flow, step by step */
const STYLE_NAMES = { pmax: 'Performance Max', responsive_display: 'Responsive Display', fixed_display: 'Fixed Display' };
async function submitAd(ws, p, tag = null) {
  step(p.key + ' submit');
  const r = await api('prepareAdDesignPublication', { workspaceId: ws.id, target: 'ads', formats: ['landscape', 'portrait', 'square'], includeCopy: true, queueOnly: true, reviewCopy: clone(COPY[p.key]) });
  const ok = check(r.ok === true && r.status === 'PENDING' && /^design-review-[a-f0-9]{32}$/.test(r.approvalId || ''), p.short + ': sent to Approval as a pending complete ad', { tag, detail: why(r) });
  const doc = ok && approval(r.approvalId);
  check(doc && doc.designReview.productId === p.gid && doc.designReview.groupRef === GROUP.ref && doc.designReview.workspaceId === ws.id && doc.designReview.destination === p.url && reqsIn(p.key + ' submit').length === 0,
    p.short + ': the review pins its own product, group, workspace and destination; nothing is sent to Google', { tag, detail: doc ? JSON.stringify({ productId: doc.designReview.productId, destination: doc.designReview.destination }) : why(r) });
  return ok ? { id: r.approvalId, reviewHash: r.reviewHash } : null;
}
async function reviewPreviews(st, ws, p, ap, label = 'review') {
  const f = ws.fx[p.key]; step(p.key + ' ' + label);
  const r = ap ? await api('adDesignSubmissionStatus', { id: ap.id, hash: ap.reviewHash }) : { error: 'no approval' };
  st.gate(p.short + ': review previews load', () => r.ok === true, () => why(r));
  const responsive = () => (r.images || []).filter(i => i.kind === 'responsive'), fixed = () => (r.images || []).filter(i => i.kind === 'fixed');
  st.check(p.short + ': review shows its own landscape, square and portrait photos', () => responsive().length === 3 && Object.entries(f.images).every(([format, a]) => responsive().some(i => i.format === format && i.hash === a.hash && i.width === a.width && i.height === a.height && /^https:\/\//.test(i.url))),
    () => 'shown ' + JSON.stringify(responsive().map(i => [i.format, (i.hash || '').slice(0, 10)])) + ' own ' + JSON.stringify(Object.entries(f.images).map(([k, a]) => [k, a.hash.slice(0, 10)])));
  st.check(p.short + ': review shows its own fixed display proofs, Google-supported sizes only', () => { const own = f.proofs.filter(x => x.supported); return fixed().length === own.length && own.every(x => fixed().some(i => i.hash === x.asset.hash && i.width === x.width && i.height === x.height)); },
    () => 'shown ' + JSON.stringify(fixed().map(i => [i.width + 'x' + i.height, (i.hash || '').slice(0, 10)])));
  st.check(p.short + ': review shows its own three saved films', () => { const v = r.videos || []; return v.length === 3 && f.films.every(film => v.some(x => x.key === film.key && x.asset && x.asset.hash === film.asset.hash && /^https:\/\//.test(x.url))); },
    () => 'videos ' + JSON.stringify((r.videos || []).map(v => [v.key, v.asset && v.asset.hash.slice(0, 10)])) + ' warnings ' + JSON.stringify(r.warnings));
  st.check(p.short + ': review links the product page, previews load without warnings', () => r.destination === p.url && Array.isArray(r.warnings) && !r.warnings.length, () => r.destination + ' ' + JSON.stringify(r.warnings));
  return r;
}
async function editMessaging(st, ws, p, ap, copy, expectEdited) {
  step(p.key + ' edit messaging');
  const before = clone(ctx.store.docs.get(wsPath(ws.id)));
  const r = await api('updateAdDesignSubmission', { id: ap.id, hash: ap.reviewHash, copy, includeVideos: true });
  st.gate(p.short + ': messaging edit is saved to the pending review', () => r.ok === true && /^[a-f0-9]{64}$/.test(r.reviewHash || '') && r.reviewHash !== ap.reviewHash, () => why(r));
  if (r.ok) ap.reviewHash = r.reviewHash;
  st.check(p.short + ': the review keeps the edited messaging; the design workspace and Google are untouched', () => { const d = approval(ap.id); return JSON.stringify(d.designReview.copy) === JSON.stringify(copy) && d.status === 'PENDING' && JSON.stringify(ctx.store.docs.get(wsPath(ws.id))) === JSON.stringify(before) && !reqsIn(p.key + ' edit messaging').length; });
  st.check(p.short + ': the review marks the copy as ' + (expectEdited ? 'edited' : 'unchanged') + ' against its own saved artwork', () => approval(ap.id).designReview.copyEdited === expectEdited, () => 'copyEdited=' + (approval(ap.id) || { designReview: {} }).designReview.copyEdited);
  return r;
}
async function preparePlan(st, p, ap, choice, label = 'plan') {
  step(p.key + ' ' + label);
  const before = counts(), r = await api('publishAdDesignSubmission', { id: ap.id, hash: ap.reviewHash, prepareOnly: true, ...choice });
  const styles = choice.styles.slice().sort().join(' + ');
  st.gate(p.short + ': plan is prepared for ' + styles, () => r.ok !== false && /^[a-f0-9]{64}$/.test(r.planHash || '') && r.plan, () => why(r));
  if (r.reviewHash && r.reviewHash !== ap.reviewHash) ap.reviewHash = r.reviewHash;
  if (r.planHash) ap.planHash = r.planHash;
  const plan = r.plan || {}, doc = () => approval(ap.id), today = ymd(0);
  st.check(p.short + ': plan summary is paused, product destination, chosen budgets, durations and countries', () => {
    const cs = plan.campaigns || []; if (plan.status !== 'PAUSED' || plan.destination !== p.url || cs.length !== choice.styles.length) return false;
    if (JSON.stringify(plan.countries) !== JSON.stringify(choice.countries.map(String).sort())) return false;
    return cs.every(c => { if (c.joins) return c.addedDaily === choice.budgets.pmax; const days = Number(choice.durations[c.style]); return c.dailyBudget === choice.budgets[c.style] && (days > 0 ? c.days === days && c.endDate === addDays(today, days - 1) : c.days === null && c.endDate === null); });
  }, () => JSON.stringify(plan).slice(0, 500));
  st.check(p.short + ': plan uses only this product\'s own saved photographs and proofs', () => {
    const f = ap.fx, gen = (doc().pipelinePlan.payload.generatedAssets || []), own = new Set([...Object.values(f.images).map(a => a.hash)]);
    const photos = gen.filter(x => !x.fixed && !/logo/.test(x.asset.kind || '') && !/^Brites_GAds_Creative\/[^/]+\/pipeline_logo/.test(x.asset.path));
    const needed = choice.styles.some(s => s !== 'fixed_display') ? (choice.styles.includes('pmax') ? 3 : 2) : 0;
    return photos.length === needed && photos.every(x => own.has(x.asset.hash)) && gen.filter(x => x.fixed).length === (choice.styles.includes('fixed_display') ? f.proofs.filter(x => x.supported).length : 0);
  }, () => JSON.stringify((doc().pipelinePlan || { payload: {} }).payload.generatedAssets.map(x => [x.asset.kind, x.asset.width + 'x' + x.asset.height, x.asset.hash.slice(0, 10)])));
  st.check(p.short + ': preparing the plan sends nothing to Google', () => counts() === before && !reqsIn(p.key + ' ' + label).length);
  return r;
}
async function checkWithGoogle(st, p, ap, label = 'check with Google') {
  step(p.key + ' ' + label);
  const before = counts(), r = st.blocked ? null : await api('publishAdDesignSubmission', { id: ap.id, hash: ap.reviewHash, planHash: ap.planHash, validateOnly: true }), reqs = reqsIn(p.key + ' ' + label);
  const name = p.short + (label === 'check with Google' ? '' : ' (' + label + ')');
  st.gate(name + ': Check with Google is accepted', () => r.ok === true && /validation mode/.test(r.message || ''), () => why(r) + (reqs[0] ? ' · ' + JSON.stringify(reqs[0].errors).slice(0, 600) : ''));
  st.check(name + ': Check with Google sends one validateOnly:true request and creates nothing', () => reqs.length === 1 && reqs[0].validateOnly && counts() === before, () => reqs.length + ' requests');
  return { r, rec: reqs[0] };
}
// A published new ad: Google-side checks of every campaign the plan created.
function verifyPublication(st, p, ap, choice, recs, copy) {
  const real = recs.filter(r => !r.validateOnly), rec = real[0], a = rec && audit(rec), f = ap.fx, today = ymd(0);
  st.check(p.short + ': Approve sends one validate-only request, then exactly one real request with the same operations', () => recs.length === 2 && recs[0].validateOnly && !recs[1].validateOnly && recs.every(r => r.ok) && JSON.stringify(recs[0].ops) === JSON.stringify(recs[1].ops),
    () => recs.map(r => (r.validateOnly ? 'validate' : 'real') + ':' + (r.ok ? 'ok' : JSON.stringify(r.errors).slice(0, 300))).join(' | '));
  st.check(p.short + ': every created campaign is PAUSED; nothing is ENABLED outside a paused campaign', () => a.campaigns.length && a.campaigns.every(c => c.status === 'PAUSED') && !enabledProblems(recs).length, () => enabledProblems(recs).join('; '));
  const own = choice.styles.filter(s => !(s === 'pmax' && choice.pmaxTarget));
  st.check(p.short + ': one new campaign per chosen ad type, named for this product', () => a.campaigns.length === own.length && own.every(s => a.campaigns.some(c => c.name.startsWith('Brites · ' + p.title.slice(0, 60) + ' · ' + STYLE_NAMES[s] + ' · '))),
    () => JSON.stringify(a.campaigns.map(c => c.name)));
  st.check(p.short + ': budgets are sent in micros (multiples of the currency unit) and equal the chosen daily budgets', () => a.campaigns.every(c => { const b = a.budgets.find(x => x.resourceName === c.campaignBudget), style = Object.keys(STYLE_NAMES).find(s => c.name.includes(' · ' + STYLE_NAMES[s] + ' · '));
    return b && Number(b.amountMicros) === Math.round(choice.budgets[style] * 1e6) && Number(b.amountMicros) % 10000 === 0; }), () => JSON.stringify(a.budgets.map(b => b.amountMicros)));
  st.check(p.short + ': run lengths end on the planned day; until-paused campaigns have no end date', () => a.campaigns.every(c => { const style = Object.keys(STYLE_NAMES).find(s => c.name.includes(' · ' + STYLE_NAMES[s] + ' · ')), days = Number(choice.durations[style]);
    return days > 0 ? c.endDateTime === addDays(today, days - 1) + ' 23:59:59' : c.endDateTime === undefined; }), () => JSON.stringify(a.campaigns.map(c => [c.name.split(' · ')[2], c.endDateTime])));
  st.check(p.short + ': each campaign targets exactly the chosen countries', () => a.campaigns.every(c => JSON.stringify(a.criteria.filter(x => x.campaign === c.resourceName && x.location).map(x => x.location.geoTargetConstant.split('/')[1]).sort()) === JSON.stringify(choice.countries.map(String).sort())));
  st.check(p.short + ': Google accepted the real request with no rule violations', () => rec.ok && !rec.errors.length);
  if (choice.styles.includes('pmax')) {
    const grp = () => a.groups[0];
    st.check(p.short + ' PMax: one asset group whose final URL is the product\'s https page', () => a.groups.length === 1 && JSON.stringify(grp().finalUrls) === JSON.stringify([p.url]), () => JSON.stringify(a.groups.map(x => x.finalUrls)));
    st.check(p.short + ' PMax: listing groups isolate exactly the product\'s offers', () => !isolationProblem(a.filtersOf(grp().resourceName), p), () => isolationProblem(a.filtersOf(grp().resourceName), p));
    st.check(p.short + ' PMax: headlines 3-15 (≤30, one ≤15), long headlines 1-5 (≤90), descriptions 2-5 (≤90, one ≤60) equal the reviewed messaging', () => {
      const t = ft => a.linksOf(grp().resourceName, ft).map(l => l.info.text), h = t('HEADLINE'), lh = t('LONG_HEADLINE'), d = t('DESCRIPTION');
      return h.length >= 3 && h.length <= 15 && h.every(x => len(x) <= 30) && h.some(x => len(x) <= 15) && lh.length >= 1 && lh.length <= 5 && lh.every(x => len(x) <= 90) && d.length >= 2 && d.length <= 5 && d.every(x => len(x) <= 90) && d.some(x => len(x) <= 60)
        && JSON.stringify(h) === JSON.stringify(copy.headlines) && JSON.stringify(lh) === JSON.stringify(copy.longHeadlines) && JSON.stringify(d) === JSON.stringify(copy.descriptions); });
    st.check(p.short + ' PMax: business name ≤25 and logo links follow the campaign\'s brand guidelines', () => { const bn = a.linksOf(grp().resourceName, 'BUSINESS_NAME'), logo = a.linksOf(grp().resourceName, 'LOGO');
      return choice.pmaxTarget ? !bn.length && !logo.length : bn.length === 1 && len(bn[0].info.text) <= 25 && logo.length === 1; });
    st.check(p.short + ' PMax: landscape 1.91:1 ≥600×314, square 1:1 ≥300×300, portrait 4:5 ≥480×600, logo 1:1 ≥128, wide logo 4:1, all ≤5 MB, and the photos are this product\'s own', () => {
      const one = (ft, spec, ownHash) => { const l = a.linksOf(grp().resourceName, ft); return l.length === 1 && l.every(x => x.info.kind === 'image' && Math.abs(x.info.width / x.info.height - spec[0]) <= spec[0] * 0.01 && x.info.width >= spec[1] && x.info.height >= spec[2] && x.info.bytes <= 5120 * 1024 && (!ownHash || x.info.contentHash === ownHash)); };
      return one('MARKETING_IMAGE', IMAGE_SPEC.MARKETING_IMAGE, f.images.landscape.hash) && one('SQUARE_MARKETING_IMAGE', IMAGE_SPEC.SQUARE_MARKETING_IMAGE, f.images.square.hash) && one('PORTRAIT_MARKETING_IMAGE', IMAGE_SPEC.PORTRAIT_MARKETING_IMAGE, f.images.portrait.hash)
        && (choice.pmaxTarget || (one('LOGO', IMAGE_SPEC.LOGO) && one('LANDSCAPE_LOGO', IMAGE_SPEC.LANDSCAPE_LOGO))); });
    st.check(p.short + ' PMax: the native Shop now action is linked', () => a.linksOf(grp().resourceName, 'CALL_TO_ACTION_SELECTION').some(l => l.info.cta === 'SHOP_NOW'));
    if (!choice.pmaxTarget) st.check(p.short + ' PMax: the new campaign bids for purchases only (its own conversion goals in the same request)', () => { const u = a.updates.filter(x => x.type === 'campaignConversionGoalOperation');
      return u.length === 3 && u.every(x => x.update.resourceName.startsWith(a.campaigns.find(c => c.advertisingChannelType === 'PERFORMANCE_MAX').resourceName.replace('/campaigns/', '/campaignConversionGoals/') + '~') && x.update.biddable === /~PURCHASE~/.test(x.update.resourceName)); });
  }
  if (choice.styles.includes('responsive_display')) {
    const ad = () => a.ads.map(x => x.ad).find(x => x.responsiveDisplayAd), r = () => ad().responsiveDisplayAd;
    st.check(p.short + ' Responsive Display: ≤5 headlines ≤30, one long headline ≤90, ≤5 descriptions ≤90, business name ≤25, https product URL', () => r().headlines.length >= 1 && r().headlines.length <= 5 && r().headlines.every(x => len(x.text) <= 30) && len(r().longHeadline.text) <= 90
      && r().descriptions.length >= 1 && r().descriptions.length <= 5 && r().descriptions.every(x => len(x.text) <= 90) && len(r().businessName) <= 25 && JSON.stringify(ad().finalUrls) === JSON.stringify([p.url])
      && JSON.stringify(r().headlines.map(x => x.text)) === JSON.stringify(copy.headlines.slice(0, 5)) && r().longHeadline.text === copy.longHeadlines[0]);
    st.check(p.short + ' Responsive Display: own landscape and square photos, square and 4:1 logos', () => { const im = x => a.info(x.asset);
      return r().marketingImages.length === 1 && im(r().marketingImages[0]).contentHash === f.images.landscape.hash && r().squareMarketingImages.length === 1 && im(r().squareMarketingImages[0]).contentHash === f.images.square.hash
        && r().squareLogoImages.every(x => im(x).width === im(x).height && im(x).width >= 128) && r().logoImages.every(x => Math.abs(im(x).width / im(x).height - 4) <= 0.04); });
  }
  if (choice.styles.includes('fixed_display')) {
    const imgs = () => a.ads.map(x => x.ad).filter(x => x.imageAd).map(x => ({ ...a.info(x.imageAd.imageAsset.asset), url: x.finalUrls }));
    st.check(p.short + ' Fixed Display: one image ad per supported proof size (unsupported sizes dropped), each ≤150 KB, https product URL', () => { const want = f.proofs.filter(x => x.supported).map(x => x.key).sort();
      return JSON.stringify(imgs().map(i => i.width + 'x' + i.height).sort()) === JSON.stringify(want) && imgs().every(i => FIXED_SIZES.has(i.width + 'x' + i.height) && i.bytes <= 150 * 1024 && JSON.stringify(i.url) === JSON.stringify([p.url])); },
      () => JSON.stringify(imgs().map(i => [i.width + 'x' + i.height, i.bytes])));
  }
  return a;
}
function verifyFilms(st, p, ap, groupRn, campaignRn, filmStep) {
  const f = ap.fx, job = () => motionJob(ap.wsId, f), uploads = () => [...ctx.google.uploads.values()].filter(u => ctx.google.sessions.get(u._sid).step === filmStep), recs = () => reqsIn(filmStep);
  st.check(p.short + ' films: three reviewed films upload to YouTube as unlisted videos, byte-identical to the saved files', () => uploads().length === 3 && uploads().every(u => u.videoPrivacy === 'UNLISTED' && f.films.some(x => x.fidelity.videoHash === u._sha256))
    && new Set(uploads().map(u => u._sha256)).size === 3, () => JSON.stringify(uploads().map(u => [u.videoPrivacy, u._sha256.slice(0, 10)])) + ' publication ' + JSON.stringify((job() || {}).publication || null).slice(0, 300));
  st.check(p.short + ' films: attachment is validated first, then attached to the new paused asset group', () => { const r = recs(); return r.length === 2 && r[0].validateOnly && !r[1].validateOnly && r.every(x => x.ok) && r.every(x => { const a = audit(x);
    return a.links.length === 3 && a.links.every(l => l.assetGroup === groupRn && l.fieldType === 'YOUTUBE_VIDEO') && a.ops.filter(o => o.assetOperation).every(o => o.assetOperation.create.youtubeVideoAsset) && !a.campaigns.length; }); },
    () => recs().map(x => (x.validateOnly ? 'validate' : 'real') + ':' + (x.ok ? 'ok' : JSON.stringify(x.errors).slice(0, 200))).join(' | '));
  st.check(p.short + ' films: the group shows the three uploaded videos, the campaign stays PAUSED, and the publication is attached', () => { const vids = ctx.google.links.filter(l => l.assetGroup === groupRn && l.fieldType === 'YOUTUBE_VIDEO').map(l => ctx.google.assets.get(l.asset).youtubeVideoAsset.youtubeVideoId).sort();
    const pub = job().publication; return JSON.stringify(vids) === JSON.stringify(uploads().map(u => u.videoId).sort()) && ctx.google.campaigns.get(campaignRn).status === 'PAUSED' && pub.phase === 'attached' && pub.target && pub.target.groupRef === groupRn; },
    () => JSON.stringify((job() || {}).publication || null).slice(0, 400));
}
// Prepare -> Check with Google -> Approve -> verify, for one review.
async function publishAd(st, ws, p, ap, choice, copy, opts = {}) {
  await preparePlan(st, p, ap, choice);
  await checkWithGoogle(st, p, ap);
  step(p.key + ' approve');
  const before = new Set(ctx.google.campaigns.keys()), r = st.blocked ? null : await approve(ap);
  st.gate(p.short + ': Approve ad publishes the plan (status APPLIED)', () => r && r.ok !== false && r.status === 'APPLIED' && approval(ap.id).status === 'APPLIED', () => why(r) + ' · approval ' + JSON.stringify(approval(ap.id) && { status: approval(ap.id).status, lastError: approval(ap.id).lastError }));
  st.check(p.short + ': the Approve request only approves and queues the publication: it answers before the background worker starts and sends nothing to Google', () => queuedOnly(r), () => queuedDetail(r));
  st.check(p.short + ': the worker saves the outcome on the approval for its card (publishOutcome APPLIED with the message the request used to return)', () => { const o = approval(ap.id).publishOutcome, w = r.meta.worker;
    return o && o.status === 'APPLIED' && w && o.message === w.message && /created paused/.test(o.message) && o.at >= r.meta.request.requestedAt && approval(ap.id).publishRequestedAt === r.meta.request.requestedAt; }, () => JSON.stringify(approval(ap.id) && approval(ap.id).publishOutcome));
  const recs = reqsIn(p.key + ' approve'), a = st.blocked ? null : verifyPublication(st, p, ap, choice, recs, copy);
  st.check(p.short + ': the approval records the new campaign IDs', () => { const ids = (approval(ap.id).publishedCampaignIds || []).map(id => RN('campaigns', id)), fresh = [...ctx.google.campaigns.keys()].filter(k => !before.has(k));
    return JSON.stringify(ids.sort()) === JSON.stringify(fresh.sort()) && fresh.length === a.campaigns.length; });
  if (choice.styles.includes('pmax') && opts.films !== false) {
    const real = recs.find(x => !x.validateOnly), grp = real && a && (real.idMap.get(a.groups[0].resourceName)), camp = grp && ctx.google.assetGroups.get(grp).campaign;
    st.check(p.short + ' films: Approve starts the reviewed film upload for the new group', () => /uploading to YouTube/.test(r.message || ''), () => r && r.message);
    if (!st.blocked) verifyFilms(st, p, ap, grp, camp, p.key + ' approve · adMotionPublication'); else ['three reviewed films upload', 'attachment is validated first', 'the group shows the three uploaded videos'].forEach(n => st.check(p.short + ' films: ' + n, () => false));
  }
  return { r, a, recs };
}

/* ================================================================ scenarios */
const SCENARIOS = [];
const scenario = (key, title, fn) => SCENARIOS.push({ key, title, fn });

scenario('S1', 'S1 four products in their own workspaces', async () => {
  await boot('S1 own workspaces');
  const plans = {
    duck: { edit: EDITED_DUCK, choice: { styles: ['pmax', 'responsive_display'], budgets: { pmax: 12, responsive_display: 6 }, countries: ['2840', '2124'], durations: { pmax: 30, responsive_display: 14 } } },
    gecko: { choice: { styles: ['fixed_display', 'responsive_display', 'pmax'], budgets: { fixed_display: 5, responsive_display: 5, pmax: 10 }, countries: ['2124'], durations: { fixed_display: 7, responsive_display: 30, pmax: 45 } } },
    bunny: { choice: { styles: ['pmax'], budgets: { pmax: 15 }, countries: ['2840'], durations: { pmax: 0 } } },
    saturn: { choice: { styles: ['responsive_display', 'fixed_display'], budgets: { responsive_display: 6, fixed_display: 4 }, countries: ['2840', '2124'], durations: { responsive_display: 10, fixed_display: 10 } } } };
  for (const p of PRODUCTS) {
    const ws = await seedWorkspace({ id: 'design_own_' + p.key, products: [p], selectedKey: p.key, scoped: true }), st = stage(null, p.short), plan = plans[p.key];
    const ap = await submitAd(ws, p); if (!ap) st.blocked = 'submission failed'; else Object.assign(ap, { fx: ws.fx[p.key], wsId: ws.id });
    await reviewPreviews(st, ws, p, ap);
    await editMessaging(st, ws, p, ap, plan.edit || clone(COPY[p.key]), !!plan.edit);
    if (plan.edit) { step(p.key + ' fixed after edit');
      const r = st.blocked ? null : await api('publishAdDesignSubmission', { id: ap.id, hash: ap.reviewHash, prepareOnly: true, styles: ['fixed_display'], budgets: { fixed_display: 5 }, countries: ['2840'], durations: { fixed_display: 7 } });
      st.check(p.short + ': Fixed Display is refused after the messaging was edited (its finished artwork shows the old copy)', () => r && r.ok === false && /edited messaging differs/.test(r.error || ''), () => why(r)); }
    await publishAd(st, ws, p, ap, plan.choice, plan.edit || COPY[p.key]);
    // A second look at the published ad's review: the card is closed, the approval stays APPLIED.
    step(p.key + ' after'); const again = st.blocked ? null : await api('adDesignSubmissionStatus', { id: ap.id, hash: ap.reviewHash });
    st.check(p.short + ': a published review no longer opens as pending, and its approval stays APPLIED', () => again && again.ok === false && approval(ap.id).status === 'APPLIED', () => why(again));
  }
});

scenario('S2', 'S2 plan matrix, existing PMax, budget ceiling', async () => {
  await boot('S2 plans');
  const p = P.duck, ws = await seedWorkspace({ id: 'design_plans_duck', products: [p], selectedKey: 'duck', scoped: true }), st = stage(null, 'plan matrix');
  const ap = await submitAd(ws, p); if (!ap) st.blocked = 'submission failed'; else Object.assign(ap, { fx: ws.fx.duck, wsId: ws.id });
  await editMessaging(st, ws, p, ap, clone(COPY.duck), false);
  const matrix = [
    ['pmax only', { styles: ['pmax'], budgets: { pmax: 11 }, countries: ['2840', '2124'], durations: { pmax: 30 } }],
    ['responsive display only', { styles: ['responsive_display'], budgets: { responsive_display: 7.5 }, countries: ['2124'], durations: { responsive_display: 21 } }],
    ['fixed display only', { styles: ['fixed_display'], budgets: { fixed_display: 4.25 }, countries: ['2840'], durations: { fixed_display: 0 } }],
    ['all three', { styles: ['pmax', 'fixed_display', 'responsive_display'], budgets: { pmax: 10, fixed_display: 5, responsive_display: 5 }, countries: ['2840', '2124'], durations: { pmax: 60, fixed_display: 14, responsive_display: 0 } }]];
  for (const [label, choice] of matrix) {
    const s = stage(null, label), local = { ...ap }; s.blocked = st.blocked;
    await preparePlan(s, p, local, choice, 'plan ' + label);
    const { rec } = await checkWithGoogle(s, p, local, 'check ' + label); ap.reviewHash = local.reviewHash;
    if (!s.blocked) { const a = audit(rec);
      s.check('Duck ' + label + ': the validated request creates one PAUSED campaign per ad type with micros budgets, planned end dates and chosen countries', () => a.campaigns.length === choice.styles.length && a.campaigns.every(c => c.status === 'PAUSED')
        && a.budgets.every(b => Number(b.amountMicros) % 10000 === 0) && choice.styles.every(sname => { const c = a.campaigns.find(x => x.name.includes(' · ' + STYLE_NAMES[sname] + ' · ')), b = c && a.budgets.find(x => x.resourceName === c.campaignBudget), days = choice.durations[sname];
          return c && b && Number(b.amountMicros) === Math.round(choice.budgets[sname] * 1e6) && (days > 0 ? c.endDateTime === addDays(ymd(0), days - 1) + ' 23:59:59' : c.endDateTime === undefined)
            && JSON.stringify(a.criteria.filter(x => x.campaign === c.resourceName && x.location).map(x => x.location.geoTargetConstant.split('/')[1]).sort()) === JSON.stringify(choice.countries.slice().sort()); }), () => JSON.stringify(a.campaigns.map(c => [c.name, c.status, c.endDateTime])));
      s.check('Duck ' + label + ': temporary IDs are unique across campaign lanes and every reference follows its creation', () => rec.ok && !rec.errors.some(e => /TEMP|TEMPORARY/.test(e.code)));
      if (choice.styles.includes('pmax')) s.check('Duck ' + label + ': the PMax group isolates the product and links the product page', () => a.groups.length === 1 && !isolationProblem(a.filtersOf(a.groups[0].resourceName), p) && a.groups[0].finalUrls[0] === p.url, () => isolationProblem(a.filtersOf(a.groups[0].resourceName), p));
      if (choice.styles.includes('fixed_display')) s.check('Duck ' + label + ': fixed image ads use only supported sizes', () => JSON.stringify(a.ads.filter(x => x.ad.imageAd).map(x => { const i = a.info(x.ad.imageAd.imageAsset.asset); return i.width + 'x' + i.height; }).sort()) === JSON.stringify(['160x600', '300x250', '320x50', '728x90']));
      if (choice.styles.includes('responsive_display')) s.check('Duck ' + label + ': the responsive display ad stays within its limits', () => a.ads.filter(x => x.ad.responsiveDisplayAd).length === 1 && rec.ok); }
    if (s.blocked) st.blocked = st.blocked || s.blocked;
  }
  // The daily ceiling: 70 of 100 is already enabled, so 35 more is refused before any plan is built.
  step('duck plan over ceiling'); const before = counts();
  const over = st.blocked ? null : await api('publishAdDesignSubmission', { id: ap.id, hash: ap.reviewHash, prepareOnly: true, styles: ['pmax'], budgets: { pmax: 35 }, countries: ['2840'], durations: { pmax: 30 } });
  st.check('Duck: a plan over the daily ceiling is refused at preparation and nothing is sent', () => over && over.ok === false && /Over your daily ceiling/.test(over.error || '') && counts() === before && !reqsIn('duck plan over ceiling').length, () => why(over));
  // Joining the existing paused Performance Max campaign (brand guidelines on) with a budget raise of 5.
  const join = { styles: ['pmax'], budgets: { pmax: 5 }, countries: ['2840', '2124'], durations: {}, pmaxTarget: '9001' }, js = stage(null, 'join existing'); js.blocked = st.blocked;
  await preparePlan(js, p, ap, join, 'plan join');
  js.check('Duck join: the plan joins “Brites · Animal Charms PMax”, raising its budget 25 → 30, and creates no campaign', () => { const c = approval(ap.id).pipelinePlan.summary.campaigns[0], ops = approval(ap.id).pipelinePlan.payload.mutateOperations;
    return c.joins === true && c.existingCampaignId === '9001' && c.dailyBudget === 30 && !ops.some(o => o.campaignOperation || (o.campaignBudgetOperation && o.campaignBudgetOperation.create)); });
  await checkWithGoogle(js, p, ap);
  step('duck approve'); const fox = clone(ctx.google.links.filter(l => l.assetGroup === RN('assetGroups', '9002'))), campaignsBefore = ctx.google.campaigns.size;
  const pub = js.blocked ? null : await approve(ap);
  js.gate('Duck join: Approve adds the product group to the existing paused campaign', () => pub && pub.status === 'APPLIED' && /joined/.test(pub.message || ''), () => why(pub));
  const recs = reqsIn('duck approve'), real = recs.filter(r => !r.validateOnly);
  js.check('Duck join: validated first, then one real request; no new campaign; the budget is raised in micros', () => recs.length === 2 && recs[0].validateOnly && real.length === 1 && ctx.google.campaigns.size === campaignsBefore && ctx.google.budgets.get(RN('campaignBudgets', '90010')).amountMicros === '30000000'
    && audit(real[0]).updates.length === 1 && audit(real[0]).updates[0].updateMask === 'amount_micros');
  const newGroup = () => [...ctx.google.assetGroups.values()].find(x => x.campaign === RN('campaigns', '9001') && x.id !== '9002');
  js.check('Duck join: the new group isolates the product, links its page, carries no brand links (brand guidelines), and the fox group is untouched', () => newGroup() && newGroup().finalUrls[0] === p.url && !isolationProblem(ctx.google.filters.filter(x => x.assetGroup === newGroup().resourceName), p)
    && !ctx.google.links.some(l => l.assetGroup === newGroup().resourceName && ['BUSINESS_NAME', 'LOGO', 'LANDSCAPE_LOGO'].includes(l.fieldType)) && JSON.stringify(ctx.google.links.filter(l => l.assetGroup === RN('assetGroups', '9002'))) === JSON.stringify(fox)
    && ctx.google.campaigns.get(RN('campaigns', '9001')).status === 'PAUSED' && !enabledProblems(recs).length);
  if (!js.blocked) verifyFilms(js, p, ap, newGroup().resourceName, RN('campaigns', '9001'), 'duck approve · adMotionPublication');
  // The ceiling at publication: the plan fit when prepared, then another campaign's budget grew.
  const g2 = P.gecko, ws2 = await seedWorkspace({ id: 'design_plans_gecko', products: [g2], selectedKey: 'gecko', scoped: true }), cs = stage(null, 'ceiling at publish');
  const ap2 = await submitAd(ws2, g2); if (!ap2) cs.blocked = 'submission failed'; else Object.assign(ap2, { fx: ws2.fx.gecko, wsId: ws2.id });
  await editMessaging(cs, ws2, g2, ap2, clone(COPY.gecko), false);
  await preparePlan(cs, g2, ap2, { styles: ['pmax'], budgets: { pmax: 20 }, countries: ['2840'], durations: { pmax: 30 } });
  ctx.google.budgets.get(RN('campaignBudgets', '91000')).amountMicros = '90000000';
  step('gecko approve'); const b2 = counts(), r2 = cs.blocked ? null : await approve(ap2);
  cs.check('Gecko: Approve over the daily ceiling (enabled budgets grew to 90) is refused; nothing is created', () => r2 && r2.ok === false && /Over your daily ceiling/.test(r2.error || '') && /Nothing was published/.test(r2.error || '') && counts() === b2 && !reqsIn('gecko approve').some(x => !x.validateOnly),
    () => why(r2));
  cs.check('Gecko: the ceiling is checked by the background publication: the request itself was queued and sent nothing', () => queuedOnly(r2), () => queuedDetail(r2));
  cs.check('Gecko: the refused ad returns to its Approval card (PENDING, plan kept) with the reason and is not left publishing', () => { const d = approval(ap2.id); return d.status === 'PENDING' && /Over your daily ceiling/.test(d.lastError || '') && d.pipelinePlan && d.pipelinePlan.hash === ap2.planHash && !d.pipelineReview && !d.applyAttempt && !d.needsReconciliation; },
    () => JSON.stringify(approval(ap2.id) && { status: approval(ap2.id).status, lastError: approval(ap2.id).lastError }));
  ctx.google.budgets.get(RN('campaignBudgets', '91000')).amountMicros = '70000000';
});

scenario('S3', 'S3 dry-run mode', async () => {
  await boot('S3 dry run', { dryRun: true });
  const p = P.gecko, ws = await seedWorkspace({ id: 'design_dry_gecko', products: [p], selectedKey: 'gecko', scoped: true }), st = stage(null, 'dry run');
  const ap = await submitAd(ws, p); if (!ap) st.blocked = 'submission failed'; else Object.assign(ap, { fx: ws.fx.gecko, wsId: ws.id });
  await reviewPreviews(st, ws, p, ap);
  await editMessaging(st, ws, p, ap, clone(COPY.gecko), false);
  const choice = { styles: ['pmax', 'responsive_display'], budgets: { pmax: 10, responsive_display: 5 }, countries: ['2840'], durations: { pmax: 30, responsive_display: 30 } };
  await preparePlan(st, p, ap, choice); await checkWithGoogle(st, p, ap);
  step('gecko approve'); const before = counts(), r = st.blocked ? null : await approve(ap), recs = reqsIn('gecko approve');
  st.gate('Gecko dry run: Approve reports validation only', () => r && r.status === 'VALIDATED' && r.dryRun === true && /dry-run mode kept them unpublished/.test(r.message || ''), () => why(r));
  st.check('Gecko dry run: exactly one request, validateOnly:true, accepted; nothing is created', () => recs.length === 1 && recs[0].validateOnly && recs[0].ok && counts() === before && !ctx.google.uploads.size, () => recs.map(x => x.validateOnly).join(','));
  st.check('Gecko dry run: the ad returns to Approval (PENDING) with its reviewed plan and the dry-run check recorded; no campaign IDs; no film upload starts', () => { const d = approval(ap.id); return d.status === 'PENDING' && Number(d.validatedAt) > 0 && d.dryRunCheck && d.dryRunCheck.planHash === ap.planHash && d.pipelinePlan && d.pipelinePlan.hash === ap.planHash && d.reviewHash === ap.reviewHash && !d.pipelineReview && !d.publishedCampaignIds && !callsIn('gecko approve').some(c => c.task === 'adMotionPublication') && !motionJob(ws.id, ws.fx.gecko).publication; },
    () => JSON.stringify(approval(ap.id) && { status: approval(ap.id).status, dryRunCheck: approval(ap.id).dryRunCheck, validatedAt: approval(ap.id).validatedAt }));
  st.check('Gecko dry run: queued by the request, validated by the worker; the outcome VALIDATED is saved on the approval (which is PENDING again) for its card', () => queuedOnly(r) && approval(ap.id).status === 'PENDING' && approval(ap.id).publishOutcome.status === 'VALIDATED' && approval(ap.id).publishOutcome.message === r.meta.worker.message, () => queuedDetail(r) + ' ' + JSON.stringify(approval(ap.id).publishOutcome));
  st.check('Gecko dry run: the response says the dry run only validated and the ad stays in Approval', () => /Nothing was published; the ad stays in Approval/.test(r.message || ''), () => why(r));
  // With dry run off again, the same reviewed ad is published from the same Approve ad button, with its films.
  ctx.store.docs.get('Brites_GAds_Control/control').dryRun = false;
  step('gecko approve live'); const live = st.blocked ? null : await approve(ap), liveRecs = reqsIn('gecko approve live');
  st.gate('Gecko: after a dry-run check, turning dry run off and approving the same reviewed plan publishes it', () => live && live.status === 'APPLIED' && approval(ap.id).status === 'APPLIED', () => why(live) + ' · approval status ' + (approval(ap.id) || {}).status);
  const a = st.blocked ? null : verifyPublication(st, p, ap, choice, liveRecs, COPY.gecko), real = liveRecs.find(x => !x.validateOnly), grp = real && a && real.idMap.get(a.groups[0].resourceName), camp = grp && ctx.google.assetGroups.get(grp).campaign;
  st.check('Gecko: the new Approve clears the dry run\'s saved outcome when it answers, so its card waits for this publication', () => live && live.meta && live.meta.outcomeAtAnswer === null && approval(ap.id).publishOutcome.status === 'APPLIED', () => JSON.stringify(live && live.meta && live.meta.outcomeAtAnswer));
  st.check('Gecko films: Approve after the dry run starts the reviewed film upload for the new group', () => /uploading to YouTube/.test(live.message || ''), () => live && live.message);
  if (!st.blocked) verifyFilms(st, p, ap, grp, camp, 'gecko approve live · adMotionPublication'); else ['three reviewed films upload', 'attachment is validated first', 'the group shows the three uploaded videos'].forEach(n => st.check(p.short + ' films: ' + n, () => false));
  step('gecko approve live again'); const again = st.blocked ? null : await approve(ap);
  st.check('Gecko: approving the published ad again is refused and sends nothing; Google holds exactly one campaign per style', () => again && again.ok === false && !reqsIn('gecko approve live again').length && [...ctx.google.campaigns.values()].filter(c => c.name.startsWith('Brites · ' + p.title) && c.status !== 'REMOVED').length === choice.styles.length, () => why(again));
});

scenario('S4', 'S4 idempotency and reconciliation', async () => {
  await boot('S4 idempotency');
  const ready = async (p, choice) => { const ws = await seedWorkspace({ id: 'design_idem_' + p.key, products: [p], selectedKey: p.key, scoped: true }), st = stage(null, p.short);
    const ap = await submitAd(ws, p); if (!ap) st.blocked = 'submission failed'; else Object.assign(ap, { fx: ws.fx[p.key], wsId: ws.id });
    await editMessaging(st, ws, p, ap, clone(COPY[p.key]), false); await preparePlan(st, p, ap, choice); return { ws, st, ap }; };
  const campaignsNamed = prefix => [...ctx.google.campaigns.values()].filter(c => c.name.startsWith(prefix) && c.status !== 'REMOVED');
  const pmaxOnly = budget => ({ styles: ['pmax'], budgets: { pmax: budget }, countries: ['2840'], durations: { pmax: 30 } });
  // a) Two Approve clicks at once.
  { const p = P.bunny, { ws, st, ap } = await ready(p, pmaxOnly(15));
    step('bunny approve twice'); const before = ctx.google.campaigns.size;
    const both = st.blocked ? [] : await Promise.all([1, 2].map(() => approve(ap)));
    const real = reqsIn('bunny approve twice').filter(r => !r.validateOnly && audit(r).campaigns.length);
    st.check('Bunny: two simultaneous Approve clicks publish once and refuse the other', () => both.filter(r => r.status === 'APPLIED').length === 1 && both.filter(r => r.ok === false).length === 1, () => both.map(why).join(' | '));
    st.check('Bunny: exactly one campaign-creating request and one new campaign', () => real.length === 1 && ctx.google.campaigns.size === before + 1 && campaignsNamed('Brites · ' + p.title).length === 1);
    step('bunny approve again'); const again = st.blocked ? null : await approve(ap);
    st.check('Bunny: approving the published review again is refused and sends nothing', () => again && again.ok === false && !reqsIn('bunny approve again').length && campaignsNamed('Brites · ' + p.title).length === 1, () => why(again));
    step('bunny apply again'); const apply = st.blocked ? null : await api('apply', { id: ap.id }), worker = callsIn('bunny apply again').find(c => c.task === 'publishApproval');
    st.check('Bunny: publishing the applied approval from its card again changes nothing ("already published")', () => !reqsIn('bunny apply again · publishApproval').length && approval(ap.id).status === 'APPLIED' && campaignsNamed('Brites · ' + p.title).length === 1 && /already published/.test(JSON.stringify(worker && worker.worker || '')),
      () => JSON.stringify(worker && worker.worker || apply).slice(0, 300));
    step('bunny resubmit'); const docsBefore = [...ctx.store.docs.keys()].filter(k => k.startsWith('Brites_GAds_Approvals/') && !k.slice(22).includes('/')).length;
    const re = st.blocked ? null : await api('prepareAdDesignPublication', { workspaceId: ws.id, target: 'ads', formats: ['landscape', 'portrait', 'square'], includeCopy: true, queueOnly: true, reviewCopy: clone(COPY[p.key]) });
    st.check('Bunny: sending the same complete ad to Approval again reports it as published and creates no second review', () => re && re.status === 'APPLIED' && /already been published/.test(re.message || '') && [...ctx.store.docs.keys()].filter(k => k.startsWith('Brites_GAds_Approvals/') && !k.slice(22).includes('/')).length === docsBefore, () => why(re)); }
  // b) Merchant eligibility, then a lost response that is reconciled as published.
  { const p = P.gecko, { st, ap } = await ready(p, pmaxOnly(10)), offer = ctx.google.products.find(x => x.itemId === p.offers[0]);
    Object.assign(offer, { status: 'NOT_ELIGIBLE', availability: 'OUT_OF_STOCK' });
    step('gecko approve ineligible'); const inel = st.blocked ? null : await approve(ap);
    st.check('Gecko: Approve is refused while its Merchant offer is not eligible; the review stays pending and nothing is sent', () => inel && inel.ok === false && /no longer eligible|none of this product.s \d+ Merchant Center offers? can serve/.test(inel.error || '') && /Nothing was created|no longer eligible/.test(inel.error || '') && approval(ap.id).status === 'PENDING' && !reqsIn('gecko approve ineligible').some(r => r.ops.length), () => why(inel));
    Object.assign(offer, { status: 'ELIGIBLE', availability: 'IN_STOCK' });
    ctx.google.faults.push({ kind: 'lost', when: rec => audit(rec).campaigns.length > 0 });
    step('gecko approve lost'); const lost = st.blocked ? null : await approve(ap);
    st.check('Gecko: a lost Google response is reported as unconfirmed, never as a failure to retry', () => lost && lost.ok === false && /could not be confirmed/.test(lost.error || ''), () => why(lost));
    st.check('Gecko: the unconfirmed result is saved for its card (publishOutcome APPLY_UNKNOWN with the could-not-be-confirmed message)', () => queuedOnly(lost) && approval(ap.id).publishOutcome && approval(ap.id).publishOutcome.status === 'APPLY_UNKNOWN' && /could not be confirmed/.test(approval(ap.id).publishOutcome.message), () => queuedDetail(lost) + ' ' + JSON.stringify(approval(ap.id).publishOutcome));
    st.check('Gecko: the approval is APPLY_UNKNOWN and needs reconciliation; Google holds exactly one campaign', () => { const d = approval(ap.id); return d.status === 'APPLY_UNKNOWN' && d.needsReconciliation === true && campaignsNamed('Brites · ' + p.title).length === 1; },
      () => JSON.stringify({ status: (approval(ap.id) || {}).status, n: campaignsNamed('Brites · ' + p.title).length }));
    step('gecko apply unknown'); await api('apply', { id: ap.id });
    st.check('Gecko: publishing an unconfirmed approval again is refused and sends nothing', () => !reqsIn('gecko apply unknown · publishApproval').length && approval(ap.id).status === 'APPLY_UNKNOWN' && campaignsNamed('Brites · ' + p.title).length === 1);
    step('gecko reconcile'); const rec = await api('reconcileApproval', { id: ap.id, outcome: 'published' });
    st.check('Gecko: reconciling as published records APPLIED without sending anything; still one campaign', () => rec.status === 'APPLIED' && approval(ap.id).status === 'APPLIED' && !reqsIn('gecko reconcile').length && campaignsNamed('Brites · ' + p.title).length === 1, () => why(rec)); }
  // c) A partial failure: nothing was created; reconciled as not published, then published once.
  { const p = P.duck, { st, ap } = await ready(p, pmaxOnly(12));
    ctx.google.faults.push({ kind: 'partial', when: rec => audit(rec).campaigns.length > 0 });
    step('duck approve partial'); const part = st.blocked ? null : await approve(ap);
    st.check('Duck: a partial-failure response is unconfirmed (APPLY_UNKNOWN) and nothing was created', () => part && part.ok === false && approval(ap.id).status === 'APPLY_UNKNOWN' && campaignsNamed('Brites · ' + p.title).length === 0, () => why(part));
    step('duck reconcile'); await api('reconcileApproval', { id: ap.id, outcome: 'not_published' });
    st.check('Duck: reconciling as not published returns the ad to its Approval card (PENDING, plan kept) with the reason', () => { const d = approval(ap.id); return d.status === 'PENDING' && /not published/.test(d.lastError || '') && d.pipelinePlan && d.pipelinePlan.hash === ap.planHash && !d.pipelineReview && !d.needsReconciliation; },
      () => JSON.stringify(approval(ap.id) && { status: approval(ap.id).status, lastError: approval(ap.id).lastError }));
    step('duck approve again'); const again = st.blocked ? null : await approve(ap);
    st.check('Duck: Approve ad publishes it again: exactly one PAUSED campaign (validated first), with its films', () => { const r = reqsIn('duck approve again'); return again && again.status === 'APPLIED' && approval(ap.id).status === 'APPLIED' && r.length === 2 && r[0].validateOnly && campaignsNamed('Brites · ' + p.title).length === 1 && campaignsNamed('Brites · ' + p.title)[0].status === 'PAUSED' && /uploading to YouTube/.test(again.message || ''); },
      () => why(again));
    step('duck apply again'); await api('apply', { id: ap.id });
    st.check('Duck: a further publish request creates nothing', () => !reqsIn('duck apply again · publishApproval').length && campaignsNamed('Brites · ' + p.title).length === 1); }
  // d) The duplicate-name guard, then a definite Google rejection.
  { const p = P.saturn, { st, ap } = await ready(p, pmaxOnly(9)), name = !st.blocked && approval(ap.id).pipelinePlan.summary.campaigns[0].name;
    if (name) ctx.google.campaigns.set(RN('campaigns', '9500'), { resourceName: RN('campaigns', '9500'), id: '9500', name, status: 'PAUSED', advertisingChannelType: 'PERFORMANCE_MAX', campaignBudget: RN('campaignBudgets', '91000') });
    step('saturn approve duplicate'); const dup = st.blocked ? null : await approve(ap);
    st.check('Saturn: a campaign with the planned name already in Google refuses publication, sends nothing and returns the ad to its card', () => dup && dup.ok === false && /already exists/.test(dup.error || '') && !reqsIn('saturn approve duplicate').length && approval(ap.id).status === 'PENDING', () => why(dup));
    if (name) ctx.google.campaigns.get(RN('campaigns', '9500')).status = 'REMOVED';
    ctx.google.faults.push({ kind: 'definite', when: rec => audit(rec).campaigns.length > 0 });
    step('saturn approve rejected'); const rejFrom = Date.now(), rej = st.blocked ? null : await approve(ap);
    st.check('Saturn: a definite Google rejection returns the ad to its Approval card (PENDING) with the error (no reconciliation needed)', () => { const d = approval(ap.id); return rej && rej.ok === false && d.status === 'PENDING' && !d.needsReconciliation && /mutate failed/.test(d.lastError || '') && campaignsNamed('Brites · ' + p.title).filter(c => c.id !== '9500').length === 0; },
      () => JSON.stringify({ status: (approval(ap.id) || {}).status, lastError: ((approval(ap.id) || {}).lastError || '').slice(0, 200) }));
    st.check('Saturn: the rejected Approve saves when it failed (lastErrorAt), so its card shows that time', () => { const t = approval(ap.id).lastErrorAt; return Number.isFinite(t) && t >= rejFrom && t <= Date.now(); }, () => JSON.stringify({ lastErrorAt: approval(ap.id).lastErrorAt, rejFrom }));
    st.check('Saturn: Google refused the background publication: the request was queued and sent nothing; the refusal came back on its card, newer than the request, with no outcome saved as published', () => queuedOnly(rej) && approval(ap.id).lastErrorAt >= rej.meta.request.requestedAt && !(approval(ap.id).publishOutcome && approval(ap.id).publishOutcome.at >= rej.meta.request.requestedAt), () => queuedDetail(rej));
    // The hand-off to the worker fails: nothing was sent, the ad returns to its card with the reason, and the request says so.
    ctx.dispatchFail = 'publishSubmission'; step('saturn approve not started'); const nsFrom = Date.now(), ns = st.blocked ? null : await approve(ap); ctx.dispatchFail = null;
    st.check('Saturn: when the background publication cannot start, the request answers with the reason, nothing is sent, and the ad is back on its card (PENDING, plan kept) with lastError and lastErrorAt', () => { const d = approval(ap.id);
      return ns && ns.ok === false && /Publishing could not start \(Background dispatch failed: HTTP 500\)\. Nothing was sent to Google\. Approve ad again\./.test(ns.error || '') && d.status === 'PENDING' && d.lastError === ns.error && d.lastErrorAt >= nsFrom && !d.pipelineReview && !d.publishRequestedAt && d.pipelinePlan && d.pipelinePlan.hash === ap.planHash
        && !reqsIn('saturn approve not started').length && callsIn('saturn approve not started').every(c => !c.started); }, () => why(ns) + ' ' + JSON.stringify(approval(ap.id) && { status: approval(ap.id).status, lastError: approval(ap.id).lastError }));
    step('saturn approve'); const ok = st.blocked ? null : await approve(ap);
    st.check('Saturn: Approve ad after the rejection creates exactly one PAUSED campaign', () => ok && ok.status === 'APPLIED' && approval(ap.id).status === 'APPLIED' && campaignsNamed('Brites · ' + p.title).filter(c => c.id !== '9500').length === 1, () => why(ok));
    st.check('Saturn: the new Approve clears the saved reason and its time', () => approval(ap.id).lastError === null && approval(ap.id).lastErrorAt === null, () => JSON.stringify({ lastError: approval(ap.id).lastError, lastErrorAt: approval(ap.id).lastErrorAt })); }
});

scenario('S5', 'S5 films refused unless the destination isolates the product', async () => {
  await boot('S5 film isolation');
  const p = P.saturn, ws = await seedWorkspace({ id: 'design_films_saturn', products: [p], selectedKey: 'saturn', scoped: true }), st = stage(null, 'films');
  const ap = await submitAd(ws, p); if (!ap) st.blocked = 'submission failed'; else Object.assign(ap, { fx: ws.fx.saturn, wsId: ws.id });
  await editMessaging(st, ws, p, ap, clone(COPY.saturn), false);
  await preparePlan(st, p, ap, { styles: ['pmax'], budgets: { pmax: 10 }, countries: ['2840'], durations: { pmax: 30 } });
  // Before the film worker runs, another product's offer enters the new group's product filter.
  let groupRn = null, campaignRn = null;
  ctx.beforeWorker = body => { if ((body.tasks || [])[0] !== 'adMotionPublication' || groupRn) return;
    const g = ctx.google, rec = g.requests.filter(r => r.ok && !r.validateOnly && audit(r).groups.length).pop(); groupRn = rec.idMap.get(audit(rec).groups[0].resourceName); campaignRn = g.assetGroups.get(groupRn).campaign;
    const root = g.filters.find(f => f.assetGroup === groupRn && f.type === 'SUBDIVISION');
    g.filters.push({ resourceName: RN('assetGroupListingGroupFilters', groupRn.split('/').pop() + '~' + g.id()), assetGroup: groupRn, parentListingGroupFilter: root.resourceName, type: 'UNIT_INCLUDED', listingSource: 'SHOPPING', caseValue: { productItemId: { value: P.gecko.offers[0] } } }); };
  step('saturn approve'); const r = st.blocked ? null : await approve(ap);
  st.gate('Saturn: the ad publishes (APPLIED, paused)', () => r && r.status === 'APPLIED', () => why(r)); ctx.beforeWorker = null;
  const job = () => motionJob(ws.id, ws.fx.saturn), noVideos = () => !ctx.google.links.some(l => l.assetGroup === groupRn && l.fieldType === 'YOUTUBE_VIDEO');
  st.check('Saturn films: refused when the group\'s product filter includes another product; nothing uploaded or attached; free to reset', () => job().publication.phase === 'blocked' && /isolate this exact product/.test(job().publication.error || '') && job().publication.resettable === true && !ctx.google.uploads.size && noVideos(),
    () => JSON.stringify(job().publication).slice(0, 300));
  const hash = () => require(path.join(FN, 'googleAdsMotionPublication.js')).reviewHash(job()), start = s => { step(s); return api('startAdMotionPublication', { workspaceId: ws.id, productId: p.gid, groupRef: GROUP.ref, jobId: ws.fx.saturn.motionJobId, reviewHash: hash() }); };
  if (!st.blocked) ctx.google.filters = ctx.google.filters.filter(f => !(f.assetGroup === groupRn && f.caseValue && f.caseValue.productItemId && f.caseValue.productItemId.value === P.gecko.offers[0]));
  if (!st.blocked) ctx.google.campaigns.get(campaignRn).status = 'ENABLED';
  const enabled = st.blocked ? null : await start('saturn films enabled');
  st.check('Saturn films: refused while the campaign is not paused; nothing uploaded or attached', () => enabled && enabled.ok === false && /paused campaign/.test(enabled.error || '') && !ctx.google.uploads.size && noVideos(), () => why(enabled));
  if (!st.blocked) { ctx.google.campaigns.get(campaignRn).status = 'PAUSED'; ctx.google.assetGroups.get(groupRn).finalUrls = [GROUP.url]; }
  const moved = st.blocked ? null : await start('saturn films destination');
  st.check('Saturn films: refused when the group no longer links the exact product page', () => moved && moved.ok === false && /exact product destination/.test(moved.error || '') && !ctx.google.uploads.size && noVideos(), () => why(moved));
  if (!st.blocked) ctx.google.assetGroups.get(groupRn).finalUrls = [p.url];
  const ok = st.blocked ? null : await start('saturn films');
  st.gate('Saturn films: once the paused group isolates the product again, the reset publication runs', () => ok && ok.ok === true && ok.queued === true, () => why(ok));
  if (!st.blocked) verifyFilms(st, p, ap, groupRn, campaignRn, 'saturn films · adMotionPublication');
});

scenario('S6', 'S6 four products in one workspace (the four waiting ads)', async () => {
  await boot('S6 one workspace');
  // Gecko is selected first; Duck, Bunny and Saturn are remembered designs. Each is sent to Approval while
  // selected, switching with the real saveAdDesign action, which ends with Duck selected.
  const ws = await seedWorkspace({ id: 'design_shared_animal_charms', products: PRODUCTS, selectedKey: 'gecko' }), aps = {};
  const order = ['gecko', 'bunny', 'saturn', 'duck'];
  for (const [i, key] of order.entries()) {
    const p = P[key];
    if (i) { step('switch to ' + key); const s = await api('saveAdDesign', { workspaceId: ws.id, productId: p.gid, groupRef: GROUP.ref }), w = ctx.store.docs.get(wsPath(ws.id));
      check(w.settings.productId === p.gid && w.job && w.job.id === ws.fx[key].job.id && JSON.stringify(w.messaging.copy) === JSON.stringify(COPY[key]), 'switching the workspace to ' + p.short + ' restores its remembered design, job and messaging (saveAdDesign)',
        { detail: (s.ok === false ? 'status after save: ' + why(s) + ' · ' : '') + JSON.stringify({ productId: w.settings.productId, job: w.job && w.job.id }) }); }
    const ap = await submitAd(ws, p); if (ap) aps[key] = Object.assign(ap, { fx: ws.fx[key], wsId: ws.id });
  }
  const w = ctx.store.docs.get(wsPath(ws.id));
  check(w.settings.productId === P.duck.gid && ['gecko', 'bunny', 'saturn'].every(k => { const d = w.productDesigns[sha([GROUP.ref, P[k].gid]).slice(0, 32)]; return d && d.jobId === ws.fx[k].job.id && ctx.store.docs.has(wsPath(ws.id) + '/history/' + ws.fx[k].job.id); }),
    'Duck is selected; Gecko, Bunny and Saturn are remembered in productDesigns with their jobs in the workspace history');
  const plans = {
    duck: { styles: ['pmax', 'responsive_display'], budgets: { pmax: 12, responsive_display: 6 }, countries: ['2840', '2124'], durations: { pmax: 30, responsive_display: 14 } },
    gecko: { styles: ['pmax', 'responsive_display', 'fixed_display'], budgets: { pmax: 10, responsive_display: 5, fixed_display: 5 }, countries: ['2124'], durations: { pmax: 45, responsive_display: 30, fixed_display: 7 } },
    bunny: { styles: ['pmax'], budgets: { pmax: 15 }, countries: ['2840'], durations: { pmax: 0 } },
    saturn: { styles: ['pmax', 'fixed_display'], budgets: { pmax: 10, fixed_display: 5 }, countries: ['2840', '2124'], durations: { pmax: 30, fixed_display: 10 } } };
  // Frozen saved-package snapshots: after Duck was sent, the studio saves new Duck messaging and a new landscape crop.
  step('duck studio edit');
  const studio = await api('saveAdDesignCopy', { workspaceId: ws.id, copy: clone(EDITED_DUCK), expectedRevision: ctx.store.docs.get(wsPath(ws.id)).revision });
  const recrop = await jpeg('duck-recrop', 1200, 628, '#c9a227'), recropHash = ctx.E.creativeHash(recrop.toString('base64')), recropPath = `Brites_GAds_Creative/${ws.id}/crop_duck_landscape_v2-${recropHash}.jpg`;
  ctx.store.files.set(recropPath, recrop);
  const recropped = clone(ctx.store.docs.get(wsPath(ws.id)).placements).map(pl => pl.productId === P.duck.gid && pl.format === 'landscape' ? { ...pl, imageId: 'crop_duck_landscape_v2', asset: { path: recropPath, hash: recropHash, bytes: recrop.length, width: 1200, height: 628, kind: 'crop', mimeType: 'image/jpeg' } } : pl);
  await ctx.store.db.doc(wsPath(ws.id)).update({ placements: recropped, revision: ctx.store.docs.get(wsPath(ws.id)).revision + 1, updatedAt: Date.now() });
  { const now = ctx.store.docs.get(wsPath(ws.id));
    check(JSON.stringify(now.messaging.copy) === JSON.stringify(EDITED_DUCK) && now.placements.some(pl => pl.asset && pl.asset.hash === recropHash) && now.settings.productId === P.duck.gid,
      'a later studio edit of Duck (new messaging saved, landscape re-cropped) is stored in the workspace', { detail: why(studio) }); }
  const stages = {};
  for (const key of ['duck', 'gecko', 'bunny', 'saturn']) { const tag = key === 'duck' ? null : 'scope'; stages[key] = stage(tag, P[key].short); if (!aps[key]) stages[key].blocked = 'submission failed'; }
  // Review previews of all four while Duck is selected.
  for (const key of ['duck', 'gecko', 'bunny', 'saturn']) { const r = await reviewPreviews(stages[key], ws, P[key], aps[key]);
    stages[key].check(P[key].short + ': the review is a current frozen snapshot (not stale) after later studio changes', () => r.sourceStale === false && approval(aps[key].id).designReview.sourceMode === 'saved-package', () => 'sourceStale=' + r.sourceStale); }
  for (const key of ['duck', 'gecko', 'bunny', 'saturn']) {
    const p = P[key], st = stages[key], ap = aps[key], selectedBefore = JSON.stringify(ctx.store.docs.get(wsPath(ws.id)).settings);
    await editMessaging(st, ws, p, ap, clone(COPY[key]), false);
    await publishAd(st, ws, p, ap, plans[key], COPY[key]);
    check(JSON.stringify(ctx.store.docs.get(wsPath(ws.id)).settings) === selectedBefore, 'publishing ' + p.short + ' leaves the workspace selection (Duck) unchanged');
  }
  const sentAssets = () => ctx.google.requests.flatMap(r => [...r.assetInfo.values()]);
  check(!sentAssets().some(i => i.contentHash === recropHash) && !sentAssets().some(i => i.kind === 'text' && EDITED_DUCK.headlines.concat(EDITED_DUCK.descriptions).includes(i.text) && !COPY.duck.headlines.concat(COPY.duck.descriptions).includes(i.text)),
    'Duck publishes the photos and messaging it was sent with, not the later studio crop or messaging');
  check([...ctx.google.campaigns.values()].filter(c => /^Brites · (Duck|Gecko|Bunny|Planet Saturn)/.test(c.name)).every(c => c.status === 'PAUSED'), 'no campaign created in this workspace is ENABLED');
});

scenario('S7', 'S7 the four waiting ads as legacy reviews (sent before the snapshot fix)', async () => {
  await boot('S7 legacy reviews');
  const D = require(path.join(FN, 'googleAdsAdDesign.js')), H = ctx.E.creativeHash, SR = require(path.join(FN, 'googleAdsSubmissionReview.js'));
  // The selection hash the code before 19635c8 stored as sourceHash (its _adDesignSelectionHash of the workspace when sent).
  const legacySourceHash = w => H({ productId: w.settings.productId, groupRef: w.settings.groupRef, placements: D.chosenPlacements(w), messaging: w.messaging || null, jobId: w.job && w.job.id || null, result: w.job && w.job.result || null });
  const ws = await seedWorkspace({ id: 'design_legacy_animal_charms', products: PRODUCTS, selectedKey: 'gecko' }), aps = {}, legacy = {};
  const col = ctx.store.db.collection('Brites_GAds_Approvals');
  for (const [i, key] of ['gecko', 'bunny', 'saturn', 'duck'].entries()) {
    const p = P[key];
    if (i) { step('switch to ' + key); await api('saveAdDesign', { workspaceId: ws.id, productId: p.gid, groupRef: GROUP.ref }); }
    const atSend = clone(ctx.store.docs.get(wsPath(ws.id))), sent = await submitAd(ws, p);
    if (!sent) continue;
    // Rewrite the review exactly as the code before 19635c8 wrote it: the same designReview without sourceSetId,
    // pinned publicationImages, artworkCopy and sourceMode; sourceHash = the selection hash; id from that reviewHash.
    const doc = ctx.store.docs.get('Brites_GAds_Approvals/' + sent.id), r = clone(doc.designReview);
    for (const k of ['sourceSetId', 'publicationImages', 'artworkCopy', 'sourceMode']) delete r[k];
    const sourceHash = legacySourceHash(atSend), reviewHash = H({ sourceHash, designReview: r }), id = 'design-review-' + reviewHash.slice(0, 32);
    let item = { ...doc, sourceHash, reviewHash, designReview: r };
    // Gecko's messaging was edited in Approvals before the upgrade, while Gecko was still selected (the old update()).
    if (key === 'gecko') { const edited = { ...r, copy: clone(EDITED_GECKO), includeVideos: true, copyEdited: H(EDITED_GECKO) !== H(atSend.messaging.copy) };
      item = { ...item, designReview: edited, reviewHash: H({ sourceHash, designReview: edited }), pipelinePlan: null, pipelineReview: null, vetted: false, updatedAt: Date.now() }; }
    await col.doc(sent.id).delete(); await col.doc(id).set(item);
    aps[key] = { id, reviewHash: item.reviewHash, fx: ws.fx[key], wsId: ws.id }; legacy[key] = { sourceHash, reviewHash: item.reviewHash };
  }
  const w = ctx.store.docs.get(wsPath(ws.id));
  check(w.settings.productId === P.duck.gid && Object.keys(aps).length === 4 && Object.values(aps).every(a => { const d = approval(a.id); return d.status === 'PENDING' && !d.designReview.sourceMode && !d.designReview.sourceSetId && !d.designReview.publicationImages; })
    && approval(aps.gecko.id).designReview.copyEdited === true, 'four legacy reviews wait in Approvals (no sourceMode, sourceSetId or pinned photos); Duck is selected; Gecko carries an Approvals edit');
  const plans = {
    duck: { styles: ['pmax', 'responsive_display'], budgets: { pmax: 12, responsive_display: 6 }, countries: ['2840', '2124'], durations: { pmax: 30, responsive_display: 14 } },
    gecko: { styles: ['pmax', 'responsive_display'], budgets: { pmax: 10, responsive_display: 5 }, countries: ['2124'], durations: { pmax: 45, responsive_display: 30 } },
    bunny: { styles: ['pmax', 'fixed_display'], budgets: { pmax: 15, fixed_display: 5 }, countries: ['2840'], durations: { pmax: 0, fixed_display: 7 } },
    saturn: { styles: ['pmax', 'responsive_display', 'fixed_display'], budgets: { pmax: 10, responsive_display: 5, fixed_display: 5 }, countries: ['2840', '2124'], durations: { pmax: 30, responsive_display: 10, fixed_display: 10 } } };
  for (const key of ['duck', 'gecko', 'bunny', 'saturn']) {
    const p = P[key], ap = aps[key], f = ws.fx[key], st = stage(null, p.short + ' legacy'), selected = key === 'duck', copy = key === 'gecko' ? EDITED_GECKO : COPY[key];
    if (!ap) st.blocked = 'submission failed';
    const before = ap && clone(approval(ap.id)), r = await reviewPreviews(st, ws, p, ap, 'legacy review');
    // A remembered product's older review reads its own scope (its saved settings, messaging and image job), so an
    // unchanged review is as current as the selected product's.
    st.check(p.short + ' legacy: opening the review does not rewrite it; it reports a current source (unchanged since it was sent' + (selected ? ')' : ', read from its own remembered scope)'),
      () => r.sourceStale === false && JSON.stringify(approval(ap.id)) === JSON.stringify(before), () => 'sourceStale=' + r.sourceStale);
    const converted = () => { const d = approval(ap.id), rv = d.designReview;
      return rv.sourceMode === 'saved-package' && d.sourceHash === SR.snapshotHash(rv, H) && d.reviewHash === ap.reviewHash && d.reviewHash !== legacy[key].reviewHash && d.sourceRefresh && d.sourceRefresh.previousSourceHash === legacy[key].sourceHash
        && (rv.publicationImages || []).length === 3 && Object.entries(f.images).every(([format, a]) => rv.publicationImages.some(x => x.format === format && x.asset.hash === a.hash)) && rv.productId === p.gid && JSON.stringify(rv.copy) === JSON.stringify(copy) && rv.layoutReview && rv.layoutReview.jobId === f.editorJobId; },
      convertedDetail = () => { const d = approval(ap.id) || {}, rv = d.designReview || {}; return JSON.stringify({ sourceMode: rv.sourceMode, images: (rv.publicationImages || []).map(x => [x.format, x.asset.hash.slice(0, 10)]), own: Object.entries(f.images).map(([k, a]) => [k, a.hash.slice(0, 10)]), refresh: d.sourceRefresh, layout: rv.layoutReview }); };
    // Bunny and Saturn: a messaging save before any plan converts the older review, exactly as preparing does.
    if (!selected && key !== 'gecko') { await editMessaging(st, ws, p, ap, clone(copy), false);
      st.check(p.short + ' legacy: saving its messaging before the plan converts it in place to a frozen saved package of its own photos (sourceMode, snapshot sourceHash, refresh history)', converted, convertedDetail); }
    // Gecko carries an Approvals edit from before the upgrade: converting it keeps that edit marked against its own
    // saved artwork text, so Fixed Display (whose artwork shows the original text) stays refused.
    if (key === 'gecko') { step('gecko legacy fixed after edit');
      const fixed = st.blocked ? null : await api('publishAdDesignSubmission', { id: ap.id, hash: ap.reviewHash, prepareOnly: true, styles: ['fixed_display'], budgets: { fixed_display: 5 }, countries: ['2840'], durations: { fixed_display: 7 } });
      if (fixed && fixed.reviewHash) ap.reviewHash = fixed.reviewHash;
      const d = () => (approval(ap.id) || { designReview: {} }).designReview;
      st.check('Gecko legacy: converting the review keeps its Approvals edit marked (copyEdited), so Fixed Display stays refused', () => fixed && fixed.ok === false && /edited messaging differs/.test(fixed.error || ''),
        () => why(fixed) + ' · after conversion copyEdited=' + d().copyEdited + ', artworkCopy is ' + (JSON.stringify((d().artworkCopy || {}).headlines) === JSON.stringify(EDITED_GECKO.headlines) ? 'the edited copy' : 'the studio copy')); }
    await preparePlan(st, p, ap, plans[key], 'legacy convert');
    if (selected || key === 'gecko') st.check(p.short + ' legacy: preparing converts it in place to a frozen saved package of its own photos (sourceMode, snapshot sourceHash, refresh history)', converted, convertedDetail);
    if (key === 'gecko') st.check('Gecko legacy: the converted review keeps its Approvals edit (copyEdited) against its own saved artwork text, never the edited copy', () => { const rv = approval(ap.id).designReview; return rv.copyEdited === true && rv.artworkCopy && JSON.stringify(rv.artworkCopy) !== JSON.stringify(EDITED_GECKO); },
      () => JSON.stringify({ copyEdited: approval(ap.id).designReview.copyEdited, artworkCopy: (approval(ap.id).designReview.artworkCopy || {}).headlines }));
    if (selected) await editMessaging(st, ws, p, ap, clone(copy), false);
    await reviewPreviews(st, ws, p, ap, 'converted review');
    const selectedBefore = JSON.stringify(ctx.store.docs.get(wsPath(ws.id)).settings);
    await publishAd(st, ws, p, ap, plans[key], copy);
    check(JSON.stringify(ctx.store.docs.get(wsPath(ws.id)).settings) === selectedBefore, 'publishing legacy ' + p.short + ' leaves the workspace selection (Duck) unchanged');
  }
  const mine = [...ctx.google.campaigns.values()].filter(c => /^Brites · (Duck|Gecko|Bunny|Planet Saturn)/.test(c.name));
  check(mine.length === 9 && mine.every(c => c.status === 'PAUSED'), 'the four legacy ads publish nine PAUSED campaigns (Duck 2, Gecko 2, Bunny 2, Saturn 3)', { detail: JSON.stringify(mine.map(c => [c.name, c.status])) });
});

scenario('S8', 'S8 adversity: interrupted approvals, generic publish, two tabs, no videos, passcode, dry-run join', async () => {
  await boot('S8 adversity');
  // a) An Approve interrupted before anything is sent returns the ad to its own card, never to a generic "Publish now".
  const p = P.duck, ws = await seedWorkspace({ id: 'design_adverse_duck', products: [p], selectedKey: 'duck', scoped: true }), st = stage(null, 'interrupted');
  const ap = await submitAd(ws, p); if (!ap) st.blocked = 'submission failed'; else Object.assign(ap, { fx: ws.fx.duck, wsId: ws.id });
  await editMessaging(st, ws, p, ap, clone(COPY.duck), false);
  const choice = { styles: ['pmax', 'responsive_display'], budgets: { pmax: 12, responsive_display: 6 }, countries: ['2840', '2124'], durations: { pmax: 30, responsive_display: 14 } };
  await preparePlan(st, p, ap, choice); await checkWithGoogle(st, p, ap);
  const onCard = (name, re) => st.check(name, () => { const d = approval(ap.id); return d.status === 'PENDING' && re.test(d.lastError || '') && d.pipelinePlan && d.pipelinePlan.hash === ap.planHash && d.reviewHash === ap.reviewHash && !d.pipelineReview && !d.applyAttempt && !d.needsReconciliation; },
    () => JSON.stringify(approval(ap.id) && { status: approval(ap.id).status, lastError: approval(ap.id).lastError, pipelineReview: approval(ap.id).pipelineReview, plan: (approval(ap.id).pipelinePlan || {}).hash === ap.planHash }));
  // Another publication holds the lease past the worker's eight-minute wait (the waits pass at once here).
  ctx.store.docs.set('Brites_GAds_State/publicationLease', { owner: 'another-approval', until: Date.now() + 3600000 });
  step('duck approve busy'); const busyFrom = Date.now(), busy = st.blocked ? null : await fastWaits(() => approve(ap));
  st.check('Duck: the busy Approve was queued: the worker waited its turn (eight minutes), then gave up without sending anything', () => queuedOnly(busy) && Date.now() - busyFrom >= 8 * 60000, () => queuedDetail(busy));
  st.check('Duck: Approve while another publication runs is refused and sends nothing', () => busy && busy.ok === false && /Another publication is still running/.test(busy.error || '') && !reqsIn('duck approve busy').length, () => why(busy));
  onCard('Duck: the interrupted ad returns to its Approval card (PENDING) with its prepared plan and the reason, so Approve ad publishes it with its Merchant check and films', /Another publication is still running/);
  st.check('Duck: the interrupted Approve saves when it failed (lastErrorAt), so its card shows that time', () => { const t = approval(ap.id).lastErrorAt; return Number.isFinite(t) && t >= busyFrom && t <= Date.now(); }, () => JSON.stringify({ lastErrorAt: approval(ap.id).lastErrorAt, busyFrom }));
  ctx.store.docs.delete('Brites_GAds_State/publicationLease');
  step('duck approve generic'); const gen = st.blocked ? null : await api('approve', { id: ap.id });
  st.check('Duck: the generic approve action cannot publish a complete ad; nothing is queued or sent and it stays PENDING', () => gen && !!gen.error && !callsIn('duck approve generic').some(c => c.task === 'publishApproval') && !reqsIn('duck approve generic').length && approval(ap.id).status === 'PENDING', () => why(gen));
  // A complete ad left Approved (by an earlier version, or a race) must not publish from the generic card: that route skips the Merchant check and the films.
  if (!st.blocked) Object.assign(approval(ap.id), { status: 'APPROVED', pipelineReview: { hash: ap.planHash, at: Date.now() }, lastError: 'Over your daily ceiling (an earlier attempt).', lastErrorAt: Date.now() - 3600000 });
  ctx.google.products.filter(x => p.offers.includes(x.itemId)).forEach(x => Object.assign(x, { status: 'NOT_ELIGIBLE', availability: 'OUT_OF_STOCK' }));
  step('duck publish again generic'); const againFrom = Date.now(), again = st.blocked ? null : await api('apply', { id: ap.id }), worker = callsIn('duck publish again generic').find(c => c.task === 'publishApproval');
  st.check('Duck: "Publish again" on a complete ad left Approved sends nothing to Google (that route skips the Merchant check and the films)', () => !reqsIn('duck publish again generic · publishApproval').length && !ctx.google.uploads.size && ![...ctx.google.campaigns.values()].some(c => c.name.startsWith('Brites · ' + p.title)),
    () => JSON.stringify(worker && worker.worker || again).slice(0, 300));
  onCard('Duck: it returns to its Approval card with its prepared plan, saying to approve it there', /Approve ad/);
  st.check('Duck: that reason is saved with its own time, not the earlier attempt\'s', () => approval(ap.id).lastErrorAt >= againFrom, () => JSON.stringify({ lastErrorAt: approval(ap.id).lastErrorAt, againFrom }));
  step('duck approve ineligible'); const inel = st.blocked ? null : await approve(ap);
  st.check('Duck: from its card, Approve is refused while none of its Merchant offers can serve; nothing is sent', () => inel && inel.ok === false && /none of this product.s 2 Merchant Center offers can serve/.test(inel.error || '') && !reqsIn('duck approve ineligible').length && approval(ap.id).status === 'PENDING', () => why(inel));
  // Merchant cannot be read at all: the final guard fails closed, nothing is sent, and the ad stays on its card.
  ctx.google.products.filter(x => p.offers.includes(x.itemId)).forEach(x => Object.assign(x, { status: 'ELIGIBLE', availability: 'IN_STOCK' }));
  ctx.google.searchFault = /shopping_product/;
  step('duck approve merchant down'); const down = st.blocked ? null : await approve(ap);
  st.check('Duck: a Merchant read failure at Approve refuses publication; nothing is sent and the ad stays PENDING', () => down && down.ok === false && !reqsIn('duck approve merchant down').length && approval(ap.id).status === 'PENDING', () => why(down));
  ctx.google.searchFault = null;
  // One of its two offers is out of stock: Performance Max serves the other, so Approve publishes.
  Object.assign(ctx.google.products.find(x => x.itemId === p.offers[0]), { status: 'NOT_ELIGIBLE', availability: 'OUT_OF_STOCK' });
  step('duck approve'); const before = new Set(ctx.google.campaigns.keys()), pub = st.blocked ? null : await approve(ap);
  st.gate('Duck: Approve from the same card publishes the same prepared plan once the earlier publication finished (one of two offers eligible)', () => pub && pub.status === 'APPLIED' && approval(ap.id).status === 'APPLIED' && !approval(ap.id).lastError, () => why(pub) + ' · ' + JSON.stringify(approval(ap.id) && { status: approval(ap.id).status, lastError: approval(ap.id).lastError }));
  const recs = reqsIn('duck approve'), a = st.blocked ? null : verifyPublication(st, p, ap, choice, recs, COPY.duck);
  const real = recs.find(x => !x.validateOnly), grp = real && a && real.idMap.get(a.groups[0].resourceName), camp = grp && ctx.google.assetGroups.get(grp).campaign;
  if (!st.blocked) verifyFilms(st, p, ap, grp, camp, 'duck approve · adMotionPublication'); else ['three reviewed films upload', 'attachment is validated first', 'the group shows the three uploaded videos'].forEach(n => st.check(p.short + ' films: ' + n, () => false));
  st.check('Duck: Google holds exactly the planned new campaigns', () => [...ctx.google.campaigns.keys()].filter(k => !before.has(k)).length === 2 && [...ctx.google.campaigns.values()].filter(c => c.name.startsWith('Brites · ' + p.title)).length === 2);
  Object.assign(ctx.google.products.find(x => x.itemId === p.offers[0]), { status: 'ELIGIBLE', availability: 'IN_STOCK' });

  // b) Two browser tabs on one review; videos excluded.
  const g = P.gecko, ws2 = await seedWorkspace({ id: 'design_adverse_gecko', products: [g], selectedKey: 'gecko', scoped: true }), tb = stage(null, 'two tabs');
  const tab = await submitAd(ws2, g); if (!tab) tb.blocked = 'submission failed'; else Object.assign(tab, { fx: ws2.fx.gecko, wsId: ws2.id });
  const h0 = tab && tab.reviewHash, editA = clone(EDITED_GECKO), editB = { ...clone(COPY.gecko), headlines: COPY.gecko.headlines.map(h => h === 'Shop Gecko Jewelry' ? 'Gecko Jewelry Gift' : h) };
  step('gecko two tabs edit'); const both = tb.blocked ? [] : await Promise.all([editA, editB].map((copy, i) => api('updateAdDesignSubmission', { id: tab.id, hash: h0, copy, includeVideos: i === 0 })));
  tb.check('Gecko: two tabs saving the same review at once: one save wins, the other is refused (never merged or overwritten silently)', () => both.filter(r => r.ok === true).length === 1 && both.filter(r => r.ok === false && /changed/.test(r.error || '')).length === 1, () => both.map(why).join(' | '));
  const winner = both.findIndex(r => r.ok === true), winCopy = [editA, editB][winner];
  tb.check('Gecko: the review holds exactly the winning tab\'s messaging', () => JSON.stringify(approval(tab.id).designReview.copy) === JSON.stringify(winCopy) && approval(tab.id).reviewHash === both[winner].reviewHash);
  if (both[winner]) tab.reviewHash = both[winner].reviewHash;
  // Tab B reloads and excludes the saved videos; tab A still holds the older review and plan.
  step('gecko tab b no videos'); const noVid = tb.blocked ? null : await api('updateAdDesignSubmission', { id: tab.id, hash: tab.reviewHash, copy: winCopy, includeVideos: false });
  tb.gate('Gecko: tab B saves the review with saved videos excluded', () => noVid && noVid.ok === true, () => why(noVid));
  const stale = { ...tab }; if (noVid && noVid.reviewHash) tab.reviewHash = noVid.reviewHash;
  step('gecko tab a stale plan'); const staleA = tb.blocked ? null : await api('publishAdDesignSubmission', { id: tab.id, hash: stale.reviewHash, prepareOnly: true, styles: ['pmax'], budgets: { pmax: 10 }, countries: ['2840'], durations: { pmax: 30 } });
  tb.check('Gecko: tab A, still showing the older review, cannot prepare a plan from it', () => staleA && staleA.ok === false && /changed/.test(staleA.error || '') && !reqsIn('gecko tab a stale plan').length, () => why(staleA));
  const pmaxOnly = { styles: ['pmax'], budgets: { pmax: 9 }, countries: ['2840'], durations: { pmax: 30 } }, tabA = { ...tab };
  await preparePlan(tb, g, tabA, { ...pmaxOnly, budgets: { pmax: 11 } }, 'plan tab a');
  await preparePlan(tb, g, tab, pmaxOnly, 'plan tab b');
  step('gecko tab a approve'); const oldPlan = tb.blocked ? null : await approve(tab, tabA.planHash);
  tb.check('Gecko: Approve from tab A with its replaced plan (11/day) is refused and sends nothing', () => tabA.planHash !== tab.planHash && oldPlan && oldPlan.ok === false && !reqsIn('gecko tab a approve').length && approval(tab.id).status === 'PENDING', () => why(oldPlan));
  tb.check('Gecko: with saved videos excluded, the plan says so and promises no film upload', () => /Saved videos are excluded/.test(approval(tab.id).pipelinePlan.summary.videoStatus || '') && !approval(tab.id).pipelinePlan.payload.meta.motion, () => approval(tab.id).pipelinePlan.summary.videoStatus);
  step('gecko approve'); const gpub = tb.blocked ? null : await approve(tab);
  tb.gate('Gecko: tab B\'s plan (9/day) publishes', () => gpub && gpub.status === 'APPLIED', () => why(gpub));
  tb.check('Gecko: the published campaign uses tab B\'s budget and messaging; no film is uploaded or promised', () => { const r = reqsIn('gecko approve').find(x => !x.validateOnly), au = r && audit(r);
    return au && au.budgets.length === 1 && Number(au.budgets[0].amountMicros) === 9e6 && !/YouTube/.test(gpub.message || '') && !callsIn('gecko approve').some(c => c.task === 'adMotionPublication') && ![...ctx.google.uploads.values()].some(u => ctx.google.sessions.get(u._sid).step.startsWith('gecko')) && !motionJob(ws2.id, ws2.fx.gecko).publication
      && JSON.stringify(au.linksOf(au.groups[0].resourceName, 'HEADLINE').map(l => l.info.text)) === JSON.stringify(winCopy.headlines); }, () => { const r = reqsIn('gecko approve').find(x => !x.validateOnly), au = r && audit(r); return why(gpub) + ' · ' + JSON.stringify(au && { budgets: au.budgets.map(x => x.amountMicros), pub: motionJob(ws2.id, ws2.fx.gecko).publication }); });

  // c) The passcode gate: every write the complete-ad flow uses refuses a request without the passcode (or with a wrong one) and changes nothing.
  step('no passcode'); const snapshot = JSON.stringify([...ctx.store.docs.entries()]), googleBefore = counts(), writes = [['publishAdDesignSubmission', { id: tab.id, hash: tab.reviewHash, planHash: tab.planHash, confirmed: true }],
    ['publishAdDesignSubmission', { id: tab.id, hash: tab.reviewHash, prepareOnly: true, ...pmaxOnly }], ['updateAdDesignSubmission', { id: tab.id, hash: tab.reviewHash, copy: winCopy, includeVideos: true }],
    ['prepareAdDesignPublication', { workspaceId: ws2.id, target: 'ads', formats: ['landscape'], includeCopy: true, queueOnly: true }], ['approve', { id: tab.id }], ['apply', { id: tab.id }], ['reconcileApproval', { id: tab.id, outcome: 'published' }],
    ['reject', { id: tab.id }], ['startAdMotionPublication', { workspaceId: ws2.id, productId: g.gid, groupRef: GROUP.ref, jobId: ws2.fx.gecko.motionJobId }], ['saveAdDesignCopy', { workspaceId: ws2.id, copy: winCopy }], ['adDesignSubmissionStatus', { id: tab.id, hash: tab.reviewHash }]];
  const refused = [];
  for (const [action, data] of writes) for (const headers of [{}, { 'x-edit-passcode': 'wrong-' + PASS }]) { const res = await ctx.kick.httpHandler({ httpMethod: 'POST', headers, body: JSON.stringify({ action, ...data }) }); while (ctx.background.length) await ctx.background.shift(); refused.push([action, res.statusCode]); }
  check(refused.every(([, code]) => code === 401) && JSON.stringify([...ctx.store.docs.entries()]) === snapshot && counts() === googleBefore && !callsIn('no passcode').length, 'every complete-ad action refuses a missing or wrong passcode (401) and changes nothing',
    { detail: JSON.stringify(refused.filter(([, code]) => code !== 401)) });

  // d) Dry run, joining the existing paused Performance Max campaign: Google validates, nothing changes, the budget stays.
  ctx.store.docs.get('Brites_GAds_Control/control').dryRun = true;
  const b = P.bunny, ws3 = await seedWorkspace({ id: 'design_adverse_bunny', products: [b], selectedKey: 'bunny', scoped: true }), dj = stage(null, 'dry-run join');
  const bap = await submitAd(ws3, b); if (!bap) dj.blocked = 'submission failed'; else Object.assign(bap, { fx: ws3.fx.bunny, wsId: ws3.id });
  await editMessaging(dj, ws3, b, bap, clone(COPY.bunny), false);
  const join = { styles: ['pmax'], budgets: { pmax: 5 }, countries: ['2840', '2124'], durations: {}, pmaxTarget: '9001' };
  await preparePlan(dj, b, bap, join, 'plan join'); await checkWithGoogle(dj, b, bap);
  step('bunny approve dry join'); const groupsBefore = ctx.google.assetGroups.size, linksBefore = ctx.google.links.length, uploadsBefore = ctx.google.uploads.size, dry = dj.blocked ? null : await approve(bap);
  dj.check('Bunny dry-run join: Approve only validates; the joined campaign\'s budget, groups and links are unchanged; the ad stays in Approval', () => dry && dry.status === 'VALIDATED' && reqsIn('bunny approve dry join').every(r => r.validateOnly) && ctx.google.budgets.get(RN('campaignBudgets', '90010')).amountMicros === '25000000'
    && ctx.google.assetGroups.size === groupsBefore && ctx.google.links.length === linksBefore && approval(bap.id).status === 'PENDING' && ctx.google.uploads.size === uploadsBefore, () => why(dry));
  ctx.store.docs.get('Brites_GAds_Control/control').dryRun = false;
  step('bunny approve join'); const live = dj.blocked ? null : await approve(bap);
  dj.check('Bunny join: with dry run off, the same card adds the product group and raises the budget 25 → 30 once', () => live && live.status === 'APPLIED' && ctx.google.budgets.get(RN('campaignBudgets', '90010')).amountMicros === '30000000' && ctx.google.assetGroups.size === groupsBefore + 1, () => why(live));

  // f) Joining a running Performance Max campaign (the card's default when one matches): films attach only while a campaign
  // is paused, so the plan must not promise them, and Approve must not report a film failure the owner cannot fix by retrying.
  ctx.google.campaigns.get(RN('campaigns', '9001')).status = 'ENABLED';
  const s = P.saturn, ws4 = await seedWorkspace({ id: 'design_adverse_saturn', products: [s], selectedKey: 'saturn', scoped: true }), rj = stage(null, 'running join');
  const sap = await submitAd(ws4, s); if (!sap) rj.blocked = 'submission failed'; else Object.assign(sap, { fx: ws4.fx.saturn, wsId: ws4.id });
  await editMessaging(rj, ws4, s, sap, clone(COPY.saturn), false);
  await preparePlan(rj, s, sap, { styles: ['pmax'], budgets: { pmax: 0 }, countries: ['2840', '2124'], durations: {}, pmaxTarget: '9001' }, 'plan running join');
  rj.check('Saturn running join: the plan says its saved films are not attached to a running campaign (and promises no upload)', () => { const pl = approval(sap.id).pipelinePlan; return !/uploaded to YouTube/.test(pl.summary.videoStatus || '') && /paused/.test(pl.summary.videoStatus || '') && !pl.payload.meta.motion; },
    () => (approval(sap.id).pipelinePlan || { summary: {} }).summary.videoStatus);
  await checkWithGoogle(rj, s, sap);
  step('saturn approve running join'); const uploadsBefore2 = ctx.google.uploads.size, rjo = rj.blocked ? null : await approve(sap);
  rj.check('Saturn running join: Approve adds the product group to the running campaign, uploads no film and reports no film failure', () => rjo && rjo.status === 'APPLIED' && /can serve now/.test(rjo.message || '') && !/films were not|YouTube/.test(rjo.message || '') && ctx.google.uploads.size === uploadsBefore2 && ctx.google.campaigns.get(RN('campaigns', '9001')).status === 'ENABLED',
    () => why(rjo));
  ctx.google.campaigns.get(RN('campaigns', '9001')).status = 'PAUSED';

  // e) The fake itself: a new campaign without the EU political-advertising declaration is rejected (the rule is not vacuous).
  step('fake self-test'); const bad = { mutateOperations: [{ campaignBudgetOperation: { create: { resourceName: RN('campaignBudgets', '-1'), name: 'Self-test', amountMicros: '1000000', deliveryMethod: 'STANDARD', explicitlyShared: false } } },
    { campaignOperation: { create: { resourceName: RN('campaigns', '-2'), name: 'Self-test', status: 'PAUSED', advertisingChannelType: 'DISPLAY', campaignBudget: RN('campaignBudgets', '-1'), maximizeConversions: {}, urlExpansionOptOut: true } } }], validateOnly: true };
  const self = await fakeFetch(`https://googleads.googleapis.com/v24/customers/${CID}/googleAds:mutate`, { method: 'POST', body: JSON.stringify(bad) }), selfRec = reqsIn('fake self-test')[0]; if (selfRec) selfRec.fault = 'self-test';
  check(self.status === 400 && selfRec && selfRec.errors.some(e => e.code === 'REQUIRED' && /contains_eu_political_advertising/.test(e.field)) && selfRec.errors.some(e => e.code === 'UNKNOWN_FIELD'), 'the fake Google Ads rejects a new campaign missing the EU political-advertising declaration or carrying a removed field',
    { detail: JSON.stringify(selfRec && selfRec.errors) });
  // ...and an image ad without its required display URL, or with one off its final URL's domain, while one on that domain passes.
  step('fake self-test display url'); const banner = (await jpeg('selftest', 300, 250, '#d9c7b0')).toString('base64');
  const imageAd = displayUrl => ({ adGroupAdOperation: { create: { adGroup: RN('adGroups', '-3'), status: 'ENABLED', ad: { name: 'Self-test 300x250', finalUrls: ['https://www.britesjewelry.com/products/self-test'], ...(displayUrl ? { displayUrl } : {}), imageAd: { imageAsset: { asset: RN('assets', '-4') } } } } } });
  const urlTest = { mutateOperations: [{ campaignBudgetOperation: { create: { resourceName: RN('campaignBudgets', '-1'), name: 'Self-test display URL', amountMicros: '1000000', deliveryMethod: 'STANDARD', explicitlyShared: false } } },
    { campaignOperation: { create: { resourceName: RN('campaigns', '-2'), name: 'Self-test display URL', status: 'PAUSED', advertisingChannelType: 'DISPLAY', campaignBudget: RN('campaignBudgets', '-1'), maximizeConversions: {}, containsEuPoliticalAdvertising: 'DOES_NOT_CONTAIN_EU_POLITICAL_ADVERTISING' } } },
    { adGroupOperation: { create: { resourceName: RN('adGroups', '-3'), campaign: RN('campaigns', '-2'), name: 'Self-test display URL', type: 'DISPLAY_STANDARD', status: 'ENABLED' } } },
    { assetOperation: { create: { resourceName: RN('assets', '-4'), name: 'Self-test 300x250', imageAsset: { data: banner } } } }, imageAd(null), imageAd('example.com'), imageAd('britesjewelry.com')], validateOnly: true };
  const urlRes = await fakeFetch(`https://googleads.googleapis.com/v24/customers/${CID}/googleAds:mutate`, { method: 'POST', body: JSON.stringify(urlTest) }), urlBody = await urlRes.json(), urlRec = reqsIn('fake self-test display url')[0]; if (urlRec) urlRec.fault = 'self-test';
  const errorsAt = i => (urlRec ? urlRec.errors : []).filter(e => e.i === i);
  check(urlRes.status === 400 && urlBody.error.status === 'INVALID_ARGUMENT' && errorsAt(4).some(e => e.code === 'REQUIRED' && e.family === 'fieldError' && /\.ad\.display_url$/.test(e.field)) && errorsAt(5).some(e => e.code === 'POLICY_FINDING' && /\.ad\.display_url$/.test(e.field))
    && !errorsAt(6).length && urlRec.errors.every(e => e.i === 4 || e.i === 5), 'the fake Google Ads rejects an image ad without its required display URL (REQUIRED, 400 INVALID_ARGUMENT) or with one off its final URL\'s domain, and accepts one on it',
    { detail: JSON.stringify(urlRec && urlRec.errors) });
});

scenario('S9', 'S9 a waiting plan prepared before image ads carried a display URL', async () => {
  await boot('S9 saved plan without display URLs');
  const p = P.saturn, ws = await seedWorkspace({ id: 'design_saved_plan_saturn', products: [p], selectedKey: 'saturn', scoped: true }), st = stage(null, 'saved plan');
  const ap = await submitAd(ws, p); if (!ap) st.blocked = 'submission failed'; else Object.assign(ap, { fx: ws.fx.saturn, wsId: ws.id });
  await editMessaging(st, ws, p, ap, clone(COPY.saturn), false);
  const choice = { styles: ['fixed_display'], budgets: { fixed_display: 4 }, countries: ['2840', '2124'], durations: { fixed_display: 10 } };
  await preparePlan(st, p, ap, choice);
  // The plan exactly as the earlier builder stored it on the approval: the same operations without displayUrl, hashed as stored.
  const images = ops => ops.map(o => o.adGroupAdOperation && o.adGroupAdOperation.create.ad).filter(ad => ad && ad.imageAd), sizes = ap ? ap.fx.proofs.filter(x => x.supported).length : 0, d = st.blocked ? null : approval(ap.id);
  if (d) { for (const payload of [d.payload, d.pipelinePlan.payload]) images(payload.mutateOperations).forEach(ad => { delete ad.displayUrl; }); d.pipelinePlan.hash = ap.planHash = ctx.E.creativeHash(d.payload); }
  const saved = d && JSON.stringify(d.pipelinePlan.payload);
  st.gate('Saturn: the waiting plan has the earlier shape (image ads without a display URL) and its own consistent hash', () => images(d.pipelinePlan.payload.mutateOperations).length === sizes && images(d.pipelinePlan.payload.mutateOperations).every(ad => !('displayUrl' in ad)) && ctx.E.creativeHash(d.pipelinePlan.payload) === d.pipelinePlan.hash);
  step('saturn prepare again'); const again = st.blocked ? null : await api('publishAdDesignSubmission', { id: ap.id, hash: ap.reviewHash, prepareOnly: true, ...choice });
  st.check('Saturn: preparing the same choices again returns the waiting plan as saved (its card keeps it)', () => again && again.cached === true && again.planHash === ap.planHash && JSON.stringify(approval(ap.id).pipelinePlan.payload) === saved, () => why(again));
  const { rec } = await checkWithGoogle(st, p, ap);
  step('saturn approve'); const r = st.blocked ? null : await approve(ap), recs = reqsIn('saturn approve');
  st.gate('Saturn: Approve ad publishes the waiting plan (status APPLIED)', () => r && r.status === 'APPLIED' && approval(ap.id).status === 'APPLIED', () => why(r) + ' · approval ' + JSON.stringify(approval(ap.id) && { status: approval(ap.id).status, lastError: approval(ap.id).lastError }));
  if (!st.blocked) verifyPublication(st, p, ap, choice, recs, COPY.saturn);
  st.check('Saturn: every image ad sent to Google (Check with Google, validation, publication) carries the display URL of its own final URL', () => { const sent = [rec, ...recs].filter(Boolean).flatMap(x => images(x.ops));
    return sent.length === 3 * sizes && sent.every(ad => ad.displayUrl === 'britesjewelry.com' && JSON.stringify(ad.finalUrls) === JSON.stringify([p.url])); }, () => JSON.stringify([rec, ...recs].filter(Boolean).map(x => images(x.ops).map(ad => ad.displayUrl || null))));
  st.check('Saturn: the stored, reviewed plan is unchanged (no display URL written into it, same hash)', () => { const pl = approval(ap.id).pipelinePlan; return JSON.stringify(pl.payload) === saved && pl.hash === ap.planHash && ctx.E.creativeHash(approval(ap.id).payload) === ap.planHash; });
});

/* ================================================================ run */
(async () => {
  const started = realNow();
  for (const s of SCENARIOS) {
    if (ONLY && !ONLY.includes(s.key)) continue;
    try { await s.fn(); }
    catch (e) { check(false, 'scenario completed without an exception', { detail: e && e.stack ? e.stack.split('\n').slice(0, 4).join(' | ') : e }); }
    if (ctx) {
      scenarioName = ctx.name;
      const ai = ctx.calls.filter(c => c.violation === 'paid-ai'), unhandled = ctx.calls.filter(c => c.unhandled);
      check(!ai.length, 'no paid AI request was attempted', { detail: ai.map(c => c.step + ' ' + c.url).join(', ') });
      check(!unhandled.length, 'every outside request went to a synthetic endpoint (Google Ads, OAuth, YouTube upload, worker)', { detail: unhandled.map(c => c.step + ' ' + c.method + ' ' + c.url.split('?')[0]).join(', ') });
      const leaked = ctx.responses.concat(ctx.calls.filter(c => c.worker).map(c => JSON.stringify(c.worker))).filter(b => SECRETS.some(s => b.includes(s)));
      check(!leaked.length, 'no console or worker response carries the passcode, an API key or an access token', { detail: leaked.map(b => b.slice(0, 200)).join(' | ') });
      check(!enabledProblems(ctx.google.requests.filter(r => r.ok && !r.validateOnly)).length, 'no campaign was created or switched to ENABLED', { detail: enabledProblems(ctx.google.requests.filter(r => r.ok && !r.validateOnly)).join('; ') });
      if (ctx.google.unknownGaql.size) console.log('NOTE ' + ctx.name + ' GAQL the fake does not model (answered with no rows): ' + [...ctx.google.unknownGaql].join(' · '));
      const rejected = ctx.google.requests.filter(r => !r.ok && r.errors.length && !r.fault);
      if (rejected.length) console.log('NOTE ' + ctx.name + ' Google rejected ' + rejected.length + ' request(s): ' + rejected.map(r => r.step + ': ' + r.errors.map(e => e.code + ' ' + e.message).join('; ')).join(' || ').slice(0, 1500));
    }
  }
  scenarioName = 'global';
  check(!netBlocked.length, 'no real network connection was attempted', { detail: netBlocked.join(', ') });
  const by = s => results.filter(r => r.status === s), scope = results.filter(r => r.tag === 'scope');
  console.log('\nSUMMARY complete-ad-e2e: ' + by('PASS').length + ' passed, ' + by('FAIL').length + ' failed, ' + by('XFAIL').length + ' expected failures, ' + by('XPASS').length + ' unexpected passes (' + ((realNow() - started) / 1000).toFixed(1) + 's)');
  console.log('  [SCOPE] checks: ' + scope.filter(r => r.status === 'PASS').length + '/' + scope.length + ' pass' + (SCOPE_STRICT ? ' (enforced)' : ' (not enforced: COMPLETE_AD_SCOPE_STRICT=0)'));
  for (const r of by('XFAIL')) console.log('  XFAIL ' + r.scenario + ' · ' + (r.tag ? '[' + r.tag.toUpperCase() + '] ' : '') + r.name);
  for (const r of by('FAIL')) console.log('  FAIL ' + r.scenario + ' · ' + r.name);
  for (const r of by('XPASS')) console.log('  XPASS ' + r.scenario + ' · ' + r.name + ' (a [KNOWN] defect no longer reproduces: drop the tag)');
  assert.ok(results.length > 0, 'the harness recorded checks');
  if (by('FAIL').length) process.exitCode = 1;
  require('./suite-guard.cjs').done();
})();
