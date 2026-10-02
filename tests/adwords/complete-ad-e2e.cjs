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
  GADS_CLIENT_ID: 'synthetic.apps.googleusercontent.com', GADS_CLIENT_SECRET: 'synthetic', GADS_REFRESH_TOKEN: 'synthetic', GADS_DEVELOPER_TOKEN: 'synthetic',
  GADS_CUSTOMER_ID: CID, GADS_LOGIN_CUSTOMER_ID: CID, GMC_MERCHANT_ID: MERCHANT,
  // Present on purpose: a paid AI request is attempted (and caught by the fake) instead of being skipped silently.
  OPENAI_API_KEY: 'synthetic-never-sent', ANTHROPIC_API_KEY: 'synthetic-never-sent', GEMINI_API_KEY: 'synthetic-never-sent',
  SHOPIFY_STORE: 'synthetic-store.myshopify.com', SHOPIFY_CLIENT_ID: 'synthetic', SHOPIFY_CLIENT_SECRET: 'synthetic', FIREBASE_PRIVATE_KEY: 'synthetic', URL: SITE, EDIT_PASSCODE: PASS
});
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
        if ((r.youtubeVideos || []).length > 5 || (r.youtubeVideos || []).some(v => (infoOf(v.asset) || {}).kind !== 'video')) err(i, 'INVALID_VIDEO', 'youtube_videos', 'ad.responsive_display_ad.youtube_videos', 'adError');
        if (ad.name !== undefined) err(i, 'FIELD_NOT_SUPPORTED', 'Ad.name is not supported for responsive display ads', 'ad.name', 'adError');
      } else if (ad.imageAd) {
        const info = infoOf(ad.imageAd.imageAsset && ad.imageAd.imageAsset.asset);
        if (!info || info.kind !== 'image' || !FIXED_SIZES.has(info.width + 'x' + info.height)) err(i, 'IMAGE_SIZE_NOT_SUPPORTED', 'Image ad size ' + (info ? info.width + 'x' + info.height : 'unknown') + ' is not a supported Display size', 'ad.image_ad', 'imageError');
        else if (info.bytes > 150 * 1024) err(i, 'FILE_TOO_LARGE', 'Image ads are limited to 150 KB', 'ad.image_ad', 'imageError');
        if (!ad.name) err(i, 'REQUIRED', 'Image ads need a name', 'ad.name', 'adError');
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
    const task = ((body && body.tasks) || []).join(','), base = ctx.step; call.task = task;
    if (ctx.beforeWorker) await ctx.beforeWorker(body);
    ctx.background.push(new Promise(done => setImmediate(done)).then(() => { ctx.step = base + ' · ' + task; return ctx.worker.handler({ httpMethod: 'POST', headers: {}, body: String(raw) }); })
      .then(res => { call.worker = JSON.parse(res.body || '{}'); call.workerStatus = res.statusCode; }).catch(e => { call.workerError = e.message; }));
    return reply(202, '');
  }
  if (url === 'https://oauth2.googleapis.com/token') return reply(200, { access_token: 'synthetic-token', expires_in: 3600, token_type: 'Bearer' });
  if (/^https:\/\/googleads\.googleapis\.com\/resumable\//.test(url)) return upload(ctx.google, url, method, headers, body, call);
  const ads = url.match(/^https:\/\/googleads\.googleapis\.com\/(v\d+)\/customers\/(\d+)\/googleAds:(search|mutate)$/);
  if (ads) {
    if (ads[1] !== 'v24' || ads[2] !== CID) return reply(400, { error: { message: 'Unexpected Google Ads version or customer ' + ads[1] + '/' + ads[2] } });
    if (ads[3] === 'search') { call.gaql = body && body.query; return reply(200, { results: runGaql(ctx.google, body && body.query) }); }
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
  ctx = { name, step: 'setup', calls: [], background: [], store: createStore(), google: createGoogle(), beforeWorker: null };
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
  let json = {}; try { json = JSON.parse(res.body || '{}'); } catch (_) { json = { error: 'unparseable response ' + String(res.body).slice(0, 200) }; }
  return { http: res.statusCode, ...json };
}
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
  const before = new Set(ctx.google.campaigns.keys()), r = st.blocked ? null : await api('publishAdDesignSubmission', { id: ap.id, hash: ap.reviewHash, planHash: ap.planHash, confirmed: true });
  st.gate(p.short + ': Approve ad publishes the plan (status APPLIED)', () => r && r.ok !== false && r.status === 'APPLIED' && approval(ap.id).status === 'APPLIED', () => why(r) + ' · approval ' + JSON.stringify(approval(ap.id) && { status: approval(ap.id).status, lastError: approval(ap.id).lastError }));
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
  const pub = js.blocked ? null : await api('publishAdDesignSubmission', { id: ap.id, hash: ap.reviewHash, planHash: ap.planHash, confirmed: true });
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
  step('gecko approve'); const b2 = counts(), r2 = cs.blocked ? null : await api('publishAdDesignSubmission', { id: ap2.id, hash: ap2.reviewHash, planHash: ap2.planHash, confirmed: true });
  cs.check('Gecko: Approve over the daily ceiling (enabled budgets grew to 90) is refused; nothing is created', () => r2 && r2.ok === false && /Over your daily ceiling/.test(r2.error || '') && /Nothing was published/.test(r2.error || '') && counts() === b2 && !reqsIn('gecko approve').some(x => !x.validateOnly),
    () => why(r2));
  cs.check('Gecko: the refused approval keeps the reason and is not left publishing', () => { const d = approval(ap2.id); return d.status === 'APPROVED' && /Over your daily ceiling/.test(d.lastError || '') && !d.applyAttempt && !d.needsReconciliation; });
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
  step('gecko approve'); const before = counts(), r = st.blocked ? null : await api('publishAdDesignSubmission', { id: ap.id, hash: ap.reviewHash, planHash: ap.planHash, confirmed: true }), recs = reqsIn('gecko approve');
  st.gate('Gecko dry run: Approve reports validation only', () => r && r.status === 'VALIDATED' && r.dryRun === true && /dry-run mode kept them unpublished/.test(r.message || ''), () => why(r));
  st.check('Gecko dry run: exactly one request, validateOnly:true, accepted; nothing is created', () => recs.length === 1 && recs[0].validateOnly && recs[0].ok && counts() === before && !ctx.google.uploads.size, () => recs.map(x => x.validateOnly).join(','));
  st.check('Gecko dry run: the ad returns to Approval (PENDING) with its reviewed plan and the dry-run check recorded; no campaign IDs; no film upload starts', () => { const d = approval(ap.id); return d.status === 'PENDING' && Number(d.validatedAt) > 0 && d.dryRunCheck && d.dryRunCheck.planHash === ap.planHash && d.pipelinePlan && d.pipelinePlan.hash === ap.planHash && d.reviewHash === ap.reviewHash && !d.pipelineReview && !d.publishedCampaignIds && !callsIn('gecko approve').some(c => c.task === 'adMotionPublication') && !motionJob(ws.id, ws.fx.gecko).publication; },
    () => JSON.stringify(approval(ap.id) && { status: approval(ap.id).status, dryRunCheck: approval(ap.id).dryRunCheck, validatedAt: approval(ap.id).validatedAt }));
  st.check('Gecko dry run: the response says the dry run only validated and the ad stays in Approval', () => /Nothing was published; the ad stays in Approval/.test(r.message || ''), () => why(r));
  // With dry run off again, the same reviewed ad is published from the same Approve ad button, with its films.
  ctx.store.docs.get('Brites_GAds_Control/control').dryRun = false;
  step('gecko approve live'); const live = st.blocked ? null : await api('publishAdDesignSubmission', { id: ap.id, hash: ap.reviewHash, planHash: ap.planHash, confirmed: true }), liveRecs = reqsIn('gecko approve live');
  st.gate('Gecko: after a dry-run check, turning dry run off and approving the same reviewed plan publishes it', () => live && live.status === 'APPLIED' && approval(ap.id).status === 'APPLIED', () => why(live) + ' · approval status ' + (approval(ap.id) || {}).status);
  const a = st.blocked ? null : verifyPublication(st, p, ap, choice, liveRecs, COPY.gecko), real = liveRecs.find(x => !x.validateOnly), grp = real && a && real.idMap.get(a.groups[0].resourceName), camp = grp && ctx.google.assetGroups.get(grp).campaign;
  st.check('Gecko films: Approve after the dry run starts the reviewed film upload for the new group', () => /uploading to YouTube/.test(live.message || ''), () => live && live.message);
  if (!st.blocked) verifyFilms(st, p, ap, grp, camp, 'gecko approve live · adMotionPublication'); else ['three reviewed films upload', 'attachment is validated first', 'the group shows the three uploaded videos'].forEach(n => st.check(p.short + ' films: ' + n, () => false));
  step('gecko approve live again'); const again = st.blocked ? null : await api('publishAdDesignSubmission', { id: ap.id, hash: ap.reviewHash, planHash: ap.planHash, confirmed: true });
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
    const both = st.blocked ? [] : await Promise.all([1, 2].map(() => api('publishAdDesignSubmission', { id: ap.id, hash: ap.reviewHash, planHash: ap.planHash, confirmed: true })));
    const real = reqsIn('bunny approve twice').filter(r => !r.validateOnly && audit(r).campaigns.length);
    st.check('Bunny: two simultaneous Approve clicks publish once and refuse the other', () => both.filter(r => r.status === 'APPLIED').length === 1 && both.filter(r => r.ok === false).length === 1, () => both.map(why).join(' | '));
    st.check('Bunny: exactly one campaign-creating request and one new campaign', () => real.length === 1 && ctx.google.campaigns.size === before + 1 && campaignsNamed('Brites · ' + p.title).length === 1);
    step('bunny approve again'); const again = st.blocked ? null : await api('publishAdDesignSubmission', { id: ap.id, hash: ap.reviewHash, planHash: ap.planHash, confirmed: true });
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
    step('gecko approve ineligible'); const inel = st.blocked ? null : await api('publishAdDesignSubmission', { id: ap.id, hash: ap.reviewHash, planHash: ap.planHash, confirmed: true });
    st.check('Gecko: Approve is refused while its Merchant offer is not eligible; the review stays pending and nothing is sent', () => inel && inel.ok === false && /no longer eligible|none of this product.s \d+ Merchant Center offers? can serve/.test(inel.error || '') && /Nothing was created|no longer eligible/.test(inel.error || '') && approval(ap.id).status === 'PENDING' && !reqsIn('gecko approve ineligible').some(r => r.ops.length), () => why(inel));
    Object.assign(offer, { status: 'ELIGIBLE', availability: 'IN_STOCK' });
    ctx.google.faults.push({ kind: 'lost', when: rec => audit(rec).campaigns.length > 0 });
    step('gecko approve lost'); const lost = st.blocked ? null : await api('publishAdDesignSubmission', { id: ap.id, hash: ap.reviewHash, planHash: ap.planHash, confirmed: true });
    st.check('Gecko: a lost Google response is reported as unconfirmed, never as a failure to retry', () => lost && lost.ok === false && /could not be confirmed/.test(lost.error || ''), () => why(lost));
    st.check('Gecko: the approval is APPLY_UNKNOWN and needs reconciliation; Google holds exactly one campaign', () => { const d = approval(ap.id); return d.status === 'APPLY_UNKNOWN' && d.needsReconciliation === true && campaignsNamed('Brites · ' + p.title).length === 1; },
      () => JSON.stringify({ status: (approval(ap.id) || {}).status, n: campaignsNamed('Brites · ' + p.title).length }));
    step('gecko apply unknown'); await api('apply', { id: ap.id });
    st.check('Gecko: publishing an unconfirmed approval again is refused and sends nothing', () => !reqsIn('gecko apply unknown · publishApproval').length && approval(ap.id).status === 'APPLY_UNKNOWN' && campaignsNamed('Brites · ' + p.title).length === 1);
    step('gecko reconcile'); const rec = await api('reconcileApproval', { id: ap.id, outcome: 'published' });
    st.check('Gecko: reconciling as published records APPLIED without sending anything; still one campaign', () => rec.status === 'APPLIED' && approval(ap.id).status === 'APPLIED' && !reqsIn('gecko reconcile').length && campaignsNamed('Brites · ' + p.title).length === 1, () => why(rec)); }
  // c) A partial failure: nothing was created; reconciled as not published, then published once.
  { const p = P.duck, { st, ap } = await ready(p, pmaxOnly(12));
    ctx.google.faults.push({ kind: 'partial', when: rec => audit(rec).campaigns.length > 0 });
    step('duck approve partial'); const part = st.blocked ? null : await api('publishAdDesignSubmission', { id: ap.id, hash: ap.reviewHash, planHash: ap.planHash, confirmed: true });
    st.check('Duck: a partial-failure response is unconfirmed (APPLY_UNKNOWN) and nothing was created', () => part && part.ok === false && approval(ap.id).status === 'APPLY_UNKNOWN' && campaignsNamed('Brites · ' + p.title).length === 0, () => why(part));
    step('duck reconcile'); await api('reconcileApproval', { id: ap.id, outcome: 'not_published' });
    st.check('Duck: reconciling as not published returns the approval to APPROVED with the reason', () => approval(ap.id).status === 'APPROVED' && /not published/.test(approval(ap.id).lastError || ''));
    step('duck apply'); await api('apply', { id: ap.id });
    st.check('Duck: publishing it again creates exactly one PAUSED campaign (validated first)', () => { const r = reqsIn('duck apply · publishApproval'); return approval(ap.id).status === 'APPLIED' && r.length === 2 && r[0].validateOnly && campaignsNamed('Brites · ' + p.title).length === 1 && campaignsNamed('Brites · ' + p.title)[0].status === 'PAUSED'; });
    step('duck apply again'); await api('apply', { id: ap.id });
    st.check('Duck: a further publish request creates nothing', () => !reqsIn('duck apply again · publishApproval').length && campaignsNamed('Brites · ' + p.title).length === 1); }
  // d) The duplicate-name guard, then a definite Google rejection.
  { const p = P.saturn, { st, ap } = await ready(p, pmaxOnly(9)), name = !st.blocked && approval(ap.id).pipelinePlan.summary.campaigns[0].name;
    if (name) ctx.google.campaigns.set(RN('campaigns', '9500'), { resourceName: RN('campaigns', '9500'), id: '9500', name, status: 'PAUSED', advertisingChannelType: 'PERFORMANCE_MAX', campaignBudget: RN('campaignBudgets', '91000') });
    step('saturn approve duplicate'); const dup = st.blocked ? null : await api('publishAdDesignSubmission', { id: ap.id, hash: ap.reviewHash, planHash: ap.planHash, confirmed: true });
    st.check('Saturn: a campaign with the planned name already in Google refuses publication and sends nothing', () => dup && dup.ok === false && /already exists/.test(dup.error || '') && !reqsIn('saturn approve duplicate').length && approval(ap.id).status === 'APPROVED', () => why(dup));
    if (name) ctx.google.campaigns.get(RN('campaigns', '9500')).status = 'REMOVED';
    ctx.google.faults.push({ kind: 'definite', when: rec => audit(rec).campaigns.length > 0 });
    step('saturn apply rejected'); await api('apply', { id: ap.id });
    st.check('Saturn: a definite Google rejection returns the approval to APPROVED with the error (no reconciliation needed)', () => { const d = approval(ap.id); return d.status === 'APPROVED' && !d.needsReconciliation && /mutate failed/.test(d.lastError || '') && campaignsNamed('Brites · ' + p.title).filter(c => c.id !== '9500').length === 0; },
      () => JSON.stringify({ status: (approval(ap.id) || {}).status, lastError: ((approval(ap.id) || {}).lastError || '').slice(0, 200) }));
    step('saturn apply'); await api('apply', { id: ap.id });
    st.check('Saturn: publishing again after the rejection creates exactly one PAUSED campaign', () => approval(ap.id).status === 'APPLIED' && campaignsNamed('Brites · ' + p.title).filter(c => c.id !== '9500').length === 1); }
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
  step('saturn approve'); const r = st.blocked ? null : await api('publishAdDesignSubmission', { id: ap.id, hash: ap.reviewHash, planHash: ap.planHash, confirmed: true });
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

/* ================================================================ run */
(async () => {
  const started = Date.now();
  for (const s of SCENARIOS) {
    if (ONLY && !ONLY.includes(s.key)) continue;
    try { await s.fn(); }
    catch (e) { check(false, 'scenario completed without an exception', { detail: e && e.stack ? e.stack.split('\n').slice(0, 4).join(' | ') : e }); }
    if (ctx) {
      scenarioName = ctx.name;
      const ai = ctx.calls.filter(c => c.violation === 'paid-ai'), unhandled = ctx.calls.filter(c => c.unhandled);
      check(!ai.length, 'no paid AI request was attempted', { detail: ai.map(c => c.step + ' ' + c.url).join(', ') });
      check(!unhandled.length, 'every outside request went to a synthetic endpoint (Google Ads, OAuth, YouTube upload, worker)', { detail: unhandled.map(c => c.step + ' ' + c.method + ' ' + c.url.split('?')[0]).join(', ') });
      check(!enabledProblems(ctx.google.requests.filter(r => r.ok && !r.validateOnly)).length, 'no campaign was created or switched to ENABLED', { detail: enabledProblems(ctx.google.requests.filter(r => r.ok && !r.validateOnly)).join('; ') });
      if (ctx.google.unknownGaql.size) console.log('NOTE ' + ctx.name + ' GAQL the fake does not model (answered with no rows): ' + [...ctx.google.unknownGaql].join(' · '));
      const rejected = ctx.google.requests.filter(r => !r.ok && r.errors.length && !r.fault);
      if (rejected.length) console.log('NOTE ' + ctx.name + ' Google rejected ' + rejected.length + ' request(s): ' + rejected.map(r => r.step + ': ' + r.errors.map(e => e.code + ' ' + e.message).join('; ')).join(' || ').slice(0, 1500));
    }
  }
  scenarioName = 'global';
  check(!netBlocked.length, 'no real network connection was attempted', { detail: netBlocked.join(', ') });
  const by = s => results.filter(r => r.status === s), scope = results.filter(r => r.tag === 'scope');
  console.log('\nSUMMARY complete-ad-e2e: ' + by('PASS').length + ' passed, ' + by('FAIL').length + ' failed, ' + by('XFAIL').length + ' expected failures, ' + by('XPASS').length + ' unexpected passes (' + ((Date.now() - started) / 1000).toFixed(1) + 's)');
  console.log('  [SCOPE] checks: ' + scope.filter(r => r.status === 'PASS').length + '/' + scope.length + ' pass' + (SCOPE_STRICT ? ' (enforced)' : ' (not enforced: COMPLETE_AD_SCOPE_STRICT=0)'));
  for (const r of by('XFAIL')) console.log('  XFAIL ' + r.scenario + ' · ' + (r.tag ? '[' + r.tag.toUpperCase() + '] ' : '') + r.name);
  for (const r of by('FAIL')) console.log('  FAIL ' + r.scenario + ' · ' + r.name);
  for (const r of by('XPASS')) console.log('  XPASS ' + r.scenario + ' · ' + r.name + ' (a [KNOWN] defect no longer reproduces: drop the tag)');
  assert.ok(results.length > 0, 'the harness recorded checks');
  if (by('FAIL').length) process.exitCode = 1;
  require('./suite-guard.cjs').done();
})();
