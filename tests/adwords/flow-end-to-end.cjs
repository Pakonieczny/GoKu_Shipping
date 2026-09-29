// Whole-flow offline harness for the Brites Ad Autopilot console.
//
// Drives the real console API (googleAdsAutopilotKick.httpHandler), the real
// background worker and the real engine through one Search campaign's life:
//   research scan -> draft -> creative review -> approval in dry run (validate
//   only) -> approval live (publication) -> enable -> reporting -> learning
// and asserts every hand-off between them: the draft waits for approval, nothing
// reaches Google before approval, the operations sent equal the approved payload,
// and the new campaign's numbers flow into reporting and the learning record.
// Only the outside world is fake:
//   * Google Ads REST: a small synthetic account (GAQL search, Keyword Planner,
//     googleAds:mutate / campaigns:mutate recorder that also applies accepted
//     mutations, so later reports see the new campaign)
//   * OpenAI / Anthropic chat: canned JSON chosen by prompt type
//   * Shopify Admin, Frankfurter FX and the store landing pages
//   * Firestore and Storage: in memory, rejecting values real Firestore rejects
//   * the clock: shifted forward so serving days and the 14-day learning
//     windows can elapse inside one run
// Every real socket is blocked. Synthetic data only; nothing is paid for.
// Not covered here (needs product changes or live services): Performance Max
// and image/video generation, the lesson-distilling AI pass, live Shopify
// catalogue reads, Google policy review and whether ads actually serve.
// Run: node tests/adwords/flow-end-to-end.cjs   (FLOW_DEBUG=1 prints payloads)
'use strict';
const assert = require('assert/strict'), path = require('path'), Module = require('module');
const started = Date.now();
let passed = 0;
const check = (cond, name) => { assert.ok(cond, name); passed++; console.log('PASS', name); };
const debug = (label, value) => { if (process.env.FLOW_DEBUG) console.log('DEBUG', label, JSON.stringify(value, null, 1).slice(0, Number(process.env.FLOW_DEBUG) || 4000)); };

/* ---------------------------------------------------------------- network guard */
for (const [mod, keys] of [['http', ['request', 'get']], ['https', ['request', 'get']], ['net', ['connect', 'createConnection']], ['tls', ['connect']], ['http2', ['connect']]]) {
  const m = require(mod); for (const k of keys) m[k] = () => { throw new Error('flow-end-to-end: live network is forbidden (' + mod + '.' + k + ')'); };
}
globalThis.fetch = async () => { throw new Error('flow-end-to-end: live network is forbidden (global fetch)'); };

/* ---------------------------------------------------------------- adjustable clock (serving days and the learning window) */
// FLOW_NOW=2026-09-29T01:30:00Z starts the flow at that instant (e.g. either side of UTC midnight,
// when the account's Toronto date is a day behind the server's UTC date).
const RealDate = Date; let clockShift = process.env.FLOW_NOW ? RealDate.parse(process.env.FLOW_NOW) - RealDate.now() : 0;
if (process.env.FLOW_NOW && !isFinite(clockShift)) throw new Error('FLOW_NOW must be an ISO date-time');
globalThis.Date = class extends RealDate { constructor(...a) { if (a.length) super(...a); else super(RealDate.now() + clockShift); } static now() { return RealDate.now() + clockShift; } };
const advanceDays = n => { clockShift += n * 86400000; };

/* ---------------------------------------------------------------- environment */
const CID = '1234567890', PASS = 'synthetic-pass', SITE = 'https://console.synthetic.test', STORE = 'synthetic-store.myshopify.com';
for (const k of ['GADS_CURRENCY', 'GADS_TARGET_ROAS', 'GADS_MAX_DAILY_BUDGET_TOTAL', 'GADS_API_VERSION', 'GADS_GEN_MODEL', 'GMC_REFRESH_TOKEN', 'GMC_MERCHANT_ID',
  'GMC_CLIENT_ID', 'GMC_CLIENT_SECRET', 'GEMINI_API_KEY', 'GADS_CONVERSION_ACTION', 'GADS_CONVERSION_UPLOAD_API', 'GADS_NEW_CAMPAIGN_BUDGET', 'GADS_TERM_EXCLUSIONS', 'GADS_MESSAGING_RULES', 'SITE_NAME']) delete process.env[k];
Object.assign(process.env, {
  GADS_CLIENT_ID: 'synthetic.apps.googleusercontent.com', GADS_CLIENT_SECRET: 'synthetic', GADS_REFRESH_TOKEN: 'synthetic', GADS_DEVELOPER_TOKEN: 'synthetic',
  GADS_CUSTOMER_ID: CID, GADS_LOGIN_CUSTOMER_ID: CID, OPENAI_API_KEY: 'synthetic-key', ANTHROPIC_API_KEY: 'synthetic-key',
  SHOPIFY_STORE: STORE, SHOPIFY_CLIENT_ID: 'synthetic', SHOPIFY_CLIENT_SECRET: 'synthetic', FIREBASE_PRIVATE_KEY: 'synthetic',
  URL: SITE, EDIT_PASSCODE: PASS, KP_BACKOFF_MS: '1'
});

/* ---------------------------------------------------------------- in-memory Firestore */
class Timestamp {
  constructor(ms) { Object.defineProperty(this, '_ms', { value: ms }); this.seconds = Math.floor(ms / 1000); this.nanoseconds = (ms % 1000) * 1e6; }
  toMillis() { return this._ms; } toDate() { return new Date(this._ms); } valueOf() { return this._ms; }
  static now() { return new Timestamp(Date.now()); } static fromMillis(ms) { return new Timestamp(ms); } static fromDate(d) { return new Timestamp(d.getTime()); }
}
function createFirestore() {
  const docs = new Map(), files = new Map(); let auto = 0;
  const FieldValue = { serverTimestamp: () => ({ __fv: 'ts' }), increment: n => ({ __fv: 'inc', n }), arrayUnion: (...v) => ({ __fv: 'union', v }),
    arrayRemove: (...v) => ({ __fv: 'remove', v }), delete: () => ({ __fv: 'del' }) };
  const isFV = v => v && typeof v === 'object' && typeof v.__fv === 'string';
  const isPlain = v => v && typeof v === 'object' && (Object.getPrototypeOf(v) === Object.prototype || Object.getPrototypeOf(v) === null);
  // Real Firestore (firebaseAdmin.js sets no ignoreUndefinedProperties) rejects these values.
  function validate(v, at, inArray) {
    if (v === undefined) throw new Error('Firestore rejects undefined at ' + at);
    if (typeof v === 'function' || typeof v === 'symbol' || typeof v === 'bigint') throw new Error('Firestore cannot store ' + typeof v + ' at ' + at);
    if (v === null || typeof v !== 'object' || v instanceof Timestamp || Buffer.isBuffer(v) || v instanceof RealDate || isFV(v)) return;
    if (Array.isArray(v)) { if (inArray) throw new Error('Firestore rejects nested arrays at ' + at); v.forEach((x, i) => validate(x, at + '[' + i + ']', true)); return; }
    if (!isPlain(v)) throw new Error('Firestore cannot serialize ' + (v.constructor && v.constructor.name) + ' at ' + at);
    for (const [k, x] of Object.entries(v)) validate(x, at + '.' + k, false);
  }
  const clone = v => v instanceof Timestamp ? v : v instanceof RealDate ? new Timestamp(v.getTime()) : Buffer.isBuffer(v) ? Buffer.from(v)
    : Array.isArray(v) ? v.map(clone) : v && typeof v === 'object' ? Object.fromEntries(Object.entries(v).map(([k, x]) => [k, clone(x)])) : v;
  function resolve(v, prior) {
    if (isFV(v)) {
      if (v.__fv === 'ts') return new Timestamp(Date.now());
      if (v.__fv === 'inc') return (Number(prior) || 0) + v.n;
      if (v.__fv === 'union') { const out = Array.isArray(prior) ? prior.slice() : []; v.v.forEach(x => { if (!out.some(y => JSON.stringify(y) === JSON.stringify(x))) out.push(clone(x)); }); return out; }
      if (v.__fv === 'remove') return (Array.isArray(prior) ? prior : []).filter(y => !v.v.some(x => JSON.stringify(x) === JSON.stringify(y)));
    }
    if (isPlain(v)) return Object.fromEntries(Object.entries(v).filter(([, x]) => !(isFV(x) && x.__fv === 'del')).map(([k, x]) => [k, resolve(x, prior && prior[k])]));
    return clone(v);
  }
  function merge(target, patch) {
    for (const [k, v] of Object.entries(patch)) {
      if (isFV(v) && v.__fv === 'del') { delete target[k]; continue; }
      if (isPlain(v) && !isFV(v)) { target[k] = isPlain(target[k]) ? target[k] : {}; merge(target[k], v); }
      else target[k] = resolve(v, target[k]);
    }
    return target;
  }
  const sizeCheck = (p, data) => { if (Buffer.byteLength(JSON.stringify(data)) > 1048487) throw new Error('Firestore document ' + p + ' exceeds the maximum allowed size'); };
  const getField = (o, f) => String(f).split('.').reduce((a, k) => (a == null ? undefined : a[k]), o);
  const cmp = (a, b) => { a = a instanceof Timestamp ? a.toMillis() : a; b = b instanceof Timestamp ? b.toMillis() : b; return a < b ? -1 : a > b ? 1 : 0; };
  const snap = (p, ref) => { const d = docs.get(p); return { id: p.split('/').pop(), ref, exists: d !== undefined, data: () => (d === undefined ? undefined : clone(d)), get: f => clone(getField(d, f)) }; };
  // Writes validate synchronously, as the Admin SDK does; transactions and batches apply them on commit.
  function writer(p) {
    return {
      set: (data, opts) => { validate(data, p); return () => { const next = opts && opts.merge ? merge(clone(docs.get(p) || {}), data) : resolve(data, docs.get(p)); sizeCheck(p, next); docs.set(p, next); }; },
      update: data => { validate(data, p); return () => {
        if (!docs.has(p)) throw Object.assign(new Error('5 NOT_FOUND: No document to update: ' + p), { code: 5 });
        const next = clone(docs.get(p));
        for (const [key, v] of Object.entries(data)) {
          const parts = key.split('.'); let at = next;
          for (const part of parts.slice(0, -1)) at = isPlain(at[part]) ? at[part] : (at[part] = {});
          const last = parts[parts.length - 1];
          if (isFV(v) && v.__fv === 'del') delete at[last]; else at[last] = resolve(v, at[last]);
        }
        sizeCheck(p, next); docs.set(p, next); }; },
      create: data => { validate(data, p); return () => { if (docs.has(p)) throw Object.assign(new Error('6 ALREADY_EXISTS: ' + p), { code: 6 }); docs.set(p, resolve(data)); }; },
      delete: () => () => { docs.delete(p); }
    };
  }
  function docRef(p) {
    const w = writer(p), ref = {
      id: p.split('/').pop(), path: p,
      get: async () => snap(p, ref),
      set: async (data, opts) => w.set(data, opts)(), update: async data => w.update(data)(), create: async data => w.create(data)(), delete: async () => w.delete()(),
      collection: n => collRef(p + '/' + n), _w: w
    };
    return ref;
  }
  function query(p, filters, order, lim, after) {
    const q = {
      where: (f, op, v) => query(p, filters.concat([[f, op, v]]), order, lim, after),
      orderBy: (f, dir = 'asc') => query(p, filters, order.concat([[f, dir]]), lim, after),
      limit: n => query(p, filters, order, n, after),
      startAfter: v => query(p, filters, order, lim, v),
      select: () => q,
      get: async () => {
        let rows = [...docs.keys()].filter(k => k.startsWith(p + '/') && !k.slice(p.length + 1).includes('/')).map(k => ({ k, d: docs.get(k) }));
        for (const [f, op, v] of filters) rows = rows.filter(({ d }) => { const x = getField(d, f);
          if (op === '==') return x !== undefined && cmp(x, v) === 0; if (op === '!=') return x !== undefined && cmp(x, v) !== 0;
          if (op === 'in') return v.some(y => cmp(x, y) === 0); if (op === 'not-in') return x !== undefined && !v.some(y => cmp(x, y) === 0);
          if (op === 'array-contains') return Array.isArray(x) && x.some(y => cmp(y, v) === 0);
          if (x === undefined) return false; const c = cmp(x, v); return op === '<' ? c < 0 : op === '<=' ? c <= 0 : op === '>' ? c > 0 : op === '>=' ? c >= 0 : false; });
        for (const [f] of order) rows = rows.filter(({ d }) => getField(d, f) !== undefined); // Firestore omits documents missing an ordered field
        if (order.length) rows.sort((a, b) => { for (const [f, dir] of order) { const c = cmp(getField(a.d, f), getField(b.d, f)); if (c) return dir === 'desc' ? -c : c; } return 0; });
        if (after != null) { const i = rows.findIndex(r => r.k.split('/').pop() === (after.id || after)); if (i >= 0) rows = rows.slice(i + 1); }
        if (lim != null) rows = rows.slice(0, lim);
        const list = rows.map(({ k }) => snap(k, docRef(k)));
        return { docs: list, size: list.length, empty: !list.length, forEach: fn => list.forEach(fn) };
      }
    };
    return q;
  }
  function collRef(p) { return Object.assign(query(p, [], [], null, null), { id: p.split('/').pop(), path: p, doc: id => docRef(p + '/' + (id || 'auto' + (++auto))),
    add: async data => { const ref = docRef(p + '/auto' + String(++auto).padStart(6, '0')); await ref.set(data); return ref; } }); }
  const db = {
    collection: collRef, doc: p => docRef(p), getAll: async (...refs) => Promise.all(refs.map(r => r.get())),
    runTransaction: async fn => { const ops = [], tx = { get: r => r.get(), set: (r, v, o) => { ops.push(r._w.set(v, o)); return tx; }, update: (r, v) => { ops.push(r._w.update(v)); return tx; },
      delete: r => { ops.push(r._w.delete()); return tx; }, create: (r, v) => { ops.push(r._w.create(v)); return tx; } };
      const out = await fn(tx); ops.forEach(op => op()); return out; },
    batch: () => { const ops = [], b = { set: (r, v, o) => { ops.push(r._w.set(v, o)); return b; }, update: (r, v) => { ops.push(r._w.update(v)); return b; }, delete: r => { ops.push(r._w.delete()); return b; },
      create: (r, v) => { ops.push(r._w.create(v)); return b; }, commit: async () => { ops.forEach(op => op()); } }; return b; }
  };
  const bucket = { name: 'synthetic-bucket', file: p => ({ name: p,
    save: async b => { files.set(p, Buffer.from(b)); }, download: async () => { if (!files.has(p)) throw new Error('No such object: ' + p); return [files.get(p)]; },
    delete: async () => { files.delete(p); }, exists: async () => [files.has(p)], getSignedUrl: async () => ['https://storage.synthetic.test/' + encodeURIComponent(p)],
    getMetadata: async () => [{}] }), getMetadata: async () => [{ cors: [] }], setCorsConfiguration: async () => [{}] };
  const firestore = Object.assign(() => db, { FieldValue, Timestamp });
  return { docs, files, db, admin: { apps: [{}], firestore, storage: () => ({ bucket: () => bucket }) } };
}
const store = createFirestore();

/* ---------------------------------------------------------------- synthetic Google Ads account */
const tz = 'America/Toronto', FX = 0.73; // synthetic CAD->USD daily rate
const ymd = offsetDays => new Intl.DateTimeFormat('en-CA', { timeZone: tz, year: 'numeric', month: '2-digit', day: '2-digit' }).format(new Date(Date.now() + offsetDays * 86400000));
const TODAY = ymd(0), YESTERDAY = ymd(-1);
const world = { nextId: 880000, budgets: new Map(), campaigns: new Map(), adGroups: new Map(), ads: new Map(), keywords: new Map(), campaignCriteria: new Map(), assets: new Map(), campaignAssets: new Map(), metrics: [] };
const newId = () => String(++world.nextId);
function seedCampaign({ id, name, status, budget }) {
  const budgetRn = `customers/${CID}/campaignBudgets/${id}0`;
  world.budgets.set(budgetRn, { resourceName: budgetRn, amountMicros: String(budget * 1e6) });
  world.campaigns.set(id, { id, resourceName: `customers/${CID}/campaigns/${id}`, name, status, channel: 'SEARCH', budget: budgetRn, primaryStatus: status === 'ENABLED' ? 'ELIGIBLE' : 'PAUSED' });
}
seedCampaign({ id: '7001', name: 'BA · synthetic-existing-search', status: 'ENABLED', budget: 40 });
world.metrics.push({ campaignId: '7001', date: YESTERDAY, impressions: 400, clicks: 12, costMicros: 9500000, conversions: 1, conversionsValue: 58, cdConversions: 1, cdValue: 58 });

const INT64 = new Set(['id', 'amountMicros', 'costMicros', 'clicks', 'impressions', 'cpcBidMicros', 'criterionId']);
const int64 = o => Object.fromEntries(Object.entries(o).map(([k, v]) => [k, INT64.has(k) && v != null ? String(v) : v])); // REST returns int64 as strings
function campaignRow(c) {
  const b = world.budgets.get(c.budget) || {};
  return { campaign: int64({ resourceName: c.resourceName, id: c.id, name: c.name, status: c.status, advertisingChannelType: c.channel, primaryStatus: c.status === 'ENABLED' ? 'ELIGIBLE' : c.status,
    primaryStatusReasons: c.status === 'PAUSED' ? ['CAMPAIGN_PAUSED'] : [], startDateTime: c.startDateTime, endDateTime: c.endDateTime }), campaignBudget: int64({ resourceName: c.budget, amountMicros: b.amountMicros }) };
}
const unknownGaql = new Set();
function runGaql(raw) {
  const q = raw.replace(/\s+/g, ' ').trim(), from = ((q.match(/\bFROM ([a-z_]+)/i) || [])[1] || '').toLowerCase();
  const where = (q.match(/\bWHERE (.*?)(?: ORDER BY | LIMIT |$)/i) || [])[1] || '';
  const range = q.match(/segments\.date BETWEEN '(\d{4}-\d{2}-\d{2})' AND '(\d{4}-\d{2}-\d{2})'/i);
  const idEq = where.match(/campaign\.id = (\d+)/), idIn = where.match(/campaign\.id IN \(([^)]*)\)/i);
  const ids = idEq ? [idEq[1]] : idIn ? idIn[1].split(',').map(s => s.trim()) : null;
  const statusEq = where.match(/campaign\.status = '([A-Z]+)'/), statusNe = where.match(/campaign\.status != '([A-Z]+)'/), channel = where.match(/campaign\.advertising_channel_type = '([A-Z_]+)'/);
  const okCampaign = c => c && (!ids || ids.includes(c.id)) && (!statusEq || c.status === statusEq[1]) && (!statusNe || c.status !== statusNe[1]) && (!channel || c.channel === channel[1]);
  const inList = field => { const m = where.match(new RegExp(field.replace(/\./g, '\\.') + " IN \\(([^)]*)\\)", 'i')); return m ? m[1].split(',').map(s => s.trim().replace(/^'|'$/g, '')) : null; };
  if (from === 'customer') return [{ customer: { resourceName: `customers/${CID}`, id: CID, timeZone: tz, currencyCode: 'CAD', descriptiveName: 'Synthetic test account' } }];
  if (from === 'campaign' && range) {
    const cd = /conversions_by_conversion_date/.test(q);
    const rows = world.metrics.filter(m => m.date >= range[1] && m.date <= range[2] && okCampaign(world.campaigns.get(m.campaignId))).map(m => {
      const c = world.campaigns.get(m.campaignId), metrics = { impressions: m.impressions, clicks: m.clicks, costMicros: m.costMicros, conversions: m.conversions, conversionsValue: m.conversionsValue };
      if (cd) Object.assign(metrics, { conversionsByConversionDate: m.cdConversions, conversionsValueByConversionDate: m.cdValue });
      return { campaign: int64({ resourceName: c.resourceName, id: c.id, name: c.name, status: c.status, advertisingChannelType: c.channel }),
        segments: /ad_network_type/.test(q) ? { date: m.date, adNetworkType: 'SEARCH' } : { date: m.date }, metrics: int64(metrics) };
    });
    return rows.sort((a, b) => a.segments.date < b.segments.date ? -1 : 1);
  }
  if (from === 'campaign') {
    const budgets = inList('campaign_budget.resource_name');
    return [...world.campaigns.values()].filter(okCampaign).filter(c => !budgets || budgets.includes(c.budget)).map(campaignRow);
  }
  if (range) return []; // no ad-, keyword- or product-level traffic in this synthetic account
  const camp = id => world.campaigns.get(id);
  if (from === 'ad_group') { const rns = inList('ad_group.resource_name');
    return [...world.adGroups.values()].filter(g => (!rns || rns.includes(g.resourceName)) && okCampaign(camp(g.campaignId)))
      .map(g => ({ campaign: { resourceName: camp(g.campaignId).resourceName, id: g.campaignId }, adGroup: int64({ resourceName: g.resourceName, id: g.id, name: g.name, status: g.status, type: g.type, cpcBidMicros: g.cpcBidMicros, campaign: camp(g.campaignId).resourceName }) })); }
  if (from === 'ad_group_ad') { const rns = inList('ad_group_ad.ad.resource_name');
    return [...world.ads.values()].filter(a => (!rns || rns.includes(a.ad.resourceName)) && okCampaign(camp(a.campaignId))).map(a => { const g = world.adGroups.get(a.adGroup);
      return { campaign: { resourceName: camp(a.campaignId).resourceName, id: a.campaignId }, adGroup: int64({ resourceName: g.resourceName, id: g.id, name: g.name }),
        adGroupAd: { resourceName: a.resourceName, adGroup: g.resourceName, status: a.status, adStrength: 'PENDING', policySummary: { approvalStatus: 'UNKNOWN', reviewStatus: 'REVIEW_IN_PROGRESS' }, ad: a.ad } }; }); }
  if (from === 'ad_group_criterion') return [...world.keywords.values()].filter(k => okCampaign(camp(k.campaignId))).map(k => { const g = world.adGroups.get(k.adGroup);
    return { campaign: { resourceName: camp(k.campaignId).resourceName, id: k.campaignId }, adGroup: int64({ resourceName: g.resourceName, id: g.id, name: g.name }),
      adGroupCriterion: { resourceName: k.resourceName, adGroup: g.resourceName, status: k.status, type: 'KEYWORD', keyword: k.keyword } }; }); // Google omits negative:false
  if (from === 'campaign_criterion') return [...world.campaignCriteria.values()].filter(x => okCampaign(camp(x.campaignId)))
    .filter(x => !/negative = TRUE/i.test(where) || x.negative).filter(x => !/type = 'LOCATION'/.test(where) || x.location).filter(x => !/type = 'KEYWORD'/.test(where) || x.keyword)
    .map(x => ({ campaign: { resourceName: camp(x.campaignId).resourceName, id: x.campaignId }, campaignCriterion: { resourceName: x.resourceName, campaign: camp(x.campaignId).resourceName, status: 'ENABLED',
      type: x.location ? 'LOCATION' : 'KEYWORD', ...(x.negative ? { negative: true } : {}), ...(x.location ? { location: x.location } : {}), ...(x.keyword ? { keyword: x.keyword } : {}) } }));
  if (from === 'campaign_asset') return [...world.campaignAssets.values()].filter(x => okCampaign(camp(x.campaignId)))
    .map(x => ({ campaign: { resourceName: camp(x.campaignId).resourceName, id: x.campaignId }, campaignAsset: { resourceName: x.resourceName, campaign: camp(x.campaignId).resourceName, asset: x.asset, fieldType: x.fieldType, status: 'ENABLED' }, asset: world.assets.get(x.asset) || {} }));
  unknownGaql.add(from || q.slice(0, 60));
  return [];
}
// Google Ads API v24 takes Campaign.start_date_time / end_date_time only as "yyyy-MM-dd HH:mm:ss" in the account time zone (the v24
// reference, the "Create campaigns" guide and DateError.INVALID_STRING_DATE_TIME_SECONDS) and returns that layout. Some of Google's client
// samples send "yyyyMMdd HH:mm:ss" and it happens to work; this synthetic account is strict on purpose, so a writer that drifts to another
// layout fails here (validate-only requests included) instead of relying on that leniency.
const GADS_DATE_TIME = /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/;
const dateTime = (field, v) => { if (v != null && !GADS_DATE_TIME.test(String(v))) throw new Error('INVALID_STRING_DATE_TIME_SECONDS campaign.' + field + ' ' + JSON.stringify(v)); return v; };
function assertDateTimes(ops) { for (const o of ops || []) { const c = o && o.campaignOperation && (o.campaignOperation.create || o.campaignOperation.update); if (c) { dateTime('start_date_time', c.startDateTime); dateTime('end_date_time', c.endDateTime); } } }
// Applies an accepted mutation the way Google would: temporary (negative) ids become real ids.
function applyMutations(ops) {
  assertDateTimes(ops); // Google checks the whole request before it applies any of it
  const real = new Map(), map = rn => real.get(rn) || rn, idOf = rn => String(rn).split('/').pop();
  return ops.map(o => {
    const type = Object.keys(o)[0], op = o[type], c = op.create;
    if (!c) {
      if (type === 'campaignOperation' && op.update) { const camp = world.campaigns.get(idOf(op.update.resourceName)); if (!camp) throw new Error('RESOURCE_NOT_FOUND ' + op.update.resourceName); if (op.update.status) camp.status = op.update.status;
        if (op.update.startDateTime) camp.startDateTime = op.update.startDateTime; if (op.update.endDateTime) camp.endDateTime = op.update.endDateTime; return { campaignResult: { resourceName: camp.resourceName } }; }
      throw new Error('Unsupported synthetic operation ' + type);
    }
    const created = rn => { if (c.resourceName) real.set(c.resourceName, rn); return rn; };
    if (type === 'campaignBudgetOperation') { const rn = created(`customers/${CID}/campaignBudgets/${newId()}`); world.budgets.set(rn, { resourceName: rn, amountMicros: String(c.amountMicros) }); return { campaignBudgetResult: { resourceName: rn } }; }
    if (type === 'campaignOperation') { const id = newId(), rn = created(`customers/${CID}/campaigns/${id}`); if (!world.budgets.has(map(c.campaignBudget))) throw new Error('INVALID_RESOURCE_REFERENCE campaignBudget');
      world.campaigns.set(id, { id, resourceName: rn, name: c.name, status: c.status, channel: c.advertisingChannelType, budget: map(c.campaignBudget), startDateTime: c.startDateTime, endDateTime: c.endDateTime });
      return { campaignResult: { resourceName: rn } }; }
    if (type === 'adGroupOperation') { const id = newId(), rn = created(`customers/${CID}/adGroups/${id}`), campaignId = idOf(map(c.campaign)); if (!world.campaigns.has(campaignId)) throw new Error('INVALID_RESOURCE_REFERENCE campaign');
      world.adGroups.set(rn, { id, resourceName: rn, name: c.name, campaignId, status: c.status || 'ENABLED', type: c.type, cpcBidMicros: c.cpcBidMicros }); return { adGroupResult: { resourceName: rn } }; }
    const group = c.adGroup && world.adGroups.get(map(c.adGroup)), campaign = c.campaign && world.campaigns.get(idOf(map(c.campaign)));
    if (c.adGroup && !group) throw new Error('INVALID_RESOURCE_REFERENCE adGroup ' + c.adGroup);
    if (c.campaign && !campaign) throw new Error('INVALID_RESOURCE_REFERENCE campaign ' + c.campaign);
    if (type === 'adGroupAdOperation') { const adId = newId(), rn = `customers/${CID}/adGroupAds/${group.id}~${adId}`;
      world.ads.set(rn, { resourceName: rn, adGroup: group.resourceName, campaignId: group.campaignId, status: c.status, ad: { ...JSON.parse(JSON.stringify(c.ad)), type: c.ad.responsiveSearchAd ? 'RESPONSIVE_SEARCH_AD' : 'UNKNOWN', id: adId, resourceName: `customers/${CID}/ads/${adId}` } }); return { adGroupAdResult: { resourceName: rn } }; }
    if (type === 'adGroupCriterionOperation') { const rn = `customers/${CID}/adGroupCriteria/${group.id}~${newId()}`; world.keywords.set(rn, { resourceName: rn, adGroup: group.resourceName, campaignId: group.campaignId, status: c.status, keyword: c.keyword }); return { adGroupCriterionResult: { resourceName: rn } }; }
    if (type === 'campaignCriterionOperation') { const rn = `customers/${CID}/campaignCriteria/${campaign.id}~${newId()}`; world.campaignCriteria.set(rn, { resourceName: rn, campaignId: campaign.id, negative: !!c.negative, location: c.location, keyword: c.keyword }); return { campaignCriterionResult: { resourceName: rn } }; }
    if (type === 'assetOperation') { const rn = created(`customers/${CID}/assets/${newId()}`); world.assets.set(rn, { ...JSON.parse(JSON.stringify(c)), resourceName: rn }); return { assetResult: { resourceName: rn } }; }
    if (type === 'campaignAssetOperation') { const asset = map(c.asset); if (!world.assets.has(asset)) throw new Error('INVALID_RESOURCE_REFERENCE asset'); const rn = `customers/${CID}/campaignAssets/${campaign.id}~${idOf(asset)}~${c.fieldType}`; world.campaignAssets.set(rn, { resourceName: rn, campaignId: campaign.id, asset, fieldType: c.fieldType }); return { campaignAssetResult: { resourceName: rn } }; }
    throw new Error('Unsupported synthetic operation ' + type);
  });
}

/* ---------------------------------------------------------------- synthetic store + canned AI */
const COLLECTION = { handle: 'harness-owl-lovers', title: 'Harness Owl Lovers' };
const PROFILE = { handle: COLLECTION.handle, title: COLLECTION.title, sampled: 24,
  typesDetail: [{ type: 'Necklace', n: 16, priceLow: 38, priceHigh: 72, materials: [{ t: 'sterling silver' }, { t: 'gold filled' }], personalization: ['engraved'] },
    { type: 'Earrings', n: 8, priceLow: 32, priceHigh: 54, materials: [{ t: 'sterling silver' }] }],
  motifs: [{ t: 'owl', n: 20 }, { t: 'snowy owl', n: 6 }, { t: 'barn owl', n: 5 }], listingTags: [{ t: 'owl lover', n: 9 }, { t: 'bird watcher', n: 4 }],
  mats: [{ t: 'sterling silver', n: 14 }, { t: 'gold filled', n: 6 }], personalization: ['engraved', 'initial'],
  topProducts: [{ title: 'Snowy Owl Necklace', handle: 'harness-snowy-owl-necklace', productId: 'gid://shopify/Product/9000001', sold: 5 },
    { title: 'Barn Owl Earrings', handle: 'harness-barn-owl-earrings', productId: 'gid://shopify/Product/9000002', sold: 3 }] };
const KEYWORDS = { 'snowy owl necklace': 880, 'sterling silver owl necklace': 390, 'engraved owl necklace': 170, 'barn owl pendant necklace': 140,
  'owl necklace for bird lover': 90, 'barn owl earrings': 320, 'sterling silver owl earrings': 210 };
const OPPORTUNITY = { collectionTitle: COLLECTION.title, occasion: 'Evergreen gifting', startDate: TODAY, endDate: ymd(29), daysOut: 0, priority: 'high', recommendedDailyBudget: 12,
  market: { fit: 1.05, fitWhy: 'Owl motifs dominate the collection listings', demand: 'steady', angle: 'A keepsake owl for the bird lover' }, proven: false,
  rationale: 'Steady owl demand and deep motif inventory', keywords: Object.keys(KEYWORDS).map(text => ({ text, searches: 100, competition: 'LOW', cpcLow: 0.4, cpcHigh: 1.1, intent: 'medium', tail: 'MID' })),
  keywordStrategy: 'Motif plus product type, long-tail first', negatives: ['owl costume', 'owl plush', 'owl drawing', 'free'], keyPhrases: ['A little owl to keep close'],
  audience: { buyer: 'Partners of bird watchers', recipient: 'Owl lovers', motivation: 'A meaningful nature keepsake', searchStyle: 'motif plus jewelry type' } };
const RSA = { headlines: ['Owl Necklaces for Bird Lovers', 'Handcrafted Owl Jewelry', 'Snowy Owl Necklace', 'Barn Owl Earrings', 'Sterling Silver Owl Charms', 'Engraved Owl Keepsakes',
  'Gifts for Owl Lovers', 'Personalize Your Owl Charm', 'Made to Order by Brites', 'Nature Lover Jewelry', 'A Keepsake Owl for Them', 'Owl Jewelry Made With Care',
  'Shop Owl Necklaces', 'Delicate Owl Pendants', 'Wise Little Owl Charm'],
  descriptions: ['Handcrafted owl necklaces and earrings, personalized for the bird lover in your life.', 'Choose sterling silver or gold filled, then engrave a name or initial.',
    'Snowy owl and barn owl designs made to order by Brites Jewelry.', 'A thoughtful keepsake for the owl lover who has everything.'],
  sitelinks: [{ text: 'Owl Necklaces', desc: 'Snowy and barn owl designs' }, { text: 'Owl Earrings', desc: 'Sterling silver pairs' }, { text: 'Personalize', desc: 'Engrave a name' }, { text: 'Bird Lovers', desc: 'More nature charms' }],
  callouts: ['Handcrafted', 'Made to Order', 'Engraving Available', 'Sterling Silver', 'Gold Filled Options', 'Gift Ready'] };
const conceptCopy = kind => ({ headlines: [kind + ' Owl Jewelry', 'Owl ' + kind, 'Handcrafted Owl ' + kind, 'Snowy Owl ' + kind, 'Engraved Owl ' + kind, 'Gift for Owl Lovers', 'Barn Owl ' + kind + ' Gift',
  'Silver Owl ' + kind, 'Owl Keepsake by Brites', 'Personalized Owl Charm', 'Owl Love, Made by Hand'].map(t => t.slice(0, 30)),
  longHeadlines: ['Handcrafted owl ' + kind.toLowerCase() + ' for the bird lover in your life', 'Snowy owl and barn owl ' + kind.toLowerCase() + ' made to order'],
  descriptions: ['Owl ' + kind.toLowerCase() + ' handcrafted to order.', 'Choose sterling silver or gold filled owl designs from Brites Jewelry.',
    'Engrave a name or initial on an owl keepsake for the bird lover.', 'Snowy owl and barn owl ' + kind.toLowerCase() + ' for everyday wear.'] });
const aiCalls = [];
function aiAnswer(prompt) {
  if (/campaign strategist for Brites/.test(prompt)) return ['strategist', { opportunities: [OPPORTUNITY] }];
  if (/You write Google Search ad copy for Brites/.test(prompt)) return ['search-copy', RSA];
  if (/Develop one coherent premium jewellery ad concept/.test(prompt)) {
    const kind = /"name":"Earrings/.test(prompt) ? 'Earrings' : 'Necklace';
    return ['creative-concept:' + kind, { brief: { buyer: 'Someone shopping for an owl lover', promise: 'An owl ' + kind.toLowerCase() + ' made to order', visualDirection: 'Product on warm linen',
      rationale: 'Keywords name the owl motif and the ' + kind.toLowerCase(), hypothesis: 'Motif-first copy lifts purchase rate', successMetric: 'purchase ROAS', demographics: 'broad' }, copy: conceptCopy(kind), learningApplications: [] }];
  }
  if (/Independently review the proposed jewellery ad/.test(prompt)) return ['creative-check', { pass: true, issues: [] }];
  return ['unexpected', null];
}
const landingHtml = p => `<!doctype html><html><head><title>${p}</title><style>body{}</style></head><body><h1>Owl jewelry by Brites (synthetic page)</h1>
<p>Handcrafted owl necklaces and owl earrings made to order. Choose a snowy owl or barn owl design in sterling silver or gold filled, and engrave a name or initial on the charm.
Each piece is finished by hand for the bird lover in your life. Necklace chains are adjustable; earrings come as a matched pair.</p><script>var ignored=1</script></body></html>`;

/* ---------------------------------------------------------------- fake fetch (every outside call) */
const calls = [], background = []; let step = 'setup', worker = null;
const reply = (status, body, headers = {}) => ({ ok: status < 400, status, statusText: String(status), headers: { get: k => headers[String(k).toLowerCase()] || null, raw: () => headers },
  json: async () => (typeof body === 'string' ? JSON.parse(body) : body), text: async () => (typeof body === 'string' ? body : JSON.stringify(body)),
  buffer: async () => Buffer.from(typeof body === 'string' ? body : JSON.stringify(body)) });
// The shared Claude client streams (stream: true), so Claude answers as server-sent events.
const streamReply = text => {
  const events = [{ type: 'message_start', message: { id: 'msg_flow', type: 'message', role: 'assistant', model: 'claude-sonnet-5-5', content: [], stop_reason: null, usage: { input_tokens: 1, output_tokens: 1 } } },
    { type: 'content_block_start', index: 0, content_block: { type: 'text', text: '' } }, { type: 'content_block_delta', index: 0, delta: { type: 'text_delta', text } }, { type: 'content_block_stop', index: 0 },
    { type: 'message_delta', delta: { stop_reason: 'end_turn', stop_sequence: null }, usage: { output_tokens: 1 } }, { type: 'message_stop' }];
  return { ...reply(200, {}, { 'content-type': 'text/event-stream' }), body: require('stream').Readable.from([Buffer.from(events.map(e => 'event: ' + e.type + '\ndata: ' + JSON.stringify(e) + '\n\n').join(''), 'utf8')]) };
};
async function fakeFetch(url, opts = {}) {
  url = String(url); let body = null; try { body = opts.body ? JSON.parse(String(opts.body)) : null; } catch (e) { body = String(opts.body); }
  const call = { step, url, body, at: calls.length }; calls.push(call);
  if (url === SITE + '/.netlify/functions/googleAdsAutopilot-background') {
    // Netlify acknowledges a background function with 202 at once and runs it afterwards.
    background.push(new Promise(done => setImmediate(done)).then(() => worker.handler({ httpMethod: 'POST', headers: {}, body: String(opts.body) }))
      .then(res => { call.worker = JSON.parse(res.body); call.workerStatus = res.statusCode; }));
    return reply(202, '');
  }
  if (/^https:\/\/oauth2\.googleapis\.com\/token$/.test(url)) return reply(200, { access_token: 'synthetic-token', expires_in: 3600 });
  const ads = url.match(/^https:\/\/googleads\.googleapis\.com\/v\d+\/customers\/(\d+)(?:\/([A-Za-z]+:[a-zA-Z]+)|:(generateKeywordIdeas))$/);
  if (ads) {
    const method = ads[2] || ads[3];
    if (method === 'googleAds:search') return reply(200, { results: runGaql(body.query) });
    if (method === 'generateKeywordIdeas') return reply(200, { results: body.keywordSeed.keywords.map(text => ({ text, keywordIdeaMetrics: { avgMonthlySearches: String(KEYWORDS[text] || 50), competition: 'LOW', competitionIndex: '22',
      lowTopOfPageBidMicros: '450000', highTopOfPageBidMicros: '1250000', monthlySearchVolumes: Array.from({ length: 12 }, (_, i) => ({ year: '2026', month: 'M' + i, monthlySearches: String(KEYWORDS[text] || 50) })) } })) });
    if (/:mutate$/.test(method)) {
      call.mutate = { service: method.split(':')[0], validateOnly: !!body.validateOnly, operations: body.mutateOperations || body.operations };
      if (body.validateOnly) {
        try { assertDateTimes(body.mutateOperations || (body.operations || []).map(op => ({ campaignOperation: op }))); } catch (e) { return reply(400, { error: { code: 400, status: 'INVALID_ARGUMENT', message: e.message } }); }
        return reply(200, {});
      }
      try {
        if (method === 'googleAds:mutate') return reply(200, { mutateOperationResponses: applyMutations(body.mutateOperations) });
        const svc = method.split(':')[0], type = { campaigns: 'campaignOperation' }[svc]; if (!type) throw new Error('Unsupported synthetic service ' + svc);
        return reply(200, { results: applyMutations(body.operations.map(op => ({ [type]: op }))).map(r => Object.values(r)[0]) });
      } catch (e) { return reply(400, { error: { code: 400, status: 'INVALID_ARGUMENT', message: e.message } }); }
    }
    return reply(404, { error: { message: 'synthetic Google Ads has no ' + method } });
  }
  if (/^https:\/\/api\.frankfurter\.app\/\d{4}-\d{2}-\d{2}\?from=CAD&to=USD$/.test(url)) return reply(200, { amount: 1, base: 'CAD', rates: { USD: FX } });
  if (url === 'https://api.openai.com/v1/chat/completions' || url === 'https://api.anthropic.com/v1/messages') {
    const text = []; const walk = v => { if (typeof v === 'string') text.push(v); else if (Array.isArray(v)) v.forEach(walk); else if (v && typeof v === 'object') { if (typeof v.text === 'string') text.push(v.text); if (v.content) walk(v.content); } };
    walk(body.system); walk(body.messages);
    const [kind, answer] = aiAnswer(text.join('\n')); call.ai = kind; aiCalls.push({ step, kind });
    if (!answer) return reply(400, { error: { message: 'flow-end-to-end: unexpected AI prompt' } });
    const json = JSON.stringify(answer);
    return url.includes('openai') ? reply(200, { choices: [{ message: { content: json }, finish_reason: 'stop' }], usage: { prompt_tokens: 1, completion_tokens: 1 } })
      : body.stream ? streamReply(json) : reply(200, { content: [{ type: 'text', text: json }], stop_reason: 'end_turn', usage: { input_tokens: 1, output_tokens: 1 } });
  }
  if (url === `https://${STORE}/admin/oauth/access_token`) return reply(200, { access_token: 'synthetic-shop-token', expires_in: 86399 });
  if (url.startsWith(`https://${STORE}/admin/api/`)) return reply(200, { data: { collections: { edges: [], pageInfo: { hasNextPage: false } }, products: { edges: [], pageInfo: { hasNextPage: false } }, orders: { edges: [], pageInfo: { hasNextPage: false } } } });
  const page = url.match(/^https:\/\/britesjewelry\.com\/(collections|products)\/([a-z0-9-]+)/);
  if (page) return reply(200, landingHtml(page[2]), { 'content-type': 'text/html' });
  call.unhandled = true; return reply(404, { error: { message: 'flow-end-to-end: no synthetic endpoint for ' + url.split('?')[0] } });
}

/* ---------------------------------------------------------------- load the real modules with the fakes */
const realResolve = Module._resolveFilename;
Module._resolveFilename = function (request, parent, ...rest) {
  if (request === 'node-fetch') return 'SYNTHETIC:node-fetch';
  if (/(^|\/)firebaseAdmin(\.js)?$/.test(request)) return 'SYNTHETIC:firebaseAdmin';
  return realResolve.call(this, request, parent, ...rest);
};
require.cache['SYNTHETIC:node-fetch'] = { id: 'SYNTHETIC:node-fetch', filename: 'SYNTHETIC:node-fetch', loaded: true, exports: fakeFetch };
require.cache['SYNTHETIC:firebaseAdmin'] = { id: 'SYNTHETIC:firebaseAdmin', filename: 'SYNTHETIC:firebaseAdmin', loaded: true, exports: store.admin };
const FN = path.resolve(__dirname, '../../netlify/functions');
const kick = require(path.join(FN, 'googleAdsAutopilotKick.js'));
worker = require(path.join(FN, 'googleAdsAutopilot-background.js'));
const E = require(path.join(FN, 'googleAdsAutopilot.js'));

async function api(action, data = {}, pass = PASS) {
  const res = await kick.httpHandler({ httpMethod: 'POST', headers: pass ? { 'x-edit-passcode': pass } : {}, body: JSON.stringify({ action, ...data }) });
  while (background.length) await background.shift(); // let dispatched worker tasks finish before the next console request
  return { status: res.statusCode, json: JSON.parse(res.body || '{}') };
}
const mutations = () => calls.filter(c => c.mutate);
const approvalDoc = id => store.docs.get('Brites_GAds_Approvals/' + id);
const clone = v => JSON.parse(JSON.stringify(v));
const sum = (rows, f) => rows.reduce((n, r) => n + Number(f(r) || 0), 0);
const near = (a, b) => Math.abs(a - b) < 0.005;

/* ---------------------------------------------------------------- the flow */
(async () => {
  // Synthetic starting state: live-like controls, cached catalogue research, one verified lesson.
  const now = Date.now(), CONTROL = { enabled: true, dryRun: false, maxDailyBudgetTotal: 100, smartBidding: false, defaultCountries: ['2124', '2840'], creativeBudgetUsd: 8 };
  store.docs.set('Brites_GAds_Control/control', CONTROL);
  store.docs.set('Brites_GAds_State/collections', { list: [COLLECTION, { handle: 'harness-fox-friends', title: 'Harness Fox Friends' }], at: now });
  store.docs.set('Brites_GAds_State/collectionProfiles', { v: 8, at: now, list: [PROFILE], salesBasis: 'synthetic fixture' });
  const LESSON = { id: 'syn-lesson-1', rule: 'Name the owl motif and the jewelry type in the first headline.', category: 'copy', scope: 'global', channels: ['search'],
    evidenceVerified: true, confidence: 'hypothesis', support: 1, evidenceIds: ['syn-evidence-1'], evidenceAt: now - 5 * 86400000 };
  store.docs.set('Brites_GAds_State/playbook', { version: 4, updatedAt: now - 86400000, lessons: [LESSON], changeLog: 'synthetic' });

  // 0. The console API refuses a caller without the passcode.
  check((await api('dashboard', {}, null)).status === 401, 'console API rejects a request without the edit passcode');

  // 1. Research: the console asks for a scan; the worker runs it; the tab reads the saved result.
  step = 'research';
  let r = await api('opportunities', { force: true });
  check(r.status === 200 && r.json.started === true && !r.json.dispatchError, 'research scan is dispatched to the background worker');
  const scanCall = calls.find(c => c.step === 'research' && c.worker);
  check(scanCall && scanCall.body.tasks[0] === 'scanOpportunities' && scanCall.worker.status === 'ran', 'background worker ran the scan');
  r = await api('opportunities');
  debug('opportunities', { lastError: r.json.lastError, n: (r.json.opportunities || []).length, audit: r.json.scanAudit && r.json.scanAudit.checks.filter(c => c.status !== 'ok').map(c => [c.id, c.status, c.error || c.detail]) });
  const opp = (r.json.opportunities || []).find(o => o.collectionHandle === COLLECTION.handle);
  check(opp && !r.json.scanning && !r.json.lastError, 'scanned opportunity is saved and served from the cache without a rescan');
  const measured = (opp.keywordData || []).filter(k => k.real && Number(k.searches) > 0);
  check(measured.length === Object.keys(KEYWORDS).length && measured.every(k => KEYWORDS[k.text] === Number(k.searches)), 'opportunity keywords carry measured Keyword Planner volumes, not the AI estimates');
  check(opp.eligibility && opp.eligibility.ready === true && opp.acted === null, 'opportunity is eligible and not yet in use');
  check(aiCalls.filter(a => a.step === 'research').map(a => a.kind).join() === 'strategist', 'research made exactly one strategist AI call');

  // 2. Draft: "Create review draft" on the card sends the card's values (brites-adwords.html launchOpp).
  step = 'draft';
  const card = { coll: opp.collectionHandle, event: opp.occasion, budget: opp.recommendedDailyBudget, startDate: opp.startDate, endDate: opp.endDate,
    maxCpc: opp.maxCpc || opp.plan.cpc.max, smartBidding: CONTROL.smartBidding, countries: CONTROL.defaultCountries };
  r = await api('generate', card);
  check(r.json.queued === true && r.json.genId, 'draft generation is queued to the worker');
  const gen = (await api('genStatus', { genId: r.json.genId })).json;
  check(gen.ok === true && gen.approvalId, 'worker reports the draft and its approval id: ' + (gen.reason || 'ok'));
  const id = gen.approvalId;
  let dash = (await api('dashboard')).json;
  const pending = (dash.pending || []).find(p => p.id === id);
  check(pending && pending.status === 'PENDING' && pending.vetted === false && pending.creative.phase === 'not_started', 'draft appears in Approvals as PENDING and awaiting creative review');
  const draftOps = pending.payload.mutateOperations, created = type => draftOps.filter(o => o[type]).map(o => o[type].create);
  const campaignCreate = created('campaignOperation')[0], budgetCreate = created('campaignBudgetOperation')[0];
  debug('draft', { summary: pending.summary, card, campaign: campaignCreate, keywords: created('adGroupCriterionOperation').map(k => k.keyword), research: opp.keywordData.map(k => [k.text, k.real, k.searches, k.source]) });
  check(created('campaignOperation').length === 1 && campaignCreate.status === 'PAUSED' && campaignCreate.name === 'BA · ' + COLLECTION.handle + '-evergreen' && campaignCreate.advertisingChannelType === 'SEARCH' && campaignCreate.manualCpc,
    'draft creates one paused manual-CPC Search campaign named for its opportunity tag');
  check(Number(budgetCreate.amountMicros) === Math.round(card.budget * 1e6) && gen.currency === 'CAD', 'draft daily budget equals the card budget, in the account currency');
  check(created('adGroupOperation').every(g => Number(g.cpcBidMicros) === Math.round(card.maxCpc * 1e6)), 'every ad group bid cap equals the card CPC cap');
  check(campaignCreate.endDateTime === card.endDate + ' 23:59:59' && (card.startDate > TODAY ? campaignCreate.startDateTime === card.startDate + ' 00:00:00' : !campaignCreate.startDateTime),
    'draft schedule equals the card run window, written as "yyyy-MM-dd HH:mm:ss" (the layout Google documents and returns)');
  check(opp.startDate === TODAY, 'an undated card starts on the account date (' + TODAY + ' in ' + tz + '), not the server UTC date');
  const dplan = pending.payload.plan || {};
  check(dplan.duration && dplan.duration.startDate === opp.startDate && dplan.duration.endDate === opp.endDate && dplan.duration.days === opp.durationDays && dplan.budget.daily === card.budget && dplan.cpc.max === card.maxCpc,
    'the draft forecast covers the card window, budget and CPC cap');
  check(JSON.stringify(dplan.expected) === JSON.stringify(opp.plan.expected), 'the draft forecast totals equal the card totals');
  // A run starting today reaches Google without a start date: it serves from the day it is enabled.
  check((pending.summary.includes('(' + opp.startDate + ' → ' + opp.endDate + ', ' + opp.durationDays + 'd)') || pending.summary.includes('(starts when enabled, ends ' + opp.endDate + ', up to ' + opp.durationDays + 'd)')) && pending.summary.includes('Manual CPC ≤ CAD '),
    'the Approvals summary shows the card window with both end dates counted and the cap in the account currency');
  check(JSON.stringify(created('campaignCriterionOperation').filter(c => c.location).map(c => c.location.geoTargetConstant.split('/').pop())) === JSON.stringify(card.countries), 'draft targets exactly the card countries');
  const draftNegatives = created('campaignCriterionOperation').filter(c => c.negative).map(c => c.keyword);
  check(['owl costume', 'owl plush', 'owl drawing'].every(t => draftNegatives.some(k => k.text === t && k.matchType === 'BROAD')) && ['free pattern', 'for free'].every(t => draftNegatives.some(k => k.text === t && k.matchType === 'PHRASE')) && !draftNegatives.some(k => k.text === 'free'),
    'draft excludes the card theme-conflict terms and freebie phrases; the card\'s lone "free" would stop "nickel free" buyers and is left out');
  const draftKeywords = created('adGroupCriterionOperation').map(k => k.keyword.text), grounded = new Set(opp.keywordData.map(k => k.text));
  check(measured.every(k => draftKeywords.includes(k.text)) && draftKeywords.every(k => grounded.has(k)), 'draft keywords are the grounded research keywords, including every measured one');
  check(mutations().length === 0, 'nothing has been sent to Google before approval');
  const oppAfterDraft = (await api('opportunities')).json.opportunities.find(o => o.collectionHandle === COLLECTION.handle);
  check(oppAfterDraft.acted && oppAfterDraft.acted.where === 'approval' && oppAfterDraft.acted.approvalId === id, 'opportunity is marked as held by the pending draft');
  const unmeasured = draftKeywords.filter(k => !measured.some(m => m.text === k)).length;
  console.log('NOTE draft summary: "' + pending.summary + '" (' + unmeasured + ' of ' + draftKeywords.length + ' keywords have no Keyword Planner data; account currency CAD)');

  // 3. Approving before creative review is refused and dispatches nothing.
  step = 'early-approve';
  r = await api('approve', { id });
  check(r.status === 500 && /Review creative/i.test(r.json.error) && approvalDoc(id).status === 'PENDING', 'approval without creative review is refused: ' + r.json.error);
  check(!calls.some(c => c.step === 'early-approve' && c.worker) && mutations().length === 0, 'refused approval neither dispatches a publication nor reaches Google');

  // 4. Creative review: the worker writes and checks copy per ad group; the operator reviews that exact version.
  step = 'creative';
  r = await api('creativePrepare', { id });
  check(r.json.queued === true, 'creative preparation is queued to the worker');
  const status = (await api('creativeStatus', { id })).json;
  check(status.creative.phase === 'ready' && status.current === true && status.status === 'PENDING', 'creative is ready for review and matches the current draft: ' + (status.creative.error || 'ok'));
  const groups = status.creative.groups;
  check(groups.length === 2 && groups.every(g => g.channel === 'search' && g.review && g.review.pass === true), 'each Search ad group passed its copy review');
  check(groups.every(g => (g.learning.lessonIds || []).includes(LESSON.id)), 'the verified playbook lesson was supplied to every creative prompt');
  check(aiCalls.filter(a => a.step === 'creative').length === 4 && !aiCalls.some(a => a.kind === 'unexpected'), 'creative review used one concept and one check call per ad group');
  check(mutations().length === 0, 'creative review sent nothing to Google');
  r = await api('reviewCreative', { id, hash: 'f'.repeat(64) });
  check(r.status === 500 && !approvalDoc(id).creative.review, 'review of a different version hash is refused');
  r = await api('reviewCreative', { id, hash: status.creative.payloadHash });
  check(r.status === 200 && r.json.ok && approvalDoc(id).creative.review.payloadHash === status.creative.payloadHash, 'operator review is bound to the exact payload hash');
  const approved = clone(approvalDoc(id).payload);
  const ads = approved.mutateOperations.filter(o => o.adGroupAdOperation).map(o => o.adGroupAdOperation.create);
  check(ads.length === 2 && ads.every(ad => groups.some(g => g.ref === ad.adGroup && JSON.stringify(ad.ad.responsiveSearchAd.headlines.map(h => h.text)) === JSON.stringify(g.copy.headlines))), 'approved ads contain exactly the reviewed headlines of their own ad group');
  const extensions = op => op.campaignAssetOperation || (op.assetOperation && op.assetOperation.create && (op.assetOperation.create.sitelinkAsset || op.assetOperation.create.calloutAsset || op.assetOperation.create.structuredSnippetAsset));
  console.log('NOTE sitelink/callout/snippet operations: ' + draftOps.filter(extensions).length + ' in the draft, ' + approved.mutateOperations.filter(extensions).length + ' after creative review; the summary text is unchanged');

  // 5. Dry run first: approval validates the exact payload with Google and creates nothing.
  step = 'dry-run';
  check((await api('dryRun', { on: true })).json.dryRun === true, 'dry run switched on from the console');
  r = await api('approve', { id });
  let sent = mutations();
  check(r.status === 200 && r.json.queued === true && sent.length === 1 && sent[0].mutate.validateOnly === true, 'approval in dry run sends one validate-only request');
  assert.deepEqual(sent[0].mutate.operations, approved.mutateOperations); passed++; console.log('PASS dry-run validation carries the approved payload unchanged');
  check(approvalDoc(id).status === 'APPROVED' && approvalDoc(id).validatedAt && ![...world.campaigns.values()].some(c => c.name === campaignCreate.name), 'dry run leaves the draft approved but unpublished; Google holds no new campaign');
  check((await api('dashboard')).json.stuck.some(x => x.id === id && x.status === 'APPROVED'), 'dashboard lists the validated draft as approved but not yet published');

  // 6. Publish: with dry run off, "apply" validates again and sends exactly the approved operations once.
  step = 'publish';
  check((await api('dryRun', { on: false })).json.dryRun === false, 'dry run switched off from the console');
  r = await api('apply', { id });
  sent = mutations().slice(1);
  check(r.status === 200 && sent.length === 2 && sent[0].mutate.validateOnly && !sent[1].mutate.validateOnly && sent.every(c => c.mutate.service === 'googleAds'), 'publication validates first, then sends one atomic mutate');
  assert.deepEqual(sent[1].mutate.operations, approved.mutateOperations); passed++; console.log('PASS operations sent to Google equal the approved payload, operation for operation');
  const doc = approvalDoc(id);
  check(doc.status === 'APPLIED' && doc.publishedCampaignIds.length === 1 && !doc.lastError, 'approval is recorded as APPLIED with its Google campaign id');
  const campaignId = doc.publishedCampaignIds[0], campaign = world.campaigns.get(campaignId);
  check(campaign && campaign.status === 'PAUSED' && campaign.name === campaignCreate.name && world.budgets.get(campaign.budget).amountMicros === String(budgetCreate.amountMicros), 'Google holds the new campaign paused with the approved budget');
  check(campaign.endDateTime === card.endDate + ' 23:59:59' && (card.startDate > TODAY ? campaign.startDateTime === card.startDate + ' 00:00:00' : !campaign.startDateTime),
    'Google accepted the run window in its documented "yyyy-MM-dd HH:mm:ss" layout (the synthetic account refuses any other, validate-only requests included)');
  check(world.keywords.size === draftKeywords.length && [...world.ads.values()].every(a => a.status === 'ENABLED'), 'every approved keyword and ad exists in Google');
  const versions = [...store.docs.keys()].filter(k => k.startsWith('Brites_GAds_State/adVersions/campaigns/' + campaignId));
  check(versions.length > 0 && !doc.versionWarning, 'publication saved a version history entry for the new campaign');
  check((await api('approvalStatus', { id })).json.status === 'APPLIED', 'approval status endpoint reports APPLIED');
  await api('apply', { id });
  check(mutations().length === 3 && approvalDoc(id).status === 'APPLIED', 'a repeated publish request cannot send the campaign again');
  check(!aiCalls.some(a => ['dry-run', 'publish'].includes(a.step)), 'approval and publication made no AI calls');

  // 7. Reporting: the new campaign appears; the operator enables it; two days of serving must agree everywhere.
  step = 'reporting';
  dash = (await api('dashboard')).json;
  const listed = dash.lastMetrics.find(c => String(c.id) === campaignId);
  check(listed && listed.status === 'PAUSED' && listed.budget === card.budget && listed.channel === 'SEARCH' && !(dash.pending || []).some(p => p.id === id), 'dashboard lists the published campaign (paused, approved budget) and no longer as pending');
  r = await api('setStatus', { id: campaignId, status: 'ENABLED' });
  const enable = mutations().pop();
  check(r.json.ok && enable.mutate.service === 'campaigns' && enable.mutate.operations.length === 1 && enable.mutate.operations[0].update.resourceName.endsWith('/' + campaignId) && enable.mutate.operations[0].update.status === 'ENABLED',
    'enabling from the console sends one status update for that campaign only');
  const PUBLISHED = TODAY;
  advanceDays(2);
  world.metrics.push({ campaignId, date: ymd(-1), impressions: 1200, clicks: 34, costMicros: 11420000, conversions: 2, conversionsValue: 131.5, cdConversions: 1, cdValue: 64 },
    { campaignId, date: ymd(0), impressions: 300, clicks: 9, costMicros: 3180000, conversions: 0, conversionsValue: 0, cdConversions: 1, cdValue: 67.5 });
  const start = PUBLISHED, end = ymd(0), inRange = m => m.date >= start && m.date <= end, mine = world.metrics.filter(m => m.campaignId === campaignId && inRange(m)), all = world.metrics.filter(inRange);
  const range = (await api('metricsRange', { start, end })).json;
  const mr = range.snapshot.find(c => c.id === campaignId);
  check(range.ok && range.currency === 'USD' && range.budgetCurrency === 'CAD' && mr && mr.status === 'ENABLED' && mr.budget === card.budget, 'metricsRange lists the enabled campaign with its CAD budget and USD metrics');
  check(mr.endDate === card.endDate && (card.startDate > TODAY ? mr.startDate === card.startDate : !mr.startDate), 'the campaign\'s dates, read back from Google\'s "yyyy-MM-dd HH:mm:ss" values, are the card\'s dates');
  check(near(mr.costNative, sum(mine, m => m.costMicros) / 1e6) && near(mr.cost, sum(mine, m => m.costMicros) / 1e6 * FX) && mr.clicks === 43 && mr.impr === 1500, 'campaign spend, clicks and impressions match Google (CAD converted at the daily rate)');
  check(mr.conv === 2 && near(mr.value, 131.5 * FX) && mr.convCd === 2 && near(mr.valueCd, 131.5 * FX), 'click-date and conversion-date conversions are carried separately');
  const daily = (await api('dailyStats', { start, end })).json;
  const dc = (daily.campaigns || []).find(c => String(c.id) === campaignId), dTotals = Object.values(daily.totalsByDay || {});
  check(dc && near(sum(dTotals, d => d.cost), sum(all, m => m.costMicros) / 1e6 * FX) && sum(dTotals, d => d.clicks) === sum(all, m => m.clicks), 'dailyStats includes the campaign and its day totals equal Google');
  check(near(sum(dTotals, d => d.cost), sum(range.snapshot, c => c.cost)) && sum(dTotals, d => d.conv) === sum(range.snapshot, c => c.conv) && sum(dTotals, d => d.clicks) === sum(range.snapshot, c => c.clicks),
    'dailyStats and metricsRange agree on spend, clicks and conversions for the same range');
  const oppLive = (await api('opportunities')).json.opportunities.find(o => o.collectionHandle === COLLECTION.handle);
  check(oppLive && oppLive.acted && oppLive.acted.where === 'campaign' && String(oppLive.acted.campaignId) === campaignId && oppLive.acted.status === 'ENABLED', 'research list shows the opportunity as held by the live campaign');

  // 8. Learning: the publication is linked to the lesson that shaped its copy; outcomes wait for a full window.
  step = 'learning';
  let book = (await api('playbook')).json;
  let lesson = (book.lessons || []).find(l => l.id === LESSON.id);
  const record = lesson && lesson.usage.records.find(x => x.approvalId === id && x.published);
  check(record && record.campaignIds.includes(campaignId) && record.newCampaign === true, 'learning overview links the lesson to the published campaign');
  let comparison = ((lesson.outcome || {}).comparisons || []).find(c => (c.campaignIds || []).includes(campaignId));
  check(book.measurement.status === 'pending' && comparison && comparison.status === 'pending' && !comparison.before && !comparison.after && comparison.causal === false, 'outcome tracking waits for a complete window and claims no effect yet');
  advanceDays(16); // 18 days after publication: the 14-day follow-up plus three reporting days has passed
  for (let n = 3; n <= 14; n++) world.metrics.push({ campaignId, date: ymd(n - 18), impressions: 500, clicks: 15, costMicros: 6000000, conversions: n % 4 === 0 ? 1 : 0, conversionsValue: n % 4 === 0 ? 60 : 0, cdConversions: 0, cdValue: 0 });
  book = (await api('playbook')).json;
  lesson = book.lessons.find(l => l.id === LESSON.id);
  comparison = ((lesson.outcome || {}).comparisons || []).find(c => (c.campaignIds || []).includes(campaignId));
  const after = world.metrics.filter(m => m.campaignId === campaignId && m.date > PUBLISHED && m.date <= ymd(-4));
  debug('matured comparison', comparison);
  check(comparison && comparison.status === 'unmeasured' && comparison.reason === 'NO_BASELINE' && comparison.causal === false, 'a new campaign is never credited with improvement: no pre-launch baseline');
  const afterRange = (await api('metricsRange', { start: comparison.afterStart, end: comparison.afterEnd })).json.snapshot.find(c => c.id === campaignId);
  check(near(comparison.after.cost, sum(after, m => m.costMicros) / 1e6) && comparison.after.clicks === sum(after, m => m.clicks) && near(comparison.after.cost, afterRange.costNative) && comparison.currency === 'CAD',
    'learning follow-up totals equal the reporting totals for the same window (both in CAD)');

  // Nothing outside the fakes was contacted; nothing reached a real service.
  const unexpected = calls.filter(c => c.unhandled).map(c => c.url.split('?')[0]);
  check(!aiCalls.some(a => a.kind === 'unexpected'), 'every AI prompt in the flow was a known type');
  if (unexpected.length) console.log('NOTE unanswered synthetic endpoints: ' + [...new Set(unexpected)].join(', '));
  if (unknownGaql.size) console.log('NOTE GAQL resources answered with no rows: ' + [...unknownGaql].join(', '));
  const took = ((RealDate.now() - started) / 1000).toFixed(1);
  check(RealDate.now() - started < 30000, 'whole flow ran in ' + took + ' s');
  console.log('PASS ' + passed + ' whole-flow checks (research -> draft -> creative review -> dry run -> publication -> reporting -> learning)');
  process.exit(0);
})().catch(e => { console.error('FAIL step "' + step + '":', e && e.stack || e); process.exit(1); });
